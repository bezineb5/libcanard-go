package cy

import (
	"encoding/binary"
)

// HeaderSize is the size of the Cyphal session layer header in bytes.
// This matches HEADER_BYTES in the C implementation (24 bytes).
const HeaderSize = 24

// HeaderType enumerates the message-type byte carried at offset 0 of every
// Cyphal session-layer header. It matches the C header_type_t enum exactly so
// that the Go and C implementations are wire-compatible.
type HeaderType uint8

const (
	HeaderTypeMsgBE   HeaderType = 0 // Best-effort multicast/unicast message.
	HeaderTypeMsgRel  HeaderType = 1 // Reliable message.
	HeaderTypeMsgAck  HeaderType = 2 // Positive acknowledgement (unicast only).
	HeaderTypeMsgNack HeaderType = 3 // Negative acknowledgement (unicast only).
	HeaderTypeRspBE   HeaderType = 4 // Best-effort response (unicast only).
	HeaderTypeRspRel  HeaderType = 5 // Reliable response (unicast only).
	HeaderTypeRspAck  HeaderType = 6 // Response acknowledgement (unicast only).
	HeaderTypeRspNack HeaderType = 7 // Response negative acknowledgement (unicast only).
	HeaderTypeGossip HeaderType = 8 // Topic-allocation CRDT gossip.
	HeaderTypeScout  HeaderType = 9 // Discovery scout.
)

// Header represents the Cyphal session layer header (cy_message_header_t).
// It is prepended to every application message before transmission and is
// always skipped on reception. The byte layout is fixed and little-endian:
//
//	[0]      type (HeaderType)
//	[1]      reserved (always 0)
//	[2]      incompatibility flag (0 for compatible)
//	[3]      lage (log-age, signed) -- for msg types; 0 for ack/rsp
//	[4:8]    evictions (u32 LE) -- for msg/gossip/scout; 0 for ack/rsp
//	[8:16]   hash (u64 LE) -- topic hash or service hash
//	[16:24]  tag/evictions2/size -- type-dependent; for msg: message tag
type Header struct {
	// Type is the message-type byte (HeaderType).
	Type HeaderType
	// Incompat is the incompatibility flag; must be 0 on the wire.
	Incompat byte
	// Reserved is byte[1], always 0.
	Reserved byte
	// Lage is the log-age (signed) for message/gossip/scout headers.
	Lage int8
	// Evictions is the CRDT eviction counter (msg/gossip/scout) or reserved.
	Evictions uint32
	// Hash is the topic/service hash.
	Hash uint64
	// Tag is the message tag (msg/ack/rsp) or the trailing field per type.
	Tag uint64
}

// NewHeader creates a new header with the common fields.
// lage/evictions/hash/tag are set per the C do_publish_impl layout; the caller
// may override fields after construction for ack/rsp/gossip/scout variants.
func NewHeader(headerType HeaderType, lage int8, evictions uint32, hash, tag uint64) *Header {
	return &Header{
		Type:      headerType,
		Incompat:  0,
		Reserved:  0,
		Lage:      lage,
		Evictions: evictions,
		Hash:      hash,
		Tag:       tag,
	}
}

// MarshalBinary encodes the header into the C-compatible 24-byte wire form.
func (h *Header) MarshalBinary() []byte {
	buf := make([]byte, HeaderSize)
	buf[0] = byte(h.Type)
	buf[1] = h.Reserved
	buf[2] = h.Incompat
	buf[3] = byte(h.Lage)
	binary.LittleEndian.PutUint32(buf[4:8], h.Evictions)
	binary.LittleEndian.PutUint64(buf[8:16], h.Hash)
	binary.LittleEndian.PutUint64(buf[16:24], h.Tag)
	return buf
}

// UnmarshalBinary decodes the C-compatible 24-byte wire form.
func (h *Header) UnmarshalBinary(data []byte) error {
	if len(data) < HeaderSize {
		return ErrArgument
	}
	h.Type = HeaderType(data[0])
	h.Reserved = data[1]
	h.Incompat = data[2]
	h.Lage = int8(data[3])
	h.Evictions = binary.LittleEndian.Uint32(data[4:8])
	h.Hash = binary.LittleEndian.Uint64(data[8:16])
	h.Tag = binary.LittleEndian.Uint64(data[16:24])
	return nil
}

// ParseHeader parses a header from the beginning of a byte slice.
// Returns the header and the remaining payload (header stripped).
func ParseHeader(data []byte) (*Header, []byte, error) {
	if len(data) < HeaderSize {
		return nil, nil, ErrArgument
	}
	header := &Header{}
	if err := header.UnmarshalBinary(data[:HeaderSize]); err != nil {
		return nil, nil, err
	}
	return header, data[HeaderSize:], nil
}

// PrependHeader prepends a header to a payload.
// Returns a new byte slice with the header + payload.
func PrependHeader(header *Header, payload []byte) []byte {
	headerData := header.MarshalBinary()
	result := make([]byte, HeaderSize+len(payload))
	copy(result, headerData)
	copy(result[HeaderSize:], payload)
	return result
}

// ExtractHeader extracts the header from a message and returns the payload.
// Reads the first HeaderSize bytes via absolute offset 0, so it is safe to call
// after a prior Skip (which only moves the skip offset used for subsequent
// Read/Payload calls).
func ExtractHeader(message *Message) (*Header, []byte, error) {
	if message.Size() < HeaderSize {
		return nil, nil, ErrArgument
	}
	headerData := make([]byte, HeaderSize)
	n := message.Read(0, HeaderSize, headerData)
	if n != HeaderSize {
		return nil, nil, ErrArgument
	}
	header := &Header{}
	if err := header.UnmarshalBinary(headerData); err != nil {
		return nil, nil, err
	}
	return header, message.Payload()[HeaderSize:], nil
}

// =====================================================================================================================
// Wire-compatible acknowledgement and response messages.
//
// In the C implementation these are carried inside the 24-byte cy_message_header_t:
//   - ACK/NACK  (unicast):  type=2/3, [8:16]=topic_hash, [16:24]=message_tag          (evictions[4:8]=0)
//   - RSP       (unicast):  type=4/5, [8:16]=service_hash, [16:24]=message_tag,
//                          [1]=tag, [2:8]=seqno(u48 LE)                            (evictions[4:8]=0)
//   - RSP ACK   (unicast):  type=6/7, [8:16]=hash, [16:24]=message_tag, [1]=tag, [2:8]=seqno
// The following helpers build/parse those headers directly so callers never
// construct a stray 4-byte protocol header.

// NewACKHeader builds an ACK/NACK header (C header_msg_ack / header_msg_nack).
func NewACKHeader(positive bool, topicHash, tag uint64) *Header {
	t := HeaderTypeMsgAck
	if !positive {
		t = HeaderTypeMsgNack
	}
	return &Header{Type: t, Hash: topicHash, Tag: tag}
}

// MarshalRSPHeader builds a response header (C header_rsp_be / header_rsp_rel) as the
// exact 24-byte wire form. Layout (must match cy.c do_respond):
//
//	[0]      = type (HeaderTypeRspBE / HeaderTypeRspRel)
//	[1]      = tag (small response tag, 0..255)
//	[2:8]    = seqno (u48 LE)
//	[8:16]   = service/topic hash
//	[16:24]  = message_tag (u64 LE) -- the original request's message tag
func MarshalRSPHeader(reliable bool, tag byte, seqno, serviceHash, messageTag uint64) []byte {
	t := byte(HeaderTypeRspBE)
	if reliable {
		t = byte(HeaderTypeRspRel)
	}
	return marshalResponseHeader(t, tag, seqno, serviceHash, messageTag)
}

// MarshalRSPACKHeader builds a response acknowledgement (C header_rsp_ack / header_rsp_nack).
// Layout matches cy.c send_response_ack:
//
//	[0]      = type (HeaderTypeRspAck / HeaderTypeRspNack)
//	[1]      = tag
//	[2:8]    = seqno (u48 LE)
//	[8:16]   = hash
//	[16:24]  = message_tag
func MarshalRSPACKHeader(positive bool, tag byte, seqno, hash, messageTag uint64) []byte {
	t := byte(HeaderTypeRspAck)
	if !positive {
		t = byte(HeaderTypeRspNack)
	}
	return marshalResponseHeader(t, tag, seqno, hash, messageTag)
}

func marshalResponseHeader(typ byte, tag byte, seqno, hash, messageTag uint64) []byte {
	buf := make([]byte, HeaderSize)
	buf[0] = typ
	buf[1] = tag
	// seqno is a 48-bit little-endian integer occupying bytes [2:8].
	binary.LittleEndian.PutUint64(buf[2:], seqno) // writes 8 bytes; bytes [8:10] overwritten below
	// Clear the top 16 bits that spilled into the hash region.
	buf[8] = 0
	buf[9] = 0
	binary.LittleEndian.PutUint64(buf[8:], hash)
	binary.LittleEndian.PutUint64(buf[16:], messageTag)
	return buf
}

// ParsedResponseHeader is the decoded form of a C-compatible response/response-ack header.
type ParsedResponseHeader struct {
	Type       HeaderType
	Reliable   bool
	Tag        byte
	Seqno      uint64
	Hash       uint64
	MessageTag uint64
}

// ParseResponseHeader decodes a 24-byte response (or response-ack) header.
// It assumes the buffer is at least HeaderSize bytes and the type byte is one of
// HeaderTypeRspBE / HeaderTypeRspRel / HeaderTypeRspAck / HeaderTypeRspNack.
func ParseResponseHeader(data []byte) (ParsedResponseHeader, error) {
	if len(data) < HeaderSize {
		return ParsedResponseHeader{}, ErrArgument
	}
	p := ParsedResponseHeader{Type: HeaderType(data[0])}
	switch p.Type {
	case HeaderTypeRspRel:
		p.Reliable = true
	case HeaderTypeRspAck:
	case HeaderTypeRspNack:
	case HeaderTypeRspBE:
	default:
		return ParsedResponseHeader{}, ErrArgument
	}
	p.Tag = data[1]
	// seqno is u48 LE at [2:8].
	p.Seqno = uint64(data[2]) | uint64(data[3])<<8 | uint64(data[4])<<16 |
		uint64(data[5])<<24 | uint64(data[6])<<32 | uint64(data[7])<<40
	p.Hash = binary.LittleEndian.Uint64(data[8:16])
	p.MessageTag = binary.LittleEndian.Uint64(data[16:24])
	return p, nil
}
