package cy

import (
	"encoding/binary"
)

// HeaderSize is the size of the Cyphal session layer header in bytes.
// This matches HEADER_BYTES in the C implementation (24 bytes).
const HeaderSize = 24

// Header represents the Cyphal session layer header.
// This is prepended to all application messages before transmission.
// The header format is designed to be parsed efficiently and to provide
// all necessary information for the session layer protocol.
type Header struct {
	// Tag is a unique identifier for this message.
	// Used for reliable delivery tracking and deduplication.
	Tag uint64
	
	// SequenceNumber is used for ordered messages.
	// Incremented for each message sent on a topic.
	SequenceNumber uint64
	
	// Timestamp is the message timestamp in microseconds.
	// This is optional and may be zero if not used.
	Timestamp Microsecond
	
	// Priority is the message priority.
	Priority Priority
	
	// SourceNodeID is the unique identifier of the source node.
	// This is used for routing responses.
	SourceNodeID uint64
	
	// Reserved bytes for future use.
	Reserved [4]byte
}

// NewHeader creates a new header with the specified values.
func NewHeader(tag uint64, seqno uint64, timestamp Microsecond, priority Priority, sourceNodeID uint64) *Header {
	return &Header{
		Tag:          tag,
		SequenceNumber: seqno,
		Timestamp:    timestamp,
		Priority:     priority,
		SourceNodeID:  sourceNodeID,
	}
}

// MarshalBinary encodes the header into binary form.
// The format is little-endian for compatibility with the C implementation.
func (h *Header) MarshalBinary() []byte {
	buf := make([]byte, HeaderSize)
	
	// Tag (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[0:8], h.Tag)
	
	// SequenceNumber (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[8:16], h.SequenceNumber)
	
	// Timestamp (8 bytes, little-endian, signed)
	// We use int64 for timestamp
	binary.LittleEndian.PutUint64(buf[16:24], uint64(h.Timestamp))
	
	return buf
}

// UnmarshalBinary decodes a header from binary form.
func (h *Header) UnmarshalBinary(data []byte) error {
	if len(data) < HeaderSize {
		return ErrArgument
	}
	
	// Tag
	h.Tag = binary.LittleEndian.Uint64(data[0:8])
	
	// SequenceNumber
	h.SequenceNumber = binary.LittleEndian.Uint64(data[8:16])
	
	// Timestamp
	h.Timestamp = Microsecond(int64(binary.LittleEndian.Uint64(data[16:24])))
	
	return nil
}

// ParseHeader parses a header from the beginning of a byte slice.
// Returns the header and the remaining payload.
func ParseHeader(data []byte) (*Header, []byte, error) {
	if len(data) < HeaderSize {
		return nil, nil, ErrArgument
	}
	
	header := &Header{}
	err := header.UnmarshalBinary(data[:HeaderSize])
	if err != nil {
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
// This is used when receiving messages.
func ExtractHeader(message *Message) (*Header, []byte, error) {
	if message.Size() < HeaderSize {
		return nil, nil, ErrArgument
	}
	
	// Read the header data
	headerData := make([]byte, HeaderSize)
	n := message.Read(0, HeaderSize, headerData)
	if n != HeaderSize {
		return nil, nil, ErrArgument
	}
	
	// Parse the header
	header := &Header{}
	err := header.UnmarshalBinary(headerData)
	if err != nil {
		return nil, nil, err
	}
	
	// Get the payload
	payload := make([]byte, message.Size()-HeaderSize)
	n = message.Read(HeaderSize, message.Size()-HeaderSize, payload)
	if n != len(payload) {
		return nil, nil, ErrArgument
	}
	
	return header, payload, nil
}

// ProtocolMessageType represents the type of protocol messages.
type ProtocolMessageType uint8

// Protocol Message Types
const (
	// ProtocolMessageGossip indicates a gossip message.
	ProtocolMessageGossip ProtocolMessageType = iota
	// ProtocolMessageACK indicates an acknowledgment message.
	ProtocolMessageACK
	// ProtocolMessageNACK indicates a negative acknowledgment message.
	ProtocolMessageNACK
	// ProtocolMessageRequest indicates a request message.
	ProtocolMessageRequest
	// ProtocolMessageResponse indicates a response message.
	ProtocolMessageResponse
)

// ProtocolHeader represents the header for protocol messages.
// Protocol messages have a different header format than application messages.
type ProtocolHeader struct {
	// MessageType indicates the type of protocol message.
	MessageType uint8
	
	// Reserved bytes.
	Reserved [3]byte
	
	// Payload follows
}

// ProtocolHeaderSize is the size of the protocol header.
const ProtocolHeaderSize = 4

// MarshalBinary encodes the protocol header.
func (h *ProtocolHeader) MarshalBinary() []byte {
	return []byte{
		h.MessageType,
		h.Reserved[0],
		h.Reserved[1],
		h.Reserved[2],
	}
}

// UnmarshalBinary decodes a protocol header.
func (h *ProtocolHeader) UnmarshalBinary(data []byte) error {
	if len(data) < ProtocolHeaderSize {
		return ErrArgument
	}
	
	h.MessageType = data[0]
	h.Reserved[0] = data[1]
	h.Reserved[1] = data[2]
	h.Reserved[2] = data[3]
	
	return nil
}

// GossipMessage represents a gossip message for topic discovery.
// Gossip messages contain the CRDT state of topics.
type GossipMessage struct {
	// Header is the protocol header.
	Header ProtocolHeader
	
	// Hash is the hash of the topic name.
	Hash uint64
	
	// LogAge is the log2 of seconds since topic creation.
	LogAge int32
	
	// Evictions is the number of times the topic has been evicted and recreated.
	Evictions uint32
}

// GossipMessageSize is the total size of a gossip message.
const GossipMessageSize = ProtocolHeaderSize + 8 + 4 + 4 // 20 bytes

// MarshalBinary encodes a gossip message.
func (g *GossipMessage) MarshalBinary() []byte {
	buf := make([]byte, GossipMessageSize)
	
	// Protocol header
	buf[0] = byte(ProtocolMessageGossip)
	
	// Hash (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[4:12], g.Hash)
	
	// LogAge (4 bytes, little-endian, signed)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(g.LogAge))
	
	// Evictions (4 bytes, little-endian)
	binary.LittleEndian.PutUint32(buf[16:20], g.Evictions)
	
	return buf
}

// UnmarshalBinary decodes a gossip message.
func (g *GossipMessage) UnmarshalBinary(data []byte) error {
	if len(data) < GossipMessageSize {
		return ErrArgument
	}
	
	// Protocol header
	g.Header.MessageType = data[0]
	
	// Hash
	g.Hash = binary.LittleEndian.Uint64(data[4:12])
	
	// LogAge
	g.LogAge = int32(binary.LittleEndian.Uint32(data[12:16]))
	
	// Evictions
	g.Evictions = binary.LittleEndian.Uint32(data[16:20])
	
	return nil
}

// ACKMessage represents an acknowledgment message for reliable delivery.
type ACKMessage struct {
	// Header is the protocol header.
	Header ProtocolHeader
	
	// Tag is the message tag being acknowledged.
	Tag uint64
	
	// SourceNodeID is the node ID sending the ACK.
	SourceNodeID uint64
}

// ACKMessageSize is the total size of an ACK message.
const ACKMessageSize = ProtocolHeaderSize + 8 + 8 // 20 bytes

// MarshalBinary encodes an ACK message.
func (a *ACKMessage) MarshalBinary() []byte {
	buf := make([]byte, ACKMessageSize)
	
	// Protocol header
	buf[0] = byte(ProtocolMessageACK)
	
	// Tag (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[4:12], a.Tag)
	
	// SourceNodeID (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[12:20], a.SourceNodeID)
	
	return buf
}

// UnmarshalBinary decodes an ACK message.
func (a *ACKMessage) UnmarshalBinary(data []byte) error {
	if len(data) < ACKMessageSize {
		return ErrArgument
	}
	
	// Protocol header
	a.Header.MessageType = data[0]
	
	// Tag
	a.Tag = binary.LittleEndian.Uint64(data[4:12])
	
	// SourceNodeID
	a.SourceNodeID = binary.LittleEndian.Uint64(data[12:20])
	
	return nil
}

// NACKMessage represents a negative acknowledgment message.
type NACKMessage struct {
	// Header is the protocol header.
	Header ProtocolHeader
	
	// Tag is the message tag being negatively acknowledged.
	Tag uint64
	
	// SourceNodeID is the node ID sending the NACK.
	SourceNodeID uint64
	
	// ErrorCode indicates the reason for the NACK.
	ErrorCode Error
}

// NACKMessageSize is the total size of a NACK message.
const NACKMessageSize = ProtocolHeaderSize + 8 + 8 + 1 // 21 bytes

// MarshalBinary encodes a NACK message.
func (n *NACKMessage) MarshalBinary() []byte {
	buf := make([]byte, NACKMessageSize)
	
	// Protocol header
	buf[0] = byte(ProtocolMessageNACK)
	
	// Tag (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[4:12], n.Tag)
	
	// SourceNodeID (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[12:20], n.SourceNodeID)
	
	// ErrorCode (1 byte)
	buf[20] = byte(n.ErrorCode)
	
	return buf
}

// UnmarshalBinary decodes a NACK message.
func (n *NACKMessage) UnmarshalBinary(data []byte) error {
	if len(data) < NACKMessageSize {
		return ErrArgument
	}
	
	// Protocol header
	n.Header.MessageType = data[0]
	
	// Tag
	n.Tag = binary.LittleEndian.Uint64(data[4:12])
	
	// SourceNodeID
	n.SourceNodeID = binary.LittleEndian.Uint64(data[12:20])
	
	// ErrorCode
	n.ErrorCode = Error(data[20])
	
	return nil
}

// RequestMessage represents a request message for RPC.
type RequestMessage struct {
	// Header is the protocol header.
	Header ProtocolHeader
	
	// Tag is the unique request tag.
	Tag uint64
	
	// SourceNodeID is the node ID sending the request.
	SourceNodeID uint64
	
	// ServiceID is the subject-ID of the service being called.
	ServiceID uint32
	
	// RequestID is a unique identifier for this request within the tag.
	RequestID uint32
}

// RequestMessageSize is the total size of a request message.
const RequestMessageSize = ProtocolHeaderSize + 8 + 8 + 4 + 4 // 24 bytes

// MarshalBinary encodes a request message.
func (r *RequestMessage) MarshalBinary() []byte {
	buf := make([]byte, RequestMessageSize)
	
	// Protocol header
	buf[0] = byte(ProtocolMessageRequest)
	
	// Tag (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[4:12], r.Tag)
	
	// SourceNodeID (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[12:20], r.SourceNodeID)
	
	// ServiceID (4 bytes, little-endian)
	binary.LittleEndian.PutUint32(buf[20:24], r.ServiceID)
	
	// RequestID (4 bytes, little-endian)
	binary.LittleEndian.PutUint32(buf[24:28], r.RequestID)
	
	return buf
}

// UnmarshalBinary decodes a request message.
func (r *RequestMessage) UnmarshalBinary(data []byte) error {
	if len(data) < RequestMessageSize {
		return ErrArgument
	}
	
	// Protocol header
	r.Header.MessageType = data[0]
	
	// Tag
	r.Tag = binary.LittleEndian.Uint64(data[4:12])
	
	// SourceNodeID
	r.SourceNodeID = binary.LittleEndian.Uint64(data[12:20])
	
	// ServiceID
	r.ServiceID = binary.LittleEndian.Uint32(data[20:24])
	
	// RequestID
	r.RequestID = binary.LittleEndian.Uint32(data[24:28])
	
	return nil
}

// ResponseMessage represents a response message for RPC.
type ResponseMessage struct {
	// Header is the protocol header.
	Header ProtocolHeader
	
	// Tag is the unique request tag being responded to.
	Tag uint64
	
	// SourceNodeID is the node ID sending the response.
	SourceNodeID uint64
	
	// RequestID is the request identifier this response is for.
	RequestID uint32
	
	// Status is the response status (0 = OK, non-zero = error).
	Status uint8
	
	// Reserved bytes.
	Reserved [3]byte
}

// ResponseMessageSize is the total size of a response message.
const ResponseMessageSize = ProtocolHeaderSize + 8 + 8 + 4 + 1 + 3 // 28 bytes

// MarshalBinary encodes a response message.
func (r *ResponseMessage) MarshalBinary() []byte {
	buf := make([]byte, ResponseMessageSize)
	
	// Protocol header
	buf[0] = byte(ProtocolMessageResponse)
	
	// Tag (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[4:12], r.Tag)
	
	// SourceNodeID (8 bytes, little-endian)
	binary.LittleEndian.PutUint64(buf[12:20], r.SourceNodeID)
	
	// RequestID (4 bytes, little-endian)
	binary.LittleEndian.PutUint32(buf[20:24], r.RequestID)
	
	// Status (1 byte)
	buf[24] = r.Status
	
	// Reserved (3 bytes)
	copy(buf[25:28], r.Reserved[:])
	
	return buf
}

// UnmarshalBinary decodes a response message.
func (r *ResponseMessage) UnmarshalBinary(data []byte) error {
	if len(data) < ResponseMessageSize {
		return ErrArgument
	}
	
	// Protocol header
	r.Header.MessageType = data[0]
	
	// Tag
	r.Tag = binary.LittleEndian.Uint64(data[4:12])
	
	// SourceNodeID
	r.SourceNodeID = binary.LittleEndian.Uint64(data[12:20])
	
	// RequestID
	r.RequestID = binary.LittleEndian.Uint32(data[20:24])
	
	// Status
	r.Status = data[24]
	
	// Reserved
	copy(r.Reserved[:], data[25:28])
	
	return nil
}

// ParseProtocolMessage parses a protocol message from a byte slice.
// Returns the specific message type and the parsed message.
func ParseProtocolMessage(data []byte) (interface{}, error) {
	if len(data) < ProtocolHeaderSize {
		return nil, ErrArgument
	}
	
	msgType := ProtocolMessageType(data[0])
	
	switch msgType {
	case ProtocolMessageGossip:
		gossip := &GossipMessage{}
		err := gossip.UnmarshalBinary(data)
		return gossip, err
		
	case ProtocolMessageACK:
		ack := &ACKMessage{}
		err := ack.UnmarshalBinary(data)
		return ack, err
		
	case ProtocolMessageNACK:
		nack := &NACKMessage{}
		err := nack.UnmarshalBinary(data)
		return nack, err
		
	case ProtocolMessageRequest:
		request := &RequestMessage{}
		err := request.UnmarshalBinary(data)
		return request, err
		
	case ProtocolMessageResponse:
		response := &ResponseMessage{}
		err := response.UnmarshalBinary(data)
		return response, err
		
	default:
		return nil, ErrArgument
	}
}

// IsProtocolMessage checks if a message starts with a protocol header.
func IsProtocolMessage(data []byte) bool {
	if len(data) < ProtocolHeaderSize {
		return false
	}
	
	msgType := ProtocolMessageType(data[0])
	return msgType >= ProtocolMessageGossip && msgType <= ProtocolMessageResponse
}
