package tests

import (
	"encoding/binary"
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestHeaderMarshalUnmarshal tests basic header serialization.
func TestHeaderMarshalUnmarshal(t *testing.T) {
	header := cy.NewHeader(cy.HeaderTypeMsgBE, 42, 7, 0x0123456789ABCDEF, 0xFEDCBA9876543210)

	data := header.MarshalBinary()

	if len(data) != cy.HeaderSize {
		t.Errorf("Expected header size %d, got %d", cy.HeaderSize, len(data))
	}

	decoded := &cy.Header{}
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("Failed to unmarshal header: %v", err)
	}

	if decoded.Type != header.Type {
		t.Errorf("Type mismatch: expected %d, got %d", header.Type, decoded.Type)
	}
	if decoded.Lage != header.Lage {
		t.Errorf("Lage mismatch: expected %d, got %d", header.Lage, decoded.Lage)
	}
	if decoded.Evictions != header.Evictions {
		t.Errorf("Evictions mismatch: expected %d, got %d", header.Evictions, decoded.Evictions)
	}
	if decoded.Hash != header.Hash {
		t.Errorf("Hash mismatch: expected %016x, got %016x", header.Hash, decoded.Hash)
	}
	if decoded.Tag != header.Tag {
		t.Errorf("Tag mismatch: expected %016x, got %016x", header.Tag, decoded.Tag)
	}
}

// TestHeaderPrependAndParse tests prepending and parsing headers.
func TestHeaderPrependAndParse(t *testing.T) {
	header := cy.NewHeader(cy.HeaderTypeMsgRel, -3, 99, 0x1111111111111111, 0x2222222222222222)

	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}

	data := cy.PrependHeader(header, payload)

	expectedSize := cy.HeaderSize + len(payload)
	if len(data) != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, len(data))
	}

	parsedHeader, parsedPayload, err := cy.ParseHeader(data)
	if err != nil {
		t.Fatalf("Failed to parse header: %v", err)
	}

	if parsedHeader.Tag != header.Tag {
		t.Errorf("Tag mismatch after parse")
	}
	if parsedHeader.Hash != header.Hash {
		t.Errorf("Hash mismatch after parse")
	}

	if len(parsedPayload) != len(payload) {
		t.Errorf("Payload size mismatch: expected %d, got %d", len(payload), len(parsedPayload))
	}
	for i := range payload {
		if parsedPayload[i] != payload[i] {
			t.Errorf("Payload mismatch at index %d", i)
		}
	}
}

// TestHeaderParseInvalid tests parsing invalid headers.
func TestHeaderParseInvalid(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"too_short", []byte{0x01, 0x02, 0x03}},
		{"exact_size_minus_one", make([]byte, cy.HeaderSize-1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := cy.ParseHeader(tt.data)
			if err == nil {
				t.Error("Expected error for invalid header")
			}
		})
	}
}

// TestHeaderZeroValues tests header with zero values.
func TestHeaderZeroValues(t *testing.T) {
	header := &cy.Header{}

	data := header.MarshalBinary()

	decoded := &cy.Header{}
	if err := decoded.UnmarshalBinary(data); err != nil {
		t.Fatalf("Failed to unmarshal zero header: %v", err)
	}

	if decoded.Type != 0 || decoded.Lage != 0 || decoded.Evictions != 0 ||
		decoded.Hash != 0 || decoded.Tag != 0 {
		t.Errorf("Expected all zero fields, got %+v", decoded)
	}
}

// TestACKHeader verifies the ACK/NACK header wire layout matches C:
// type at [0], topic hash at [8:16], message tag at [16:24].
func TestACKHeader(t *testing.T) {
	h := cy.NewACKHeader(true, 0xDEADBEEF, 0x1234)
	if h.Type != cy.HeaderTypeMsgAck {
		t.Fatalf("expected MsgAck, got %d", h.Type)
	}
	buf := h.MarshalBinary()
	if buf[0] != byte(cy.HeaderTypeMsgAck) {
		t.Errorf("type byte mismatch")
	}
	if got := uint64(buf[8]) | uint64(buf[9])<<8 | uint64(buf[10])<<16 | uint64(buf[11])<<24 |
		uint64(buf[12])<<32 | uint64(buf[13])<<40 | uint64(buf[14])<<48 | uint64(buf[15])<<56; got != 0xDEADBEEF {
		t.Errorf("hash mismatch: %016x", got)
	}
	if got := uint64(buf[16]) | uint64(buf[17])<<8 | uint64(buf[18])<<16 | uint64(buf[19])<<24 |
		uint64(buf[20])<<32 | uint64(buf[21])<<40 | uint64(buf[22])<<48 | uint64(buf[23])<<56; got != 0x1234 {
		t.Errorf("tag mismatch: %016x", got)
	}

	nack := cy.NewACKHeader(false, 1, 2)
	if nack.Type != cy.HeaderTypeMsgNack {
		t.Errorf("expected MsgNack, got %d", nack.Type)
	}
}

// TestRSPHeader verifies the response header wire layout matches C do_respond exactly:
// [0]=type, [1]=tag, [2:8]=seqno(u48 LE), [8:16]=hash, [16:24]=message_tag.
func TestRSPHeader(t *testing.T) {
	buf := cy.MarshalRSPHeader(true, 0xAB, 0x112233445566, 0xCAFE, 0x7788)
	if len(buf) != cy.HeaderSize {
		t.Fatalf("expected %d bytes, got %d", cy.HeaderSize, len(buf))
	}
	if buf[0] != byte(cy.HeaderTypeRspRel) {
		t.Errorf("type byte mismatch: %d", buf[0])
	}
	if buf[1] != 0xAB {
		t.Errorf("tag byte mismatch: %x", buf[1])
	}
	// seqno u48 LE at [2:8].
	gotSeq := uint64(buf[2]) | uint64(buf[3])<<8 | uint64(buf[4])<<16 |
		uint64(buf[5])<<24 | uint64(buf[6])<<32 | uint64(buf[7])<<40
	if gotSeq != 0x112233445566 {
		t.Errorf("seqno mismatch: %016x", gotSeq)
	}
	if got := binary.LittleEndian.Uint64(buf[8:16]); got != 0xCAFE {
		t.Errorf("hash mismatch: %016x", got)
	}
	if got := binary.LittleEndian.Uint64(buf[16:24]); got != 0x7788 {
		t.Errorf("message_tag mismatch: %016x", got)
	}

	ack := cy.MarshalRSPACKHeader(false, 0x01, 5, 0xBABE, 0x99)
	if ack[0] != byte(cy.HeaderTypeRspNack) {
		t.Errorf("ack type mismatch: %d", ack[0])
	}
	if ack[1] != 0x01 {
		t.Errorf("ack tag mismatch")
	}
	gotAckSeq := uint64(ack[2]) | uint64(ack[3])<<8 | uint64(ack[4])<<16 |
		uint64(ack[5])<<24 | uint64(ack[6])<<32 | uint64(ack[7])<<40
	if gotAckSeq != 5 {
		t.Errorf("ack seqno mismatch: %d", gotAckSeq)
	}

	// Round-trip parse.
	dec, err := cy.ParseResponseHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Type != cy.HeaderTypeRspRel || !dec.Reliable {
		t.Errorf("parsed type/reliable mismatch")
	}
	if dec.Tag != 0xAB || dec.Seqno != 0x112233445566 || dec.Hash != 0xCAFE || dec.MessageTag != 0x7788 {
		t.Errorf("parsed fields mismatch: %+v", dec)
	}
}
