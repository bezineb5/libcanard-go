package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestHeaderMarshalUnmarshal tests basic header serialization.
func TestHeaderMarshalUnmarshal(t *testing.T) {
	// Create a header
	// Note: Priority and SourceNodeID are not part of the serialized header
	// They are part of the transport layer (CAN ID, UDP header)
	header := cy.NewHeader(
		0x0123456789ABCDEF, // Tag
		0xFEDCBA9876543210, // SequenceNumber
		cy.Microsecond(1234567890), // Timestamp
		0, // Priority (not serialized)
		0, // SourceNodeID (not serialized)
	)
	
	// Marshal to binary
	data := header.MarshalBinary()
	
	// Verify size
	if len(data) != cy.HeaderSize {
		t.Errorf("Expected header size %d, got %d", cy.HeaderSize, len(data))
	}
	
	// Unmarshal
	decoded := &cy.Header{}
	err := decoded.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal header: %v", err)
	}
	
	// Verify fields
	if decoded.Tag != header.Tag {
		t.Errorf("Tag mismatch: expected %016x, got %016x", header.Tag, decoded.Tag)
	}
	if decoded.SequenceNumber != header.SequenceNumber {
		t.Errorf("SequenceNumber mismatch: expected %016x, got %016x", 
			header.SequenceNumber, decoded.SequenceNumber)
	}
	if decoded.Timestamp != header.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", 
			header.Timestamp, decoded.Timestamp)
	}
	// Note: Priority and SourceNodeID are not serialized, so we don't check them
}

// TestHeaderPrependAndParse tests prepending and parsing headers.
func TestHeaderPrependAndParse(t *testing.T) {
	// Create a header
	header := cy.NewHeader(
		0x1111111111111111,
		0x2222222222222222,
		cy.Microsecond(999999999),
		cy.PriorityFast,
		0x33333333,
	)
	
	// Create payload
	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}
	
	// Prepend header
	data := cy.PrependHeader(header, payload)
	
	// Verify total size
	expectedSize := cy.HeaderSize + len(payload)
	if len(data) != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, len(data))
	}
	
	// Parse header
	parsedHeader, parsedPayload, err := cy.ParseHeader(data)
	if err != nil {
		t.Fatalf("Failed to parse header: %v", err)
	}
	
	// Verify header
	if parsedHeader.Tag != header.Tag {
		t.Errorf("Tag mismatch after parse")
	}
	
	// Verify payload
	if len(parsedPayload) != len(payload) {
		t.Errorf("Payload size mismatch: expected %d, got %d", 
			len(payload), len(parsedPayload))
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
	
	// Marshal
	data := header.MarshalBinary()
	
	// Unmarshal
	decoded := &cy.Header{}
	err := decoded.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal zero header: %v", err)
	}
	
	// All fields should be zero
	if decoded.Tag != 0 {
		t.Errorf("Expected Tag 0, got %d", decoded.Tag)
	}
	if decoded.SequenceNumber != 0 {
		t.Errorf("Expected SequenceNumber 0, got %d", decoded.SequenceNumber)
	}
	if decoded.Timestamp != 0 {
		t.Errorf("Expected Timestamp 0, got %d", decoded.Timestamp)
	}
	if decoded.Priority != 0 {
		t.Errorf("Expected Priority 0, got %d", decoded.Priority)
	}
}

// TestProtocolHeader tests protocol message headers.
func TestProtocolHeader(t *testing.T) {
	// Test each protocol message type
	for msgType := cy.ProtocolMessageGossip; msgType <= cy.ProtocolMessageResponse; msgType++ {
		header := cy.ProtocolHeader{
			MessageType: uint8(msgType),
		}
		
		// Marshal
		data := header.MarshalBinary()
		
		// Verify size
		if len(data) != cy.ProtocolHeaderSize {
			t.Errorf("Protocol header size mismatch for type %d", msgType)
		}
		
		// First byte should be message type
		if data[0] != byte(msgType) {
			t.Errorf("Message type mismatch: expected %d, got %d", msgType, data[0])
		}
		
		// Unmarshal
		decoded := &cy.ProtocolHeader{}
		err := decoded.UnmarshalBinary(data)
		if err != nil {
			t.Fatalf("Failed to unmarshal protocol header: %v", err)
		}
		
		if decoded.MessageType != uint8(msgType) {
			t.Errorf("Decoded message type mismatch")
		}
	}
}

// TestGossipMessage tests gossip message serialization.
func TestGossipMessage(t *testing.T) {
	gossip := &cy.GossipMessage{
		Header: cy.ProtocolHeader{
			MessageType: uint8(cy.ProtocolMessageGossip),
		},
		Hash:       0x123456789ABCDEF0,
		LogAge:     42,
		Evictions:  7,
	}
	
	// Marshal
	data := gossip.MarshalBinary()
	
	// Verify size
	if len(data) != cy.GossipMessageSize {
		t.Errorf("Expected gossip message size %d, got %d", 
			cy.GossipMessageSize, len(data))
	}
	
	// Unmarshal
	decoded := &cy.GossipMessage{}
	err := decoded.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal gossip message: %v", err)
	}
	
	// Verify fields
	if decoded.Header.MessageType != uint8(cy.ProtocolMessageGossip) {
		t.Error("MessageType mismatch")
	}
	if decoded.Hash != gossip.Hash {
		t.Errorf("Hash mismatch: expected %016x, got %016x", gossip.Hash, decoded.Hash)
	}
	if decoded.LogAge != gossip.LogAge {
		t.Errorf("LogAge mismatch: expected %d, got %d", gossip.LogAge, decoded.LogAge)
	}
	if decoded.Evictions != gossip.Evictions {
		t.Errorf("Evictions mismatch: expected %d, got %d", 
			gossip.Evictions, decoded.Evictions)
	}
}

// TestACKMessage tests ACK message serialization.
func TestACKMessage(t *testing.T) {
	ack := &cy.ACKMessage{
		Header: cy.ProtocolHeader{
			MessageType: uint8(cy.ProtocolMessageACK),
		},
		Tag:          0xDEADBEEFCAFEBABE,
		SourceNodeID: 0x12345678,
	}
	
	// Marshal
	data := ack.MarshalBinary()
	
	// Verify size
	if len(data) != cy.ACKMessageSize {
		t.Errorf("Expected ACK message size %d, got %d", 
			cy.ACKMessageSize, len(data))
	}
	
	// Unmarshal
	decoded := &cy.ACKMessage{}
	err := decoded.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal ACK message: %v", err)
	}
	
	// Verify fields
	if decoded.Header.MessageType != uint8(cy.ProtocolMessageACK) {
		t.Error("MessageType mismatch")
	}
	if decoded.Tag != ack.Tag {
		t.Errorf("Tag mismatch: expected %016x, got %016x", ack.Tag, decoded.Tag)
	}
	if decoded.SourceNodeID != ack.SourceNodeID {
		t.Errorf("SourceNodeID mismatch: expected %016x, got %016x", 
			ack.SourceNodeID, decoded.SourceNodeID)
	}
}

// TestNACKMessage tests NACK message serialization.
func TestNACKMessage(t *testing.T) {
	// NACKMessage.ErrorCode is an int, not an error
	nack := &cy.NACKMessage{
		Header: cy.ProtocolHeader{
			MessageType: uint8(cy.ProtocolMessageNACK),
		},
		Tag:          0xFEEDBEEF,
		SourceNodeID: 0x87654321,
		ErrorCode:    1, // Use int value instead of error
	}
	
	// Marshal
	data := nack.MarshalBinary()
	
	// Verify size
	if len(data) != cy.NACKMessageSize {
		t.Errorf("Expected NACK message size %d, got %d", 
			cy.NACKMessageSize, len(data))
	}
	
	// Unmarshal
	decoded := &cy.NACKMessage{}
	err := decoded.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal NACK message: %v", err)
	}
	
	// Verify fields
	if decoded.Header.MessageType != uint8(cy.ProtocolMessageNACK) {
		t.Error("MessageType mismatch")
	}
	if decoded.Tag != nack.Tag {
		t.Errorf("Tag mismatch")
	}
	if decoded.SourceNodeID != nack.SourceNodeID {
		t.Errorf("SourceNodeID mismatch")
	}
	if decoded.ErrorCode != nack.ErrorCode {
		t.Errorf("ErrorCode mismatch")
	}
}

// TestParseProtocolMessage tests the ParseProtocolMessage function.
func TestParseProtocolMessage(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "gossip",
			data:    []byte{byte(cy.ProtocolMessageGossip), 0, 0, 0, 0x34, 0x12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name:    "ack",
			data:    []byte{byte(cy.ProtocolMessageACK), 0, 0, 0, 0xEF, 0xBE, 0xEE, 0xAD, 0xDE, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name:    "invalid_type",
			data:    []byte{0x99, 0, 0, 0}, // Invalid message type
			wantErr: true,
		},
		{
			name:    "too_short",
			data:    []byte{byte(cy.ProtocolMessageGossip)},
			wantErr: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, err := cy.ParseProtocolMessage(tt.data)
			
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			if msg == nil {
				t.Error("Expected message, got nil")
				return
			}
			
			// Type assertion based on first byte of data
			msgType := tt.data[0]
			if msgType == byte(cy.ProtocolMessageGossip) {
				_, ok := msg.(*cy.GossipMessage)
				if !ok {
					t.Error("Expected *GossipMessage")
				}
			} else if msgType == byte(cy.ProtocolMessageACK) {
				_, ok := msg.(*cy.ACKMessage)
				if !ok {
					t.Error("Expected *ACKMessage")
				}
			}
		})
	}
}

// TestIsProtocolMessage tests the IsProtocolMessage function.
func TestIsProtocolMessage(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{"empty", []byte{}, false},
		{"too_short", []byte{0x00}, false},
		{"gossip", []byte{byte(cy.ProtocolMessageGossip), 0, 0, 0}, true},
		{"ack", []byte{byte(cy.ProtocolMessageACK), 0, 0, 0}, true},
		{"nack", []byte{byte(cy.ProtocolMessageNACK), 0, 0, 0}, true},
		{"request", []byte{byte(cy.ProtocolMessageRequest), 0, 0, 0}, true},
		{"response", []byte{byte(cy.ProtocolMessageResponse), 0, 0, 0}, true},
		{"invalid_type", []byte{0x99, 0, 0, 0}, false},
		{"application_data", []byte{0x10, 0x01, 0x02, 0x03}, false},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cy.IsProtocolMessage(tt.data)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
