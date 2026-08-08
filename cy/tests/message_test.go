package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestMessagePool tests the message pooling functionality.
func TestMessagePool(t *testing.T) {
	// Acquire a message
	msg := cy.AcquireMessage()
	if msg == nil {
		t.Fatal("AcquireMessage returned nil")
	}
	
	// Message should have zero size initially
	if msg.Size() != 0 {
		t.Errorf("New message should have size 0, got %d", msg.Size())
	}
	
	// Set some data
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	msg.SetData(data)
	
	// Check size
	if msg.Size() != len(data) {
		t.Errorf("Expected size %d, got %d", len(data), msg.Size())
	}
	
	// Read data back
	readData := make([]byte, msg.Size())
	n := msg.Read(0, msg.Size(), readData)
	if n != len(data) {
		t.Errorf("Expected to read %d bytes, read %d", len(data), n)
	}
	
	// Verify data
	for i := range data {
		if readData[i] != data[i] {
			t.Errorf("Data mismatch at index %d: expected %02x, got %02x", 
				i, data[i], readData[i])
		}
	}
	
	// Release the message
	cy.ReleaseMessage(msg)
	
	// Acquire another message (should reuse from pool)
	msg2 := cy.AcquireMessage()
	if msg2 == nil {
		t.Fatal("AcquireMessage returned nil after release")
	}
	
	// Message should be empty
	if msg2.Size() != 0 {
		t.Errorf("Reused message should have size 0, got %d", msg2.Size())
	}
	
	// Clean up
	cy.ReleaseMessage(msg2)
}

// TestMessageTS tests the MessageTS (timestamped message) functionality.
func TestMessageTS(t *testing.T) {
	// Acquire a message
	msg := cy.AcquireMessage()
	defer cy.ReleaseMessage(msg)
	
	// Set data
	msg.SetData([]byte{0xAA, 0xBB, 0xCC})
	
	// Create timestamped message
	timestamp := cy.Microsecond(1234567890)
	msgTS := cy.NewMessageTS(timestamp, msg)
	
	if msgTS == nil {
		t.Fatal("NewMessageTS returned nil")
	}
	
	// Check timestamp
	if msgTS.Timestamp != timestamp {
		t.Errorf("Expected timestamp %d, got %d", timestamp, msgTS.Timestamp)
	}
	
	// Check message
	if msgTS.Content != msg {
		t.Error("MessageTS.Content should return the original message")
	}
	
	// Release
	cy.ReleaseMessageTS(msgTS)
	
	// Note: After release, the message should not be accessed
}

// TestMessageReadPartial tests reading partial data from a message.
func TestMessageReadPartial(t *testing.T) {
	msg := cy.AcquireMessage()
	defer cy.ReleaseMessage(msg)
	
	// Set data
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	msg.SetData(data)
	
	// Read first 4 bytes
	readData := make([]byte, 4)
	n := msg.Read(0, 4, readData)
	if n != 4 {
		t.Errorf("Expected to read 4 bytes, read %d", n)
	}
	
	// Verify
	for i := 0; i < 4; i++ {
		if readData[i] != data[i] {
			t.Errorf("Data mismatch at index %d: expected %02x, got %02x", 
				i, data[i], readData[i])
		}
	}
	
	// Read last 4 bytes
	n = msg.Read(4, 4, readData)
	if n != 4 {
		t.Errorf("Expected to read 4 bytes, read %d", n)
	}
	
	// Verify
	for i := 0; i < 4; i++ {
		if readData[i] != data[i+4] {
			t.Errorf("Data mismatch at index %d: expected %02x, got %02x", 
				i, data[i+4], readData[i])
		}
	}
}

// TestMessageReadOutOfBounds tests reading beyond message bounds.
func TestMessageReadOutOfBounds(t *testing.T) {
	msg := cy.AcquireMessage()
	defer cy.ReleaseMessage(msg)
	
	// Set data
	data := []byte{0x01, 0x02, 0x03}
	msg.SetData(data)
	
	// Try to read beyond the end
	readData := make([]byte, 10)
	n := msg.Read(0, 10, readData)
	
	// Should only read available bytes
	if n != len(data) {
		t.Errorf("Expected to read %d bytes, read %d", len(data), n)
	}
	
	// Try to read from beyond the end
	n = msg.Read(10, 10, readData)
	if n != 0 {
		t.Errorf("Expected to read 0 bytes from out of bounds, read %d", n)
	}
}

// TestMessageGetData tests the GetData method.
func TestMessageGetData(t *testing.T) {
	msg := cy.AcquireMessage()
	defer cy.ReleaseMessage(msg)
	
	// Set data
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	msg.SetData(data)
	
	// Get data
	retrieved := msg.GetData()
	
	// Should return a copy
	if len(retrieved) != len(data) {
		t.Errorf("Expected length %d, got %d", len(data), len(retrieved))
	}
	
	// Verify contents
	for i := range data {
		if retrieved[i] != data[i] {
			t.Errorf("Data mismatch at index %d: expected %02x, got %02x", 
				i, data[i], retrieved[i])
		}
	}
	
	// Modify retrieved data - should not affect original
	retrieved[0] = 0xFF
	if msg.GetData()[0] != data[0] {
		t.Error("GetData returned data that can be modified")
	}
}

// TestMessagePoolConcurrency tests concurrent access to the message pool.
func TestMessagePoolConcurrency(t *testing.T) {
	// This is a basic test - more comprehensive concurrency testing
	// would require the race detector
	
	// Acquire many messages
	msgs := make([]*cy.Message, 100)
	for i := range msgs {
		msgs[i] = cy.AcquireMessage()
		msgs[i].SetData([]byte{byte(i)})
	}
	
	// Release them all
	for _, msg := range msgs {
		cy.ReleaseMessage(msg)
	}
	
	// Acquire them again
	for i := range msgs {
		msgs[i] = cy.AcquireMessage()
		// Message should be empty after reuse
		if msgs[i].Size() != 0 {
			t.Errorf("Reused message %d should have size 0, got %d", i, msgs[i].Size())
		}
	}
	
	// Clean up
	for _, msg := range msgs {
		cy.ReleaseMessage(msg)
	}
}

// TestMessageTSPool tests the MessageTS pooling.
func TestMessageTSPool(t *testing.T) {
	// Acquire and release multiple MessageTS
	for i := 0; i < 10; i++ {
		msg := cy.AcquireMessage()
		msg.SetData([]byte{byte(i)})
		
		msgTS := cy.NewMessageTS(cy.Microsecond(i*1000), msg)
		
		// Verify
		if msgTS.Timestamp != cy.Microsecond(i*1000) {
			t.Errorf("Expected timestamp %d, got %d", i*1000, msgTS.Timestamp)
		}
		
		// Release both
		cy.ReleaseMessageTS(msgTS)
		// Note: MessageTS release should also release the underlying message
	}
}

// TestMessageMultipleReads tests multiple reads from the same message.
func TestMessageMultipleReads(t *testing.T) {
	msg := cy.AcquireMessage()
	defer cy.ReleaseMessage(msg)
	
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	msg.SetData(data)
	
	// Read in chunks
	var result []byte
	
	// First 2 bytes
	chunk := make([]byte, 2)
	msg.Read(0, 2, chunk)
	result = append(result, chunk...)
	
	// Next 3 bytes
	chunk = make([]byte, 3)
	msg.Read(2, 3, chunk)
	result = append(result, chunk...)
	
	// Last 3 bytes
	chunk = make([]byte, 3)
	msg.Read(5, 3, chunk)
	result = append(result, chunk...)
	
	// Verify
	if len(result) != len(data) {
		t.Errorf("Expected %d bytes, got %d", len(data), len(result))
	}
	
	for i := range data {
		if result[i] != data[i] {
			t.Errorf("Data mismatch at index %d: expected %02x, got %02x", 
				i, data[i], result[i])
		}
	}
}

// TestMessageBytes tests the Bytes type.
func TestMessageBytes(t *testing.T) {
	// Create Bytes from a slice
	data := []byte{0x01, 0x02, 0x03}
	bytes := cy.NewBytes(data)
	
	if bytes.TotalSize() != len(data) {
		t.Errorf("Expected total size %d, got %d", len(data), bytes.TotalSize())
	}
	
	// Get slice
	slice := bytes.ToSlice()
	if len(slice) != len(data) {
		t.Errorf("Expected slice length %d, got %d", len(data), len(slice))
	}
	
	for i := range data {
		if slice[i] != data[i] {
			t.Errorf("Data mismatch at index %d", i)
		}
	}
}
