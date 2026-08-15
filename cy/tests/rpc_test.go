package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestRequest tests sending a request.
func TestRequest(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise a service
	pub, err := node.Advertise("test.service")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Send a request
	future := pub.Request(node.Now()+1000000, 500000, []byte{0x01, 0x02})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()
	
	// Check the future
	if future.Tag() == 0 {
		// Tag can be 0 for the first request
	}
	
	// Check response count
	if future.ResponseCount() != 0 {
		t.Errorf("Expected response count 0, got %d", future.ResponseCount())
	}
}

// TestRequestMultiple tests sending multiple requests.
func TestRequestMultiple(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise a service
	pub, err := node.Advertise("test.service")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Send multiple requests
	futures := make([]*cy.RequestFuture, 5)
	for i := 0; i < 5; i++ {
		future := pub.Request(node.Now()+1000000, 500000, []byte{byte(i)})
		if future == nil {
			t.Fatalf("Failed to send request %d", i)
		}
		futures[i] = future
	}
	
	// Clean up
	for _, future := range futures {
		future.Destroy()
	}
}

// TestRequestTag tests the tag of a request future.
func TestRequestTag(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise a service
	pub, err := node.Advertise("test.service")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Send a request
	future1 := pub.Request(node.Now()+1000000, 500000, []byte{0x01})
	if future1 == nil {
		t.Fatal("Expected future1, got nil")
	}
	defer future1.Destroy()
	
	// Send another request
	future2 := pub.Request(node.Now()+1000000, 500000, []byte{0x02})
	if future2 == nil {
		t.Fatal("Expected future2, got nil")
	}
	defer future2.Destroy()
	
	// Tags should be sequential
	if future2.Tag() <= future1.Tag() {
		t.Errorf("Expected tag2 > tag1, got %d <= %d", future2.Tag(), future1.Tag())
	}
}

// TestRequestFutureDone tests the Done method of a request future.
func TestRequestFutureDone(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise a service
	pub, err := node.Advertise("test.service")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Send a request
	future := pub.Request(node.Now()+1000000, 500000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()
	
	// Initially, future should not be done
	if future.Done() {
		t.Error("Expected future not done")
	}
}

// TestCancelRequest tests canceling a request.
func TestCancelRequest(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise a service
	pub, err := node.Advertise("test.service")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Send a request
	future := pub.Request(node.Now()+1000000, 500000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	
	// Cancel the request
	node.RPC().CancelRequest(future.Tag())
	
	// Clean up
	future.Destroy()
}

// TestResponderWireFormat verifies that a response emitted via RPC.Respond produces the
// exact C-compatible response header: [0]=type, [1]=tag, [2:8]=seqno(u48), [8:16]=hash,
// [16:24]=message_tag. It also confirms the request's correlation (message_tag) and
// topic hash survive into the on-wire header and that the lane carries the remote id and
// priority from the breadcrumb.
func TestResponderWireFormat(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "responder_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()

	const (
		remoteID   = uint64(17)
		topicHash  = uint64(0xAABBCCDDEEFF0011)
		messageTag = uint64(0x0123456789ABCDEF)
	)

	bc := cy.NewBreadcrumb(node, cy.PriorityHigh, remoteID, topicHash, messageTag)
	// Breadcrumb seqno starts at 0; the first response reports seqno 0 (C: breadcrumb->seqno++).
	bc.Seqno = 0

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if err := node.RPC().Respond(bc, 0, payload); err != nil {
		t.Fatalf("Respond returned error: %v", err)
	}

	if len(platform.ReceivedMessages) != 1 {
		t.Fatalf("expected 1 unicast message, got %d", len(platform.ReceivedMessages))
	}
	sent := platform.ReceivedMessages[0]
	if sent.Lane.ID != remoteID {
		t.Errorf("lane remote id mismatch: got %d want %d", sent.Lane.ID, remoteID)
	}
	if sent.Lane.Priority != cy.PriorityHigh {
		t.Errorf("lane priority mismatch: got %d want %d", sent.Lane.Priority, cy.PriorityHigh)
	}

	wire := sent.Data
	parsed, perr := cy.ParseResponseHeader(wire)
	if perr != nil {
		t.Fatalf("ParseResponseHeader failed: %v", perr)
	}
	if parsed.Type != cy.HeaderTypeRspBE {
		t.Errorf("response type mismatch: got %d want %d", parsed.Type, cy.HeaderTypeRspBE)
	}
	if parsed.Reliable {
		t.Errorf("best-effort response must not be reliable")
	}
	if parsed.Hash != topicHash {
		t.Errorf("response topic hash mismatch: got %016x want %016x", parsed.Hash, topicHash)
	}
	if parsed.MessageTag != messageTag {
		t.Errorf("response message tag mismatch: got %016x want %016x", parsed.MessageTag, messageTag)
	}
	if parsed.Seqno != 0 {
		t.Errorf("response seqno mismatch: got %d want 0", parsed.Seqno)
	}
	if parsed.Tag != 0xFF {
		t.Errorf("best-effort response tag must be 0xFF, got %#x", parsed.Tag)
	}

	// The 24-byte header must be immediately followed by the application payload.
	if len(wire) != cy.HeaderSize+len(payload) {
		t.Fatalf("wire length mismatch: got %d want %d", len(wire), cy.HeaderSize+len(payload))
	}
	for i, b := range payload {
		if wire[cy.HeaderSize+i] != b {
			t.Errorf("payload byte %d mismatch: got %#x want %#x", i, wire[cy.HeaderSize+i], b)
		}
	}

	// A second response must advance the seqno.
	if err := node.RPC().Respond(bc, 0, payload); err != nil {
		t.Fatalf("second Respond returned error: %v", err)
	}
	parsed2, _ := cy.ParseResponseHeader(platform.ReceivedMessages[1].Data)
	if parsed2.Seqno != 1 {
		t.Errorf("second response seqno mismatch: got %d want 1", parsed2.Seqno)
	}
}
