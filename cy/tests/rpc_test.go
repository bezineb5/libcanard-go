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
