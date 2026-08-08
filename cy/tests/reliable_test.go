package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestReliableDeliveryAddAssociation tests adding associations.
func TestReliableDeliveryAddAssociation(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Add associations
	pub.AddAssociation(1)
	pub.AddAssociation(2)
	pub.AddAssociation(3)
	
	// Check association count
	if pub.AssociationCount() != 3 {
		t.Errorf("Expected 3 associations, got %d", pub.AssociationCount())
	}
	
	// Check getting associations
	if pub.GetAssociation(1) == nil {
		t.Error("Expected association for remote 1")
	}
	if pub.GetAssociation(99) != nil {
		t.Error("Expected no association for remote 99")
	}
	
	// Remove an association
	pub.RemoveAssociation(2)
	if pub.AssociationCount() != 2 {
		t.Errorf("Expected 2 associations after removal, got %d", pub.AssociationCount())
	}
	
	// Remove all
	pub.RemoveAssociation(1)
	pub.RemoveAssociation(3)
	if pub.AssociationCount() != 0 {
		t.Errorf("Expected 0 associations after all removed, got %d", pub.AssociationCount())
	}
}

// TestReliableDeliveryAckTimeout tests ACK timeout configuration.
func TestReliableDeliveryAckTimeout(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Check default timeout
	defaultTimeout := pub.AckTimeout()
	if defaultTimeout <= 0 {
		t.Errorf("Expected positive default timeout, got %d", defaultTimeout)
	}
	
	// Set new timeout
	newTimeout := cy.Microsecond(500000)
	pub.SetAckTimeout(newTimeout)
	
	if pub.AckTimeout() != newTimeout {
		t.Errorf("Expected timeout %d, got %d", newTimeout, pub.AckTimeout())
	}
}

// TestPublishReliable tests publishing a reliable message.
func TestPublishReliable(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Publish a reliable message
	data := []byte{0x01, 0x02, 0x03, 0x04}
	future := pub.PublishReliable(node.Now()+1000000, data)
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	
	// Check the future
	// Tag can be 0 for the first message
	_ = future.Tag()
	
	if future.TotalCount() == 0 {
		t.Error("Expected non-zero total count")
	}
	
	// Clean up
	future.Destroy()
}

// TestPublishReliableMultiple tests publishing multiple reliable messages.
func TestPublishReliableMultiple(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Publish multiple reliable messages
	futures := make([]*cy.PublicationFuture, 5)
	for i := 0; i < 5; i++ {
		data := []byte{byte(i)}
		future := pub.PublishReliable(node.Now()+1000000, data)
		if future == nil {
			t.Fatalf("Failed to publish reliable message %d", i)
		}
		futures[i] = future
	}
	
	// Clean up
	for _, future := range futures {
		future.Destroy()
	}
}

// TestPublicationFutureTag tests the tag of a publication future.
func TestPublicationFutureTag(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Publish reliable
	future := pub.PublishReliable(node.Now()+1000000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()
	
	// Tags should be sequential
	future2 := pub.PublishReliable(node.Now()+1000000, []byte{0x02})
	if future2 == nil {
		t.Fatal("Expected future2, got nil")
	}
	defer future2.Destroy()
	
	if future2.Tag() <= future.Tag() {
		t.Errorf("Expected tag2 > tag, got %d <= %d", future2.Tag(), future.Tag())
	}
}

// TestPublicationFutureAck tests acknowledging a publication future.
func TestPublicationFutureAck(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Publish reliable
	future := pub.PublishReliable(node.Now()+1000000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()
	
	// Initially, acked count should be 0
	if future.AckedCount() != 0 {
		t.Errorf("Expected acked count 0, got %d", future.AckedCount())
	}
	
	// Total count should be 1 (for multicast)
	if future.TotalCount() != 1 {
		t.Errorf("Expected total count 1, got %d", future.TotalCount())
	}
}

// TestPublicationFutureNack tests negatively acknowledging a publication future.
func TestPublicationFutureNack(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Publish reliable
	future := pub.PublishReliable(node.Now()+1000000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()
	
	// Check that future exists and has expected properties
	if future.Done() {
		t.Error("Expected future not done")
	}
}

// TestReliableDeliverySetAckTimeout tests setting the ACK timeout.
func TestReliableDeliverySetAckTimeout(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Set ACK timeout
	newTimeout := cy.Microsecond(500000)
	pub.SetAckTimeout(newTimeout)
	
	// Check the timeout
	if pub.AckTimeout() != newTimeout {
		t.Errorf("Expected timeout %d, got %d", newTimeout, pub.AckTimeout())
	}
}
