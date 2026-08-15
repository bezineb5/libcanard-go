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

	// With no known associations the total count is 0; the future still tracks
	// an ACK (e.g. from a late subscriber) and completes on the first ACK.
	if future.TotalCount() < 0 {
		t.Error("Expected non-negative total count")
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

	// Publish reliable with no known associations (a publisher with zero
	// subscribers). Under the faithful C model the future completes OK on the
	// first ACK.
	future := pub.PublishReliable(node.Now()+1000000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()

	// Initially, nothing acknowledged, not done.
	if future.AckedCount() != 0 {
		t.Errorf("Expected acked count 0, got %d", future.AckedCount())
	}
	if future.Done() {
		t.Error("Expected future not done before ACK")
	}
	// Total count reflects known associations (zero here).
	if future.TotalCount() != 0 {
		t.Errorf("Expected total count 0 (no associations), got %d", future.TotalCount())
	}

	// Route an ACK via the real wire path (header_msg_ack carries the topic
	// hash at [8:16] and the message tag at [16:24]); the Cy instance must
	// route it to this publisher and complete the future.
	ack := cy.NewACKHeader(true, pub.Topic().Hash(), future.Tag())
	ackTS := cy.NewMessageTS(node.Now()+1, cy.NewMessage(ack.MarshalBinary()))
	node.HandleMessage(cy.Lane{ID: 7}, nil, *ackTS)
	if !future.Done() {
		t.Error("Expected future done after first ACK with no associations")
	}
	if future.Error() != cy.OK {
		t.Errorf("Expected OK, got %v", future.Error())
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

// TestPublicationFutureNackIsGap verifies that a NACK is treated as a gap to be
// repaired by retransmission: it must NOT complete or fail the future (faithful
// C model), and the association stays outstanding.
func TestPublicationFutureNackIsGap(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()

	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()

	pub.AddAssociation(1)
	future := pub.PublishReliable(node.Now()+1000000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()

	// Deliver a NACK from the known association. The future must remain pending
	// (NACK is a gap signal, not a terminal error).
	nack := cy.NewACKHeader(false, pub.Topic().Hash(), future.Tag())
	nackTS := cy.NewMessageTS(node.Now()+1, cy.NewMessage(nack.MarshalBinary()))
	node.HandleMessage(cy.Lane{ID: 1}, nil, *nackTS)
	if future.Done() {
		t.Error("NACK must not complete or fail the future")
	}

	// A subsequent ACK from the same association completes it with OK.
	ack := cy.NewACKHeader(true, pub.Topic().Hash(), future.Tag())
	ackTS := cy.NewMessageTS(node.Now()+2, cy.NewMessage(ack.MarshalBinary()))
	node.HandleMessage(cy.Lane{ID: 1}, nil, *ackTS)
	if !future.Done() {
		t.Error("Expected future done after ACK")
	}
	if future.Error() != cy.OK {
		t.Errorf("Expected OK, got %v", future.Error())
	}
}

// TestReliableDeliveryMultiAssociationCompletion verifies that a publication
// with known associations completes only after every known association ACKs.
func TestReliableDeliveryMultiAssociationCompletion(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()

	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()

	pub.AddAssociation(1)
	pub.AddAssociation(2)
	pub.AddAssociation(3)

	future := pub.PublishReliable(node.Now()+1000000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()

	if future.TotalCount() != 3 {
		t.Fatalf("Expected total count 3, got %d", future.TotalCount())
	}

	// Acknowledge only two of the three.
	for _, rid := range []uint64{1, 2} {
		ack := cy.NewACKHeader(true, pub.Topic().Hash(), future.Tag())
		ts := cy.NewMessageTS(node.Now()+1, cy.NewMessage(ack.MarshalBinary()))
		node.HandleMessage(cy.Lane{ID: rid}, nil, *ts)
	}
	if future.Done() {
		t.Error("Future must not complete until all associations ACK")
	}
	if future.AckedCount() != 2 {
		t.Errorf("Expected acked count 2, got %d", future.AckedCount())
	}

	// Third ACK completes.
	ack := cy.NewACKHeader(true, pub.Topic().Hash(), future.Tag())
	ts := cy.NewMessageTS(node.Now()+2, cy.NewMessage(ack.MarshalBinary()))
	node.HandleMessage(cy.Lane{ID: 3}, nil, *ts)
	if !future.Done() {
		t.Error("Expected future done after all associations ACK")
	}
	if future.Error() != cy.OK {
		t.Errorf("Expected OK, got %v", future.Error())
	}
}

// TestReliableDeliveryUnicastExtent verifies that advertising a client publisher
// grows the node's incoming unicast extent so large reliable responses fit.
func TestReliableDeliveryUnicastExtent(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()

	if node.UnicastExtent() != 0 {
		t.Fatalf("Expected initial unicast extent 0, got %d", node.UnicastExtent())
	}
	topic, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}

	const responseExtent = 1024
	pub := cy.NewClientPublisher(node, topic.Topic(), responseExtent)
	defer pub.Destroy()

	// Extent must be responseExtent + HeaderSize (C uses max).
	want := responseExtent + cy.HeaderSize
	if node.UnicastExtent() != want {
		t.Errorf("Expected unicast extent %d, got %d", want, node.UnicastExtent())
	}
}

// TestReliableDeliveryDeadlineTimeout verifies that a publication that is never
// acknowledged materializes as ErrDelivery at the publish deadline.
func TestReliableDeliveryDeadlineTimeout(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()

	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()

	pub.AddAssociation(1)
	future := pub.PublishReliable(node.Now()+1000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()

	// Advance the scheduler past the deadline (no ACK delivered).
	node.SpinUntil(node.Now() + 5000)
	if !future.Done() {
		t.Error("Expected future done after deadline with no ACK")
	}
	if future.Error() != cy.ErrDelivery {
		t.Errorf("Expected ErrDelivery, got %v", future.Error())
	}
}

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
