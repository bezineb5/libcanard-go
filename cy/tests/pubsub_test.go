package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestAdvertise tests advertising a topic.
func TestAdvertise(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise a topic
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	
	if pub == nil {
		t.Error("Expected publisher, got nil")
	}
	
	// Check topic
	if pub.Topic() == nil {
		t.Error("Expected topic, got nil")
	}
	
	// Clean up
	pub.Destroy()
}

// TestSubscribe tests subscribing to a topic.
func TestSubscribe(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Subscribe to a topic
	sub, err := node.Subscribe("test.topic", 256)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	
	if sub == nil {
		t.Error("Expected subscriber, got nil")
	}
	
	// Clean up
	sub.Destroy()
}

// TestAdvertiseMultiple tests advertising multiple topics.
func TestAdvertiseMultiple(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Advertise multiple topics
	pubs := make([]*cy.Publisher, 5)
	for i := 0; i < 5; i++ {
		pub, err := node.Advertise("topic" + string(rune('a'+i)))
		if err != nil {
			t.Fatalf("Failed to advertise topic %d: %v", i, err)
		}
		pubs[i] = pub
	}
	
	// Clean up
	for _, pub := range pubs {
		pub.Destroy()
	}
}

// TestSubscribeMultiple tests subscribing to multiple topics.
func TestSubscribeMultiple(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Subscribe to multiple topics
	subs := make([]*cy.Subscriber, 5)
	for i := 0; i < 5; i++ {
		sub, err := node.Subscribe("topic"+string(rune('a'+i)), 256)
		if err != nil {
			t.Fatalf("Failed to subscribe to topic %d: %v", i, err)
		}
		subs[i] = sub
	}
	
	// Clean up
	for _, sub := range subs {
		sub.Destroy()
	}
}

// TestAdvertiseSubscribeSameTopic tests subscribing before advertising.
func TestAdvertiseSubscribeSameTopic(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Subscribe first
	sub, err := node.Subscribe("test.topic", 256)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Destroy()
	
	// Then advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Should have same topic
	if pub.Topic() != sub.Topic() {
		t.Error("Publisher and subscriber have different topics")
	}
}

// TestPublisherDestroy tests destroying a publisher.
func TestPublisherDestroy(t *testing.T) {
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
	
	// Destroy twice should not panic
	pub.Destroy()
	pub.Destroy()
}

// TestSubscriberDestroy tests destroying a subscriber.
func TestSubscriberDestroy(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	// Subscribe
	sub, err := node.Subscribe("test.topic", 256)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	
	// Destroy twice should not panic
	sub.Destroy()
	sub.Destroy()
}

// TestPublish tests publishing a message.
func TestPublish(t *testing.T) {
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
	
	// Publish a message
	data := []byte{0x01, 0x02, 0x03, 0x04}
	err = pub.Publish(node.Now()+1000000, data)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}
}

// TestPublishMultiple tests publishing multiple messages.
func TestPublishMultiple(t *testing.T) {
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
	
	// Publish multiple messages
	for i := 0; i < 10; i++ {
		data := []byte{byte(i)}
		err := pub.Publish(node.Now()+1000000, data)
		if err != nil {
			t.Errorf("Failed to publish message %d: %v", i, err)
		}
	}
}

// TestPublishEmpty tests publishing an empty message.
func TestPublishEmpty(t *testing.T) {
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
	
	// Publish empty message
	err = pub.Publish(node.Now()+1000000, []byte{})
	if err != nil {
		t.Fatalf("Failed to publish empty message: %v", err)
	}
}

// TestPublishLarge tests publishing a large message.
func TestPublishLarge(t *testing.T) {
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
	
	// Publish large message (max extent)
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}
	
	err = pub.Publish(node.Now()+1000000, data)
	if err != nil {
		t.Fatalf("Failed to publish large message: %v", err)
	}
}

// TestNodeDestroy tests destroying a node.
func TestNodeDestroy(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	
	// Advertise
	pub, err := node.Advertise("test.topic")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	
	// Subscribe
	sub, err := node.Subscribe("test.topic", 256)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	
	// Destroy everything
	pub.Destroy()
	sub.Destroy()
	node.Destroy()
	
	// Destroy again should not panic
	node.Destroy()
}

// TestMultipleNodes tests creating multiple nodes.
func TestMultipleNodes(t *testing.T) {
	// Create two mock platforms
	platform1 := NewMockPlatform()
	platform2 := NewMockPlatform()
	
	// Create two nodes
	node1, err := cy.New(platform1, "node1", "", "")
	if err != nil {
		t.Fatalf("Failed to create node1: %v", err)
	}
	defer node1.Destroy()
	
	node2, err := cy.New(platform2, "node2", "", "")
	if err != nil {
		t.Fatalf("Failed to create node2: %v", err)
	}
	defer node2.Destroy()
	
	// Both should work independently
	pub1, err := node1.Advertise("node1.topic")
	if err != nil {
		t.Fatalf("Failed to advertise for node1: %v", err)
	}
	defer pub1.Destroy()
	
	pub2, err := node2.Advertise("node2.topic")
	if err != nil {
		t.Fatalf("Failed to advertise for node2: %v", err)
	}
	defer pub2.Destroy()
}
