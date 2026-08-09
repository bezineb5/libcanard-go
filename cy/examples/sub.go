// Package main provides a subscriber example with response capability.
// This subscribes to topics and can optionally respond to messages.
//
// Usage:
//   go run sub.go [topic1] [topic2] ...
//
// Example:
//   go run sub.go sensors/temperature
//   go run sub.go "sensors/*"  # Pattern subscription
package main

import (
	"fmt"
	"os"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

func onMessage(arrival *cy.Arrival) {
	// Print message metadata
	fmt.Printf("💬 '%s' at %.3f s, %d bytes\n",
		arrival.Breadcrumb.Cy.FindTopicByHash(arrival.Breadcrumb.TopicHash).Name(),
		float64(arrival.Message.Timestamp)/1e6,
		len(arrival.Message.Content.Payload()))
	
	// Print payload
	payload := arrival.Message.Content.Payload()
	if len(payload) > 0 {
		fmt.Printf("Payload: % x\n", payload)
	}
	
	// Note: To respond to messages, use the Responder API in RPC callbacks
	fmt.Println()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <topic1> [topic2] ...\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Hint: Use patterns like 'sensors/*' to subscribe to multiple topics\n")
		os.Exit(1)
	}
	topics := os.Args[1:]

	// Create CAN platform
	platform, err := can.New("vcan0", 1000, 4, 42)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create CAN platform: %v\n", err)
		fmt.Fprintf(os.Stderr, "Hint: Create a virtual CAN interface with: sudo ip link add dev vcan0 type vcan\n")
		os.Exit(1)
	}
	defer platform.Destroy()

	// Create Cy instance
	node, err := cy.New(platform, "sub_node", "", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create Cy instance: %v\n", err)
		os.Exit(1)
	}
	defer node.Destroy()

	// Subscribe to all topics
	subscribers := make([]*cy.Subscriber, len(topics))
	for i, topic := range topics {
		sub, err := node.Subscribe(topic, 1024)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to subscribe to '%s': %v\n", topic, err)
			os.Exit(1)
		}
		subscribers[i] = sub
		fmt.Printf("Subscribed to: '%s'\n", topic)
		
		// Set callback for incoming messages
		sub.SetCallback(onMessage)
	}
	defer func() {
		for _, sub := range subscribers {
			sub.Destroy()
		}
	}()

	fmt.Println("Waiting for messages... (Ctrl+C to exit)")

	// Spin the event loop
	for {
		err := node.Spin(node.Now() + 100000) // Spin for 100ms
		if err != nil {
			fmt.Fprintf(os.Stderr, "Spin error: %v\n", err)
			os.Exit(1)
		}
	}
}
