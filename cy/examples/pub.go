// Package main provides a simple publisher example.
// This advertises a topic and publishes messages at regular intervals.
//
// Usage:
//   go run pub.go [topic1] [topic2] ...
//
// Example:
//   go run pub.go sensors/temperature sensors/pressure
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <topic1> [topic2] ...\n", os.Args[0])
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
	node, err := cy.New(platform, "pub_node", "", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create Cy instance: %v\n", err)
		os.Exit(1)
	}
	defer node.Destroy()

	// Advertise all topics
	publishers := make([]*cy.Publisher, len(topics))
	for i, topic := range topics {
		pub, err := node.Advertise(topic)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to advertise '%s': %v\n", topic, err)
			os.Exit(1)
		}
		publishers[i] = pub
		fmt.Printf("Advertised topic: '%s'\n", topic)
	}
	defer func() {
		for _, pub := range publishers {
			pub.Destroy()
		}
	}()

	// Publish messages at regular intervals
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	messageCount := 0
	for range ticker.C {
		messageCount++
		now := node.Now()
		
		for i, pub := range publishers {
			// Create message payload
			payload := fmt.Sprintf("Message %d from %s at %.3fs", 
				messageCount, topics[i], float64(now)/1e6)
			
			// Publish with deadline 1 second from now
			deadline := now + 1000000 // 1 second
			err := pub.Publish(deadline, []byte(payload))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to publish on '%s': %v\n", topics[i], err)
			}
		}
		
		fmt.Printf("Published %d messages\n", messageCount)
		
		// Spin to process ACKs and other events
		err := node.Spin(now + 10000) // Spin for 10ms
		if err != nil {
			fmt.Fprintf(os.Stderr, "Spin error: %v\n", err)
		}
	}
}
