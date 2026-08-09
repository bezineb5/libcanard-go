// Package main provides a simple echo subscriber example.
// This subscribes to a topic and prints received messages.
//
// Usage:
//   go run echo.go [topic]
//
// Example:
//   go run echo.go sensors/temperature
package main

import (
	"fmt"
	"os"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

func onMessage(arrival *cy.Arrival) {
	// Print message metadata
	fmt.Printf("📩 '%s' at %.3f s, %d bytes\n",
		arrival.Breadcrumb.Cy.FindTopicByHash(arrival.Breadcrumb.TopicHash).Name(),
		float64(arrival.Message.Timestamp)/1e6,
		len(arrival.Message.Content.Payload()))
	
	// Print payload as hex dump
	payload := arrival.Message.Content.Payload()
	if len(payload) > 0 {
		fmt.Printf("Payload: % x\n", payload)
	}
	fmt.Println()
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <topic>\n", os.Args[0])
		os.Exit(1)
	}
	topic := os.Args[1]

	// Create CAN platform
	// Use "can0" for real CAN, or "vcan0" for virtual CAN (for testing)
	platform, err := can.New("vcan0", 1000, 4, 42)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create CAN platform: %v\n", err)
		fmt.Fprintf(os.Stderr, "Hint: Create a virtual CAN interface with: sudo ip link add dev vcan0 type vcan\n")
		os.Exit(1)
	}
	defer platform.Destroy()

	// Create Cy instance
	node, err := cy.New(platform, "echo_node", "", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create Cy instance: %v\n", err)
		os.Exit(1)
	}
	defer node.Destroy()

	// Subscribe to the topic
	sub, err := node.Subscribe(topic, 1024)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to '%s': %v\n", topic, err)
		os.Exit(1)
	}
	defer sub.Destroy()

	// Set callback for incoming messages
	sub.SetCallback(onMessage)

	fmt.Printf("Subscribed to '%s'. Waiting for messages...\n", topic)

	// Spin the event loop
	for {
		err := node.Spin(node.Now() + 100000) // Spin for 100ms
		if err != nil {
			fmt.Fprintf(os.Stderr, "Spin error: %v\n", err)
			os.Exit(1)
		}
	}
}
