// Package main provides a time publisher example.
// This publishes the current time at regular intervals.
//
// Usage:
//   go run time_pub.go
//
// Example:
//   go run time_pub.go
//
// Subscribe to the "time" topic to receive time updates.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

func main() {
	// Create CAN platform
	platform, err := can.New("vcan0", 1000, 4, 42)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create CAN platform: %v\n", err)
		fmt.Fprintf(os.Stderr, "Hint: Create a virtual CAN interface with: sudo ip link add dev vcan0 type vcan\n")
		os.Exit(1)
	}
	defer platform.Destroy()

	// Create Cy instance
	node, err := cy.New(platform, "time_pub", "", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create Cy instance: %v\n", err)
		os.Exit(1)
	}
	defer node.Destroy()

	// Advertise time topic
	pub, err := node.Advertise("time")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to advertise 'time' topic: %v\n", err)
		os.Exit(1)
	}
	defer pub.Destroy()

	fmt.Println("Publishing time updates every second. Subscribe to 'time' topic.")

	// Publish time at regular intervals
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		cyNow := node.Now()
		
		// Create time message
		// Format: Unix timestamp in microseconds (8 bytes, big-endian)
		timestamp := uint64(now.UnixMicro())
		payload := []byte{
			byte(timestamp >> 56),
			byte(timestamp >> 48),
			byte(timestamp >> 40),
			byte(timestamp >> 32),
			byte(timestamp >> 24),
			byte(timestamp >> 16),
			byte(timestamp >> 8),
			byte(timestamp),
		}
		
		// Also include human-readable format
		humanReadable := fmt.Sprintf("%s\n", now.Format(time.RFC3339Nano))
		fullPayload := append(payload, []byte(humanReadable)...)
		
		// Publish with deadline 1 second from now
		deadline := cyNow + 1000000 // 1 second
		err := pub.Publish(deadline, fullPayload)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to publish time: %v\n", err)
		}
		
		fmt.Printf("📤 Published time: %s\n", now.Format(time.RFC3339))
		
		// Spin to process ACKs and other events
		err = node.Spin(cyNow + 10000) // Spin for 10ms
		if err != nil {
			fmt.Fprintf(os.Stderr, "Spin error: %v\n", err)
		}
	}
}
