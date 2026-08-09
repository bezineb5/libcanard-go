// Package main provides a streaming client example.
// This demonstrates how to request and receive streamed data from a server.
//
// Usage:
//   go run streaming_client.go <server_node_id>
//
// Example:
//   go run streaming_client.go 1
//
// Run streaming_server.go first, then run this client.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

// Service ID for streaming service (must match server)
const StreamingServiceID = 0x1234

func onStreamResponse(arrival *cy.Arrival) {
	// This callback receives stream chunks
	payload := arrival.Message.Content.Payload()
	message := string(payload)
	
	fmt.Printf("📩 Stream chunk: %s\n", message)
	
	// Check if this is the end of stream
	if strings.Contains(message, "10") {
		fmt.Println("✅ Stream complete!")
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <server_node_id>\n", os.Args[0])
		os.Exit(1)
	}
	serverNodeID, _ := strconv.ParseUint(os.Args[1], 10, 64)

	// Create CAN platform
	platform, err := can.New("vcan0", 1000, 4, 42)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create CAN platform: %v\n", err)
		fmt.Fprintf(os.Stderr, "Hint: Create a virtual CAN interface with: sudo ip link add dev vcan0 type vcan\n")
		os.Exit(1)
	}
	defer platform.Destroy()

	// Create Cy instance
	node, err := cy.New(platform, "stream_client", "", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create Cy instance: %v\n", err)
		os.Exit(1)
	}
	defer node.Destroy()

	// Create a request
	request := []byte("Start streaming")
	
	// Send request to the streaming service
	// The service ID determines which service handles the request
	fmt.Printf("📤 Sending stream request to node %d, service 0x%04X\n", serverNodeID, StreamingServiceID)
	
	// Create a subscriber to receive responses
	// In a real application, you'd use a unique subject-ID for the response
	// For this example, we'll use the service ID
	sub, err := node.Subscribe(fmt.Sprintf("%d", StreamingServiceID), 1024)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe: %v\n", err)
		os.Exit(1)
	}
	defer sub.Destroy()
	
	// Set callback for stream responses
	sub.SetCallback(onStreamResponse)
	
	// Send the request
	// Note: In a real implementation, you'd use cy.Request() which handles the response routing
	// For this simplified example, we just publish and expect the server to respond
	pub, err := node.Advertise(fmt.Sprintf("request.%d", StreamingServiceID))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to advertise request topic: %v\n", err)
		os.Exit(1)
	}
	defer pub.Destroy()
	
	// Publish the request
	deadline := node.Now() + 1000000 // 1 second
	err = pub.Publish(deadline, request)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to publish request: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Println("⏳ Waiting for stream response... (Ctrl+C to exit)")

	// Spin the event loop
	for {
		err := node.Spin(node.Now() + 100000) // Spin for 100ms
		if err != nil {
			fmt.Fprintf(os.Stderr, "Spin error: %v\n", err)
			os.Exit(1)
		}
	}
}
