// Package main provides a streaming server example.
// This demonstrates how to implement a service that streams data.
//
// Usage:
//   go run streaming_server.go <node_id>
//
// Example:
//   go run streaming_server.go 1234
//
// The server will stream data to any client that requests it.
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

// Service ID for streaming service
const StreamingServiceID = 0x1234

func handleStreamRequest(request []byte, responder *cy.Responder) {
	fmt.Printf("📥 Received stream request, %d bytes\n", len(request))
	
	// Stream 10 messages back to the client
	for i := 0; i < 10; i++ {
		// Create stream data
		data := fmt.Sprintf("Stream chunk %d", i+1)
		
		// Send as part of the stream
		err := responder.Send([]byte(data))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to send stream chunk %d: %v\n", i+1, err)
			return
		}
		
		fmt.Printf("↩️ Sent stream chunk %d: %s\n", i+1, data)
		
		// Small delay between chunks
		time.Sleep(100 * time.Millisecond)
	}
	
	fmt.Println("✅ Stream complete")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <node_id>\n", os.Args[0])
		os.Exit(1)
	}
	nodeID, _ := strconv.ParseUint(os.Args[1], 10, 64)

	// Create CAN platform
	platform, err := can.New("vcan0", 1000, 4, nodeID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create CAN platform: %v\n", err)
		fmt.Fprintf(os.Stderr, "Hint: Create a virtual CAN interface with: sudo ip link add dev vcan0 type vcan\n")
		os.Exit(1)
	}
	defer platform.Destroy()

	// Create Cy instance
	node, err := cy.New(platform, "stream_server", "", "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create Cy instance: %v\n", err)
		os.Exit(1)
	}
	defer node.Destroy()

	// Register streaming service
	node.RPC().RegisterService(StreamingServiceID, handleStreamRequest)
	
	fmt.Printf("Streaming server running on node %d, service ID 0x%04X\n", nodeID, StreamingServiceID)
	fmt.Println("Waiting for stream requests... (Ctrl+C to exit)")

	// Spin the event loop
	for {
		err := node.Spin(node.Now() + 100000) // Spin for 100ms
		if err != nil {
			fmt.Fprintf(os.Stderr, "Spin error: %v\n", err)
			os.Exit(1)
		}
	}
}
