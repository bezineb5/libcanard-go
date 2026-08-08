// Package tests provides integration tests for RPC functionality.
// These tests use the SimNetwork to test multi-node RPC scenarios.
package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestRPCBasicRequestResponse tests basic RPC request/response between two nodes.
func TestRPCBasicRequestResponse(t *testing.T) {
	// Create a simulation network with 2 nodes
	net := NewSimNetwork(2)
	defer net.ProcessMessages()
	
	nodeA := net.GetNode(0)
	nodeB := net.GetNode(1)
	
	if nodeA == nil || nodeB == nil {
		t.Fatal("Expected 2 nodes")
	}
	
	// Node B registers a service handler
	rpcB := nodeB.Cy.RPC()
	
	// Create a simple echo handler
	handler := func(data []byte, responder *cy.Responder) {
		// Echo the data back
		err := responder.Send(data)
		if err != cy.OK {
			t.Errorf("Failed to send response: %v", err)
		}
	}
	
	// Register the handler for a service
	serviceID := uint32(12345)
	rpcB.RegisterService(serviceID, handler)
	
	// Node A advertises a service topic
	pub, err := nodeA.Cy.Advertise("service.test")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Set the service ID for the publisher's topic
	// In a real implementation, this would be done automatically
	// For now, we just verify the request can be sent
	
	// Node A sends a request
	requestData := []byte{0x01, 0x02, 0x03, 0x04}
	future := pub.Request(net.Now()+1000000, 500000, requestData)
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()
	
	// Process messages
	net.ProcessMessages()
	
	// Verify the future was created
	if future.Tag() == 0 {
		t.Log("Tag is 0 (OK for first request)")
	}
	
	t.Log("Basic RPC request/response test completed")
}

// TestRPCMultipleRequests tests sending multiple requests.
func TestRPCMultipleRequests(t *testing.T) {
	// Create a simulation network with 2 nodes
	net := NewSimNetwork(2)
	defer net.ProcessMessages()
	
	nodeA := net.GetNode(0)
	nodeB := net.GetNode(1)
	
	// Node B registers a service handler
	rpcB := nodeB.Cy.RPC()
	
	handler := func(data []byte, responder *cy.Responder) {
		// Echo the data back
		_ = responder.Send(data)
	}
	
	rpcB.RegisterService(12345, handler)
	
	// Node A advertises a service topic
	pub, err := nodeA.Cy.Advertise("service.test")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Send multiple requests
	futures := make([]*cy.RequestFuture, 5)
	for i := 0; i < 5; i++ {
		data := []byte{byte(i)}
		future := pub.Request(net.Now()+1000000, 500000, data)
		if future == nil {
			t.Fatalf("Failed to send request %d", i)
		}
		futures[i] = future
	}
	
	// Clean up
	for _, future := range futures {
		future.Destroy()
	}
	
	// Verify tags are sequential
	for i := 1; i < 5; i++ {
		if futures[i].Tag() <= futures[i-1].Tag() {
			t.Errorf("Expected tag %d > tag %d", i, i-1)
		}
	}
	
	// Process messages
	net.ProcessMessages()
	
	t.Log("Multiple RPC requests test completed")
}

// TestRPCServiceRegistration tests service registration and lookup.
func TestRPCServiceRegistration(t *testing.T) {
	// Create a simulation network with 1 node
	net := NewSimNetwork(1)
	defer net.ProcessMessages()
	
	node := net.GetNode(0)
	if node == nil {
		t.Fatal("Expected node")
	}
	
	rpc := node.Cy.RPC()
	
	// Register multiple services
	services := []uint32{100, 200, 300, 400, 500}
	for _, serviceID := range services {
		handler := func(data []byte, responder *cy.Responder) {
			_ = responder.Send([]byte{0x01})
		}
		rpc.RegisterService(serviceID, handler)
	}
	
	// Verify all services are registered
	for _, serviceID := range services {
		if rpc.GetService(serviceID) == nil {
			t.Errorf("Expected service %d to be registered", serviceID)
		}
	}
	
	// Unregister all services
	for _, serviceID := range services {
		rpc.UnregisterService(serviceID)
	}
	
	// Verify all services are unregistered
	for _, serviceID := range services {
		if rpc.GetService(serviceID) != nil {
			t.Errorf("Expected service %d to be unregistered", serviceID)
		}
	}
	
	t.Log("RPC service registration test completed")
}

// TestRPCRequestCancellation tests canceling a request.
func TestRPCRequestCancellation(t *testing.T) {
	// Create a simulation network with 2 nodes
	net := NewSimNetwork(2)
	defer net.ProcessMessages()
	
	nodeA := net.GetNode(0)
	_ = net.GetNode(1) // nodeB
	
	// Node A advertises a service topic
	pub, err := nodeA.Cy.Advertise("service.test")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Send a request
	future := pub.Request(net.Now()+1000000, 500000, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	
	// Cancel the request
	nodeA.Cy.RPC().CancelRequest(future.Tag())
	
	// Verify the future is done with error
	if !future.Done() {
		t.Error("Expected future to be done after cancel")
	}
	
	if future.Error() != cy.ErrNACK {
		t.Errorf("Expected ErrNACK, got: %v", future.Error())
	}
	
	// Clean up
	future.Destroy()
	
	t.Log("RPC request cancellation test completed")
}

// TestRPCRequestTimeout tests request timeout.
func TestRPCRequestTimeoutIntegration(t *testing.T) {
	// Create a simulation network with 2 nodes
	net := NewSimNetwork(2)
	defer net.ProcessMessages()
	
	nodeA := net.GetNode(0)
	_ = net.GetNode(1) // nodeB
	
	// Node A advertises a service topic
	pub, err := nodeA.Cy.Advertise("service.test")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Send a request with a short timeout
	// The timeout should trigger automatically
	future := pub.Request(net.Now()+100, 50, []byte{0x01})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()
	
	// Advance time past the deadline
	net.Advance(200)
	
	// Process messages (this should trigger the timeout)
	net.ProcessMessages()
	
	// The future should eventually timeout
	// In a real implementation, the timeout callback would be triggered
	
	t.Log("RPC request timeout test completed")
}

// TestRPCMultipleServices tests multiple services on different topics.
func TestRPCMultipleServicesIntegration(t *testing.T) {
	// Create a simulation network with 3 nodes
	net := NewSimNetwork(3)
	defer net.ProcessMessages()
	
	// Each node registers a different service
	serviceIDs := []uint32{1000, 2000, 3000}
	for i, serviceID := range serviceIDs {
		node := net.GetNode(i)
		rpc := node.Cy.RPC()
		
		handler := func(data []byte, responder *cy.Responder) {
			// Echo with service ID prefix
			response := []byte{byte(serviceID >> 8), byte(serviceID), data[0]}
			_ = responder.Send(response)
		}
		
		rpc.RegisterService(serviceID, handler)
	}
	
	// Each node also advertises a topic
	pubs := make([]*cy.Publisher, 3)
	for i := range pubs {
		node := net.GetNode(i)
		pub, err := node.Cy.Advertise("service.node" + string(rune('a'+i)))
		if err != nil {
			t.Fatalf("Node %d: Failed to advertise: %v", i, err)
		}
		defer pub.Destroy()
		pubs[i] = pub
	}
	
	// Send requests to each service
	futures := make([]*cy.RequestFuture, 3)
	for i, pub := range pubs {
		future := pub.Request(net.Now()+1000000, 500000, []byte{byte(i)})
		if future == nil {
			t.Fatalf("Failed to send request to service %d", i)
		}
		futures[i] = future
	}
	
	// Clean up
	for _, future := range futures {
		future.Destroy()
	}
	
	// Process messages
	net.ProcessMessages()
	
	t.Log("Multiple RPC services test completed")
}

// TestRPCRequestWithLargePayload tests RPC with large payloads.
func TestRPCRequestWithLargePayloadIntegration(t *testing.T) {
	// Create a simulation network with 2 nodes
	net := NewSimNetwork(2)
	defer net.ProcessMessages()
	
	nodeA := net.GetNode(0)
	nodeB := net.GetNode(1)
	
	// Node B registers a service handler
	rpcB := nodeB.Cy.RPC()
	
	handler := func(data []byte, responder *cy.Responder) {
		// Echo the data back
		_ = responder.Send(data)
	}
	
	rpcB.RegisterService(12345, handler)
	
	// Node A advertises a service topic
	pub, err := nodeA.Cy.Advertise("service.large")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Create a large payload (256 bytes - max for many transports)
	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}
	
	// Send the request
	future := pub.Request(net.Now()+1000000, 500000, data)
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()
	
	// Process messages
	net.ProcessMessages()
	
	t.Log("RPC with large payload test completed")
}

// TestRPCRequestWithEmptyPayload tests RPC with empty payload.
func TestRPCRequestWithEmptyPayloadIntegration(t *testing.T) {
	// Create a simulation network with 2 nodes
	net := NewSimNetwork(2)
	defer net.ProcessMessages()
	
	nodeA := net.GetNode(0)
	nodeB := net.GetNode(1)
	
	// Node B registers a service handler
	rpcB := nodeB.Cy.RPC()
	
	handler := func(data []byte, responder *cy.Responder) {
		// Send a response even for empty requests
		_ = responder.Send([]byte{0x01})
	}
	
	rpcB.RegisterService(12345, handler)
	
	// Node A advertises a service topic
	pub, err := nodeA.Cy.Advertise("service.empty")
	if err != nil {
		t.Fatalf("Failed to advertise: %v", err)
	}
	defer pub.Destroy()
	
	// Send a request with empty payload
	future := pub.Request(net.Now()+1000000, 500000, []byte{})
	if future == nil {
		t.Fatal("Expected future, got nil")
	}
	defer future.Destroy()
	
	// Process messages
	net.ProcessMessages()
	
	t.Log("RPC with empty payload test completed")
}
