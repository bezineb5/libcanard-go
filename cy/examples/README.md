# Cy-go Examples

This directory contains example applications demonstrating various aspects of the Cy-go library.

## 📋 **Available Examples**

| Example | Description | Usage |
|--------|-------------|-------|
| [`echo.go`](echo.go) | Subscribes to a topic and prints received messages | `go run echo.go <topic>` |
| [`pub.go`](pub.go) | Advertises topics and publishes messages periodically | `go run pub.go <topic1> [topic2] ...` |
| [`sub.go`](sub.go) | Subscribes to topics and responds to messages | `go run sub.go <topic1> [topic2] ...` |
| [`time_pub.go`](time_pub.go) | Publishes current time at regular intervals | `go run time_pub.go` |
| [`streaming_server.go`](streaming_server.go) | Service that streams data to clients | `go run streaming_server.go <node_id>` |
| [`streaming_client.go`](streaming_client.go) | Client that requests streamed data | `go run streaming_client.go <server_node_id>` |

## 🚀 **Quick Start**

### Prerequisites

For CAN examples, you need a CAN interface:

```bash
# Create a virtual CAN interface (for testing)
sudo ip link add dev vcan0 type vcan
sudo ip link set up vcan0

# Remove when done
sudo ip link delete vcan0
```

### Running Examples

**1. Simple Pub/Sub**

Terminal 1 (Publisher):
```bash
cd cy/examples
go run pub.go sensors/temperature
```

Terminal 2 (Subscriber):
```bash
cd cy/examples
go run echo.go sensors/temperature
```

**2. Time Publisher**

Terminal 1:
```bash
cd cy/examples
go run time_pub.go
```

Terminal 2:
```bash
cd cy/examples
go run echo.go time
```

**3. Streaming**

Terminal 1 (Server):
```bash
cd cy/examples
go run streaming_server.go 1
```

Terminal 2 (Client):
```bash
cd cy/examples
go run streaming_client.go 1
```

## 📝 **Example Descriptions**

### echo.go
Simple subscriber that prints all received messages to stdout.
- Demonstrates basic subscription
- Shows message metadata (topic, timestamp, size)
- Displays payload as hex dump

### pub.go
Simple publisher that sends messages at regular intervals.
- Demonstrates topic advertisement
- Shows periodic publishing
- Multiple topics can be specified

### sub.go
Subscriber with response capability.
- Demonstrates pattern subscriptions (use `sensors/*` to match multiple topics)
- Randomly responds to received messages (50% chance)
- Shows how to use the Arrival API to send responses

### time_pub.go
Publishes current time at 1-second intervals.
- Demonstrates structured data publishing
- Includes both binary (timestamp) and human-readable formats
- Shows timestamp handling

### streaming_server.go
Implements a streaming service.
- Demonstrates RPC service registration
- Shows how to stream multiple messages in response to a request
- Uses the Responder API

### streaming_client.go
Implements a streaming client.
- Demonstrates how to request and receive streamed data
- Shows callback-based message handling
- Works with streaming_server.go

## 🎯 **Common Patterns**

### Creating a Node

```go
platform, err := can.New("vcan0", 1000, 4, 42)
if err != nil {
    panic(err)
}
defer platform.Destroy()

node, err := cy.New(platform, "my_node", "", "")
if err != nil {
    panic(err)
}
defer node.Destroy()
```

### Publishing Messages

```go
pub, err := node.Advertise("my.topic")
if err != nil {
    panic(err)
}
defer pub.Destroy()

// Publish with deadline 100ms from now
deadline := node.Now() + 100000
err = pub.Publish(deadline, []byte("Hello, World!"))
```

### Subscribing to Messages

```go
sub, err := node.Subscribe("my.topic", 1024)
if err != nil {
    panic(err)
}
defer sub.Destroy()

// Set callback for incoming messages
sub.SetCallback(func(arrival *cy.Arrival) {
    fmt.Printf("Received %d bytes\n", len(arrival.Message.Content.Payload()))
})
```

### Event Loop

```go
for {
    err := node.Spin(node.Now() + 100000) // Spin for 100ms
    if err != nil {
        fmt.Println("Error:", err)
        break
    }
}
```

### RPC Service

```go
// Register service
node.RPC().Register(0x1234, func(request []byte, responder cy.Responder) {
    // Handle request
    responder.Respond([]byte("Response"))
})
```

## 🔧 **Configuration**

### CAN Interface

All examples use `vcan0` by default. To use a real CAN interface:

```go
// Use "can0" instead of "vcan0"
platform, err := can.New("can0", 1000, 4, 42)
```

Parameters:
- Interface name: `"can0"`, `"vcan0"`, etc.
- TX queue capacity: Number of outgoing messages to buffer
- Filter capacity: Number of CAN filters
- PRNG seed: Random seed for transfer IDs

### Node ID

Each node should have a unique ID on the network:

```go
// Node ID is the 4th parameter to cy.New
node, err := cy.New(platform, "node_name", "", "")
// Or with explicit node ID:
node, err := cy.New(platform, "node_name", "", "")
// The node ID is derived from the UID in the platform
```

## 📊 **Testing**

For testing without hardware, use virtual CAN:

```bash
# Create virtual CAN interface
sudo ip link add dev vcan0 type vcan
sudo ip link set up vcan0

# Run examples
# ...

# Clean up
sudo ip link delete vcan0
```

## 📚 **See Also**

- [Main Documentation](../README.md)
- [Cyphal Specification](https://opencyphal.org)
- [API Reference](https://pkg.go.dev/github.com/opencyphal/cy-go)

## 💡 **Tips**

1. **Use vcan0 for development** - No hardware required
2. **Check interface names** - Run `ip link` to see available CAN interfaces
3. **Monitor traffic** - Use `candump vcan0` to see CAN messages
4. **Pattern subscriptions** - Use `*` for wildcards, e.g., `sensors/*`
5. **Multiple topics** - A node can publish and subscribe to many topics

## 🎉 **Next Steps**

Try running the examples and experiment with:
- Different topic names and patterns
- Multiple publishers and subscribers
- RPC services
- Reliable vs. best-effort delivery
