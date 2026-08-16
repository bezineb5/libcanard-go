# Cy-go: Cyphal v1.1 Implementation in Go

[![Cyphal](https://opencyphal.org/favicon-192.png)](https://opencyphal.org/)

**Cy-go** is a Go implementation of [Cyphal v1.1](https://opencyphal.org/) - a robust decentralized pub/sub protocol with tunable reliability and service discovery.

This is a migration of the [OpenCyphal-Garage/cy](https://github.com/OpenCyphal-Garage/cy) C library to Go, building upon the [libcanard-go](../) foundation.

## ✅ Features

- ✅ Platform- and transport-agnostic pub/sub API
- ✅ Automatic topic allocation using **CRDT** (Conflict-free Replicated Data Type)
- ✅ Reliable message delivery with ACK/NACK
- ✅ RPC and streaming support
- ✅ Pattern matching subscriptions
- ✅ Service discovery via gossip protocol
- ✅ **CAN transport** (SocketCAN) - *Complete*
- ✅ **UDP transport** - *Complete*
- ✅ Fault injection framework for testing
- ✅ Comprehensive test suite (240+ tests)

## Installation

```bash
go get github.com/opencyphal/cy-go
```

## Quick Start

### Basic Publisher

```go
package main

import (
	"fmt"
	"time"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

func main() {
	// Create CAN platform
	platform, err := can.New("can0", 1000, 4, 42)
	if err != nil {
		panic(err)
	}
	defer platform.Destroy()

	// Create Cy instance
	node, err := cy.New(platform, "my_node", "", "")
	if err != nil {
		panic(err)
	}
	defer node.Destroy()

	// Advertise a topic
	pub, err := node.Advertise("sensors/temperature")
	if err != nil {
		panic(err)
	}
	defer pub.Destroy()

	// Publish a message
	data := []byte{0x01, 0x02, 0x03, 0x04}
	deadline := node.Now() + 100000 // 100ms from now
	err = pub.Publish(deadline, data)
	if err != nil {
		fmt.Println("Publish error:", err)
	}

	// Spin the event loop
	for {
		err := node.Spin(node.Now() + 10000) // Spin for 10ms
		if err != nil {
			fmt.Println("Spin error:", err)
			break
		}
	}
}
```

### Basic Subscriber

```go
package main

import (
	"fmt"
	"time"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

func onMessage(arrival *cy.Arrival) {
	fmt.Printf("Received %d bytes on subject %d at %d us\n",
		len(arrival.Message.Content.Payload()),
		arrival.Message.SubjectID,
		arrival.Timestamp)
}

func main() {
	// Create platform and Cy instance
	platform, err := can.New("can0", 1000, 4, 42)
	if err != nil {
		panic(err)
	}
	defer platform.Destroy()

	node, err := cy.New(platform, "my_node", "", "")
	if err != nil {
		panic(err)
	}
	defer node.Destroy()

	// Subscribe to a topic
	sub, err := node.Subscribe("sensors/temperature", 1024)
	if err != nil {
		panic(err)
	}
	defer sub.Destroy()

	// Set callback
	sub.SetCallback(onMessage)

	// Spin the event loop
	for {
		err := node.Spin(node.Now() + 10000)
		if err != nil {
			fmt.Println("Spin error:", err)
			break
		}
	}
}
```

### RPC Server

```go
package main

import (
	"fmt"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

// Define a service type
const MyServiceType = 0x123

// Define request and response types
type MyRequest struct {
	Param1 uint32
	Param2 uint32
}

type MyResponse struct {
	Result uint32
}

func handleMyService(request []byte, responder cy.Responder) {
	// Parse request
	// ... parsing logic ...
	
	// Compute response
	response := []byte{0x01, 0x02, 0x03, 0x04}
	
	// Send response
	responder.Respond(response)
}

func main() {
	platform, _ := can.New("can0", 1000, 4, 42)
	defer platform.Destroy()

	node, _ := cy.New(platform, "rpc_server", "", "")
	defer node.Destroy()

	// Register service
	err := node.RPC().Register(MyServiceType, handleMyService)
	if err != nil {
		panic(err)
	}

	// Spin
	for {
		node.Spin(node.Now() + 10000)
	}
}
```

## Package Structure

```
cy/
├── Core Session Layer
│   ├── types.go            # Basic types and constants
│   ├── cy.go               # Main Cy instance
│   ├── diag.go             # Diagnostics (per-node health/status)
│   ├── name.go             # Node naming / service discovery
│   ├── topic.go            # Topic management
│   ├── platform.go         # Platform abstraction
│   ├── message.go          # Message handling
│   ├── future.go           # Future/promise pattern
│   ├── publisher.go        # Publisher
│   ├── subscriber.go       # Subscriber
│   ├── header.go           # Message header encoding
│   ├── gossip.go           # Gossip protocol
│   ├── crdt.go             # CRDT topic allocation
│   ├── reliable.go         # Reliable delivery (ACK/NACK)
│   ├── rpc.go              # RPC and streaming
│   └── pattern.go          # Pattern matching
│
├── Transport Layers
│   ├── can/
│   │   ├── platform.go     # CAN platform implementation
│   │   └── tests/          # CAN-specific tests
│   │
│   └── udp/
│       ├── platform.go     # UDP platform implementation
│       └── tests/          # UDP-specific tests
│
├── External Dependencies (Ported from C)
│   ├── cavl/
│   │   ├── tree.go         # AVL tree implementation
│   │   └── tests/          # AVL tree tests (12 tests)
│   └── olga/
│   │   ├── scheduler.go    # Event scheduler
│   │   └── tests/          # Scheduler tests (18 tests)
│
└── Tests
    ├── test_support.go     # MockPlatform, SimNetwork
    ├── fault_injection.go  # Fault injection framework
    ├── crdt_test.go        # CRDT tests (14 tests)
    ├── integration_*.go     # Integration tests
    ├── reliable_*.go        # Reliable delivery tests
    ├── rpc_*.go             # RPC tests
    ├── pubsub_test.go      # Pub/sub tests
    └── ...
```

## Test Results

```bash
# Run all tests
cd cy && go test ./... -timeout 30s

# Results: 240+ tests passing ✅
```

## Documentation

The planning and migration notes referenced during the C->Go port (CY_*.md, Cyphal_Specification.md) are no longer part of this repository. For usage details, consult the package source under `cy/` and the tests in `cy/tests` and `cy/*/tests`.


## Building

```bash
# Build all packages
cd cy && go build ./...

# Run tests
cd cy && go test ./... -timeout 30s
```

## Platform Support

### CAN Transport
- ✅ SocketCAN (Linux)
- ✅ Virtual CAN (vcan0 for testing)
- ✅ Classic CAN and CAN FD

### UDP Transport
- ✅ IPv4 multicast
- ✅ Manual endpoint configuration
- ✅ POSIX-compatible



## License

MIT License - see [LICENSE](../../LICENSE) for details.
