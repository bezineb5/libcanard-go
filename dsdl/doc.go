// Package dsdl provides DSDL (DroneCAN Schema Definition Language) serialization
// and deserialization for Go structs using struct tags.
//
// This package implements a fast, tag-based serializer that enables Go structs to be
// serialized to and deserialized from DSDL-compatible byte representations, as used
// by the Cyphal/CAN protocol.
//
// # Features
//
//   - Types are inferred from Go types (no need to repeat in tags)
//   - Offsets are auto-computed by default (sequential packing)
//   - Explicit offsets can be specified when needed (e.g., for padding)
//   - Cached struct layouts for lightning-fast serialization
//   - Zero allocations for unmarshal and pre-allocated marshal
//   - Support for primitive types and fixed-size arrays
//   - Little-endian encoding (Cyphal standard)
//
// # Installation
//
//	go get github.com/benjamin/libcanard-go/dsdl
//
// # Usage
//
// Define your message types with minimal tags:
//
//	// Option 1: Fully automatic (recommended for most cases)
//	type Heartbeat struct {
//	    Uptime  uint64
//	    Health  uint8
//	    Mode    uint8
//	}
//
//	// Option 2: Explicit offsets (for padding or specific layouts)
//	type Status struct {
//	    Timestamp uint64 `dsdl:"0"`
//	    _        [3]byte `dsdl:"8"`  // padding
//	    Flags    uint8  `dsdl:"11"` // explicit offset
//	}
//
// Serialize to bytes:
//
//	hb := Heartbeat{Uptime: 12345678, Health: 1, Mode: 4}
//	data, err := dsdl.Marshal(hb)
//
// Deserialize from bytes:
//
//	var hb2 Heartbeat
//	err = dsdl.Unmarshal(data, &hb2)
//
// # Performance
//
// The package caches struct layouts on first use, so subsequent operations are
// extremely fast with no reflection overhead:
//
//   - First marshal: ~320 ns (includes cache population)
//   - Cached marshal: ~24 ns (just buffer allocation)
//   - Unmarshal: ~23 ns (zero allocations)
//   - MarshalTo: ~18 ns (zero allocations)
//
// This makes it suitable for real-time embedded systems.
//
// # Tag Format
//
// The DSDL tag is optional:
//
//   - No tag: Sequential packing, type inferred from Go type
//   - Tag with offset only (e.g., `dsdl:"8"`): Use that offset, type inferred
//   - Tag with offset and type (e.g., `dsdl:"8,uint8"`): Use that offset and type
//
// Most fields won't need tags - just define the struct in DSDL field order.
//
// # Supported Types
//
// Primitive types (inferred from Go types):
//
//   - bool (1 byte)
//   - int8, int16, int32, int64 (1, 2, 4, 8 bytes)
//   - uint8, uint16, uint32, uint64 (1, 2, 4, 8 bytes)
//   - float32, float64 (4, 8 bytes)
//
// Fixed-size arrays:
//
//   - [N]bool, [N]int8-64, [N]uint8-64, [N]float32-64
//
// All values are encoded in **little-endian** format (Cyphal standard).
//
// # Integration with libcanard-go
//
// This package is designed to work seamlessly with libcanard-go:
//
//	import (
//	    "github.com/benjamin/libcanard-go"
//	    "github.com/benjamin/libcanard-go/dsdl"
//	)
//
//	type Heartbeat struct {
//	    Uptime uint64
//	    Health uint8
//	}
//
//	func main() {
//	    canard := &libcanard.Canard{}
//	    // ... initialize ...
//
//	    hb := Heartbeat{Uptime: 12345678, Health: 1}
//	    payload := dsdl.Marshal(hb)
//
//	    canard.Publish16b(1000000, libcanard.IfaceBitmapAll,
//	                      libcanard.PrioNominal, 750, 0, payload, nil)
//	}
//
// # Comparison with Other Approaches
//
// Code Generation vs. Tag-Based:
//
//   - Tag-based (this package): Easier to use, slightly slower first call
//   - Code generation: Faster, but requires build step
//
// For most use cases, the tag-based approach with caching provides an excellent
// balance of convenience and performance.
//
// # Thread Safety
//
// This package is safe for concurrent use. The struct layout cache uses sync.Map
// internally, and marshal/unmarshal operations on cached layouts are read-only.
package dsdl
