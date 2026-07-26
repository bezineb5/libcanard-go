# DSDL Serializer for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/benjamin/libcanard-go/dsdl.svg)](https://pkg.go.dev/github.com/benjamin/libcanard-go/dsdl)

This package provides **DSDL (DroneCAN Schema Definition Language) serialization** and deserialization for Go structs, designed to work seamlessly with [libcanard-go](../) for Cyphal/CAN protocol implementations.

## ✨ Key Improvements (v2)

Based on your feedback, the API has been redesigned to be **more ergonomic**:

### Before (v1 - Verbose):
```go
type Heartbeat struct {
    Uptime  uint64 `dsdl:"0,uint64"`  // ❌ Why repeat uint64?
    Health  uint8  `dsdl:"8,uint8"`   // ❌ Why repeat uint8?
    Mode    uint8  `dsdl:"9,uint8"`   // ❌ Why repeat uint8?
}
```

### After (v2 - Clean):
```go
type Heartbeat struct {
    Uptime  uint64 // ✅ Type inferred from Go, offset auto-computed
    Health  uint8  // ✅ Type inferred from Go, offset auto-computed
    Mode    uint8  // ✅ Type inferred from Go, offset auto-computed
}
```

**OR with explicit offsets when needed:**
```go
type Status struct {
    Timestamp uint64 `dsdl:"0"`  // ✅ Only offset specified
    _        [3]byte `dsdl:"8"`  // ✅ Padding with explicit offset
    Flags    uint8  `dsdl:"11"` // ✅ Explicit offset
}
```

## Features

- ✅ **Type inference** - No need to repeat types in tags
- ✅ **Auto-computed offsets** - Sequential packing by default
- ✅ **Explicit offsets** - When you need specific layouts (padding, etc.)
- ✅ **Cached struct layouts** - Lightning-fast after first use
- ✅ **Zero allocations** for unmarshal and pre-allocated marshal
- ✅ **Support for primitive types and fixed-size arrays**
- ✅ **Little-endian encoding** (Cyphal standard)

## Installation

```bash
go get github.com/benjamin/libcanard-go/dsdl
```

## Usage

### 1. Define Your Message Types

**Option A: Fully Automatic (Recommended)**
```go
type Heartbeat struct {
    Uptime                 uint64
    Health                 uint8
    Mode                   uint8
    VendorSpecificStatusCode uint16
}
```

**Option B: Explicit Offsets (For Padding)**
```go
type Status struct {
    Timestamp uint64 `dsdl:"0"`
    _        [3]byte `dsdl:"8"`  // padding
    Flags    uint8  `dsdl:"11"` // explicit offset
}
```

### 2. Serialize (Marshal)

```go
// Create a message
hb := Heartbeat{
    Uptime:                 12345678,
    Health:                 1, // NOMINAL
    Mode:                   4, // OPERATIONAL
    VendorSpecificStatusCode: 0,
}

// Marshal to bytes
data, err := dsdl.Marshal(hb)
if err != nil {
    panic(err)
}

// Or marshal to pre-allocated buffer (zero allocations)
buf := make([]byte, dsdl.Size(hb))
err = dsdl.MarshalTo(buf, hb)
```

### 3. Deserialize (Unmarshal)

```go
// Raw data from CAN bus
var hb Heartbeat
err := dsdl.Unmarshal(data, &hb)
if err != nil {
    panic(err)
}

// Now hb contains the deserialized values
fmt.Printf("Health: %d\n", hb.Health)
```

### 4. Integration with libcanard-go

```go
import (
    "github.com/benjamin/libcanard-go"
    "github.com/benjamin/libcanard-go/dsdl"
)

// Publish
func publishHeartbeat(canard *libcanard.Canard) {
    hb := Heartbeat{Uptime: 12345678, Health: 1}
    payload, _ := dsdl.Marshal(hb)
    
    canard.Publish16b(
        1000000,                    // deadline
        libcanard.IfaceBitmapAll,  // interface bitmap
        libcanard.PrioNominal,     // priority
        750,                       // subject ID (Heartbeat)
        0,                          // transfer ID
        payload,                   // payload bytes
        nil,                        // user context
    )
}

// Subscribe
func subscribeToHeartbeat(canard *libcanard.Canard) {
    sub := &libcanard.Subscription{}
    canard.Subscribe16b(sub, 750, 256, 2000000, &libcanard.SubscriptionVTable{
        OnMessage: func(s *libcanard.Subscription, ts int64, prio libcanard.Prio,
                      src uint8, tid uint8, payload libcanard.Payload) {
            data := make([]byte, payload.View.Size)
            copy(data, libcanard.UnsafeSlice(payload.View))
            
            var hb Heartbeat
            if err := dsdl.Unmarshal(data, &hb); err != nil {
                return
            }
            
            fmt.Printf("Received heartbeat from node %d: uptime=%d\n", src, hb.Uptime)
            
            // Free multi-frame payload if needed
            if payload.Origin.Size > 0 && payload.Origin.Data != nil {
                libcanard.MemFree(s.Owner.Mem.RXPayload, payload.Origin.Size, payload.Origin.Data)
            }
        },
    })
}
```

## Tag Format

The DSDL tag is **optional** and supports multiple formats:

| Tag | Meaning |
|-----|---------|
| (no tag) | Sequential packing, type inferred from Go type |
| `dsdl:"8"` | Explicit offset 8, type inferred from Go type |
| `dsdl:"8,uint8"` | Explicit offset 8 and type (rarely needed) |
| `dsdl:"-"` | Skip this field |

**Most fields won't need tags** - just define the struct in DSDL field order!

## Supported Types

### Primitive Types
| Go Type | DSDL Type | Size |
|---------|-----------|------|
| `bool` | `bool` | 1 byte |
| `int8` | `int8` | 1 byte |
| `int16` | `int16` | 2 bytes |
| `int32` | `int32` | 4 bytes |
| `int64` | `int64` | 8 bytes |
| `uint8` / `byte` | `uint8` | 1 byte |
| `uint16` | `uint16` | 2 bytes |
| `uint32` | `uint32` | 4 bytes |
| `uint64` | `uint64` | 8 bytes |
| `float32` | `float32` | 4 bytes |
| `float64` | `float64` | 8 bytes |

### Fixed-Size Arrays
```go
[N]bool      // N bytes
[N]int8     // N bytes
[N]uint8    // N bytes
[N]int16    // 2*N bytes
[N]uint16   // 2*N bytes
[N]float32  // 4*N bytes
[N]float64  // 8*N bytes
```

All values are encoded in **little-endian** format (Cyphal standard).

## Performance

The package caches struct layouts on first use, providing excellent performance:

```
BenchmarkMarshalFirstTime-15    3688086    321.6 ns/op    784 B/op    11 allocs/op
BenchmarkMarshalCached-15     49273725    23.90 ns/op    16 B/op     1 allocs/op
BenchmarkUnmarshal-15         50424934    23.21 ns/op    0 B/op      0 allocs/op
BenchmarkMarshalTo-15         65224480    18.20 ns/op    0 B/op      0 allocs/op
```

- **First marshal**: ~320 ns (includes cache population)
- **Cached marshal**: ~24 ns (just buffer allocation)
- **Unmarshal**: ~23 ns (**zero allocations!**)
- **MarshalTo**: ~18 ns (**zero allocations!**)

This is **suitable for real-time embedded systems**! 🚀

## Answers to Your Questions

### Q: Why do we have to repeat the type?
**A: You don't anymore!** In v2, types are **inferred from Go types**. The tag only needs the offset (if not sequential).

### Q: What is the first number, an offset? Can't it be automatically computed?
**A: Yes!** 
- The number **is** an offset
- It **can** be automatically computed (and is, by default!)
- You only need to specify it when you need padding or a non-sequential layout

### Examples:

**Fully automatic (most common):**
```go
type Point3D struct {
    X float32 // offset 0 (auto)
    Y float32 // offset 4 (auto)
    Z float32 // offset 8 (auto)
}
```

**With padding:**
```go
type Status struct {
    Timestamp uint64 `dsdl:"0"`  // offset 0
    _        [3]byte `dsdl:"8"`  // padding at offset 8
    Flags    uint8  `dsdl:"11"` // offset 11
}
```

## Thread Safety

This package is safe for concurrent use. The struct layout cache uses `sync.Map` internally, and marshal/unmarshal operations on cached layouts are read-only.

## Standard UAVCAN Types

For standard UAVCAN data types as defined in the Cyphal specification, see the [uavcan package](../uavcan):

```go
import "github.com/benjamin/libcanard-go/uavcan"

// Use standard types like Heartbeat
hb := uavcan.Heartbeat{
    Uptime:                 12345678,
    Health:                 uavcan.HealthNominal,
    Mode:                   uavcan.ModeActive,
    VendorSpecificStatusCode: 0,
}

// Or use type-safe SI units
velocity := uavcan.VelocityVector3{
    X: 1.5, // m/s
    Y: 2.0, // m/s
    Z: 0.5, // m/s
}
```

The `uavcan` package includes:
- Node management types (Heartbeat, GetInfo, ExecuteCommand)
- Time synchronization types
- SI unit types for physical quantities (velocity, acceleration, etc.)
- Primitive types (Empty, String, integers, floats)
- File system interface types
- Register interface types
- Plug-and-play types
- And more...

## License

This package is part of libcanard-go and is distributed under the MIT License.

## Contributing

Contributions are welcome! The v2 API is new and can be improved further. Possible enhancements:
- Support for variable-size arrays
- Support for delimited strings
- Support for unions (discriminated types)
- Performance optimizations
- Better error messages

Please open issues or pull requests for any improvements!
