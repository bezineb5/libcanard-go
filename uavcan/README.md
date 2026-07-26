# UAVCAN Standard Types for Go

This package provides Go struct definitions for all standard UAVCAN data types as defined in the Cyphal Specification Chapter 5 (Application Layer) and Chapter 6.

## Overview

The UAVCAN standard defines a comprehensive set of data types organized in various namespaces under the root `uavcan` namespace. These types facilitate cross-vendor compatibility and provide standardized interfaces for common aerospace and robotic system functions.

## Namespaces Implemented

### Core Node Management (`uavcan.node`)
- **Heartbeat** (Subject ID: 7509) - Mandatory node status message
- **GetInfo** (Service ID: 430) - Node information retrieval
- **ExecuteCommand** (Service ID: 431) - Node command execution
- **Port monitoring types** - SubjectID, ServiceID, PortID

### Time Synchronization (`uavcan.time`)
- **SynchronizedTimestamp** - Network-synchronized timestamp type
- **Synchronization** (Subject ID: 7168) - Time sync message
- **GetSynchronizationMasterInfo** - Time master information

### Physical Quantities (`uavcan.si.unit` and `uavcan.si.sample`)

#### Scalar Types
- Acceleration, Angle, AngularAcceleration, AngularVelocity
- Duration (with Wide variant), ElectricCharge, ElectricCurrent
- Energy, Force, Frequency, Length (with Wide variant)
- Luminance, MagneticFieldStrength, MagneticFluxDensity
- Mass, Power, Pressure, Temperature, Torque, Velocity
- Voltage, Volume, VolumetricFlowRate

#### Vector3 Types
- AccelerationVector3, AngularAccelerationVector3, AngularVelocityVector3
- ForceVector3, LengthVector3 (with Wide variant)
- MagneticFieldStrengthVector3, MagneticFluxDensityVector3
- TorqueVector3, VelocityVector3

#### Quaternion Type
- AngleQuaternion (W, X, Y, Z ordering)

#### Sample Types (Timestamped)
All scalar and vector types have corresponding sample types with a `SynchronizedTimestamp` field for timestamped data.

### Primitive Types (`uavcan.primitive`)
- **Empty** - Empty value (0 bytes)
- **String** - UTF-8 string
- **Unstructured** - Raw byte array
- **Scalar types** - Bit, Integer8/16/32/64, Natural8/16/32/64, Real32/64
- **Array types** - Variable-size arrays of all scalar types

### File System (`uavcan.file`)
- **GetInfo** (Service ID: 405) - Get file/directory info
- **List** (Service ID: 406) - List directory contents
- **Read** (Service ID: 408) - Read from file
- **Write** (Service ID: 409) - Write to file
- **Modify** (Service ID: 407) - Modify file metadata
- **Path, Error, FileType** - Supporting types

### Register Interface (`uavcan.register`)
- **Access** (Service ID: 384) - Read/write registers
- **List** (Service ID: 385) - List available registers
- **Name, Value, ValueType** - Supporting types

### Plug-and-Play (`uavcan.pnp`)
- **NodeIDAllocationData** (Subject ID: 8165) - Node-ID allocation
- **Cluster types** - Consensus algorithm types (AppendEntries, RequestVote, Discovery)

### Internet/LAN Forwarding (`uavcan.internet.udp`)
- **HandleIncomingPacket** (Service ID: 500) - Handle incoming UDP
- **OutgoingPacket** (Subject ID: 8174) - Send outgoing UDP
- **Endpoint** - UDP endpoint representation

### Meta-Transport (`uavcan.metatransport`)
- **CAN types** - Frame, ArbitrationID, DataClassic, DataFD, Error, etc.
- **Ethernet types** - Frame, EtherType
- **Serial types** - Fragment
- **UDP types** - Frame, Endpoint

### Diagnostics (`uavcan.diagnostic`)
- **Record** - Diagnostic message/event log entry
- **Severity** - Message severity levels

## Usage

### Import

```go
import "github.com/benjamin/libcanard-go/dsdl/uavcan"
```

### Using Heartbeat

```go
import (
    "github.com/benjamin/libcanard-go"
    "github.com/benjamin/libcanard-go/dsdl"
    "github.com/benjamin/libcanard-go/dsdl/uavcan"
)

// Create a heartbeat message
hb := uavcan.Heartbeat{
    Uptime:                 12345678,
    Health:                 uavcan.HealthNominal,
    Mode:                   uavcan.ModeActive,
    VendorSpecificStatusCode: 0,
}

// Marshal to bytes
payload, err := dsdl.Marshal(hb)
if err != nil {
    panic(err)
}

// Publish using libcanard
canard.Publish16b(
    1000000,                    // deadline
    libcanard.IfaceBitmapAll,  // interface bitmap
    libcanard.PrioNominal,     // priority
    7509,                      // Heartbeat subject ID
    0,                         // transfer ID
    payload,                   // payload bytes
    nil,                       // user context
)
```

### Using SI Unit Types

```go
// Create a timestamped velocity sample
velocitySample := uavcan.VelocityVector3Sample{
    Timestamp: 1234567890123456, // microseconds since Unix epoch
    Value: uavcan.VelocityVector3{
        X: 1.5, // m/s
        Y: 2.0, // m/s
        Z: 0.5, // m/s
    },
}

// Marshal to bytes
data, err := dsdl.Marshal(velocitySample)
```

### Using Primitive Types

```go
// Use type-safe SI units instead of raw floats
var distance uavcan.LengthScalar = 123.456 // meters
var velocity uavcan.VelocityScalar = 10.0   // m/s

// These are distinct types, preventing accidental mixing
type MyMessage struct {
    Distance uavcan.LengthScalar
    Velocity uavcan.VelocityScalar
}
```

## Fixed Port IDs

Many standard types have fixed port IDs as defined in the specification:

| Type | Port ID | Type |
|------|---------|------|
| `uavcan.node.Heartbeat` | 7509 | Subject |
| `uavcan.node.GetInfo` | 430 | Service |
| `uavcan.node.ExecuteCommand` | 431 | Service |
| `uavcan.time.Synchronization` | 7168 | Subject |
| `uavcan.pnp.NodeIDAllocationData` | 8165 | Subject |
| `uavcan.internet.udp.OutgoingPacket` | 8174 | Subject |
| `uavcan.metatransport.can.Frame` | 71 | Subject |

## Port ID Ranges

As per Cyphal Specification Section 5.1.1:

| Range | Purpose |
|-------|---------|
| [0, 6143] | Unregulated identifiers (both fixed and non-fixed) |
| [6144, 7167] | Non-standard fixed regulated identifiers (vendor-specific) |
| [7168, 8191] | Standard fixed regulated identifiers |

## Type Safety Recommendations

The specification (Section 5.2.6.2) recommends using the explicitly typed SI unit types instead of raw scalar types to improve type safety:

```go
// Not recommended
var kineticEnergy float32 // [joule]

// Recommended
var kineticEnergy uavcan.EnergyScalar // [joule]
```

This prevents accidental mixing of different physical quantities and makes the code more self-documenting.

## Coordinate Frame Conventions

As per Section 5.2.3, Cyphal follows these coordinate frame conventions:

- **World frame**: North-East-Down (NED) right-handed: X=north, Y=east, Z=down
- **Body frame**: Right-handed: X=forward, Y=right, Z=down
- **Optical frame** (cameras): Right-handed: X=right, Y=down, Z=toward scene

All coordinate systems are right-handed.

## Rotation Representation

As per Section 5.2.4, rotations should be represented using quaternions with the ordering: W, X, Y, Z.

```go
rotation := uavcan.AngleQuaternion{
    W: 0.7071,
    X: 0.0,
    Y: 0.7071,
    Z: 0.0,
}
```

## Units

As per Section 5.2.6.1, all units should be SI units (base or derived). The UAVCAN types in this package use SI units by default:

- Length: meters [m]
- Mass: kilograms [kg]
- Time: seconds [s]
- Temperature: kelvin [K]
- Electric current: amperes [A]
- etc.

For scaled units (e.g., milliseconds, kilometers), use the appropriate scaled types or add suffixes to field names as per the specification.

## License

This package is part of libcanard-go and is distributed under the MIT License.
