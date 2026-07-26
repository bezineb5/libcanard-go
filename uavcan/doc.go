// Package uavcan contains standard UAVCAN data types as defined in the
// Cyphal Specification Chapter 5 (Application Layer) and Chapter 6.
//
// This package provides Go struct definitions for all standard regulated DSDL
// data types located under the root namespace "uavcan". These types are an
// integral part of the Cyphal specification and facilitate cross-vendor
// compatibility.
//
// # Namespace Organization
//
// The types are organized according to their UAVCAN namespace:
//
//   - uavcan.node: Node management types (Heartbeat, GetInfo, ExecuteCommand, etc.)
//   - uavcan.time: Time synchronization types
//   - uavcan.si.unit: SI unit scalar and vector types for physical quantities
//   - uavcan.si.sample: Timestamped SI unit sample types
//   - uavcan.primitive: Basic primitive types (Empty, String, integers, floats, etc.)
//   - uavcan.file: Remote file system interface types
//   - uavcan.register: Register interface types
//   - uavcan.pnp: Plug-and-play node types
//   - uavcan.internet: Internet/LAN forwarding types
//   - uavcan.metatransport: Meta-transport tunneling types
//   - uavcan.diagnostic: Diagnostics and event logging types
//
// # Fixed Port IDs
//
// Many standard types have fixed port IDs (subject IDs or service IDs) as defined
// in the specification. These are documented in the type comments.
//
//   - uavcan.node.Heartbeat: Subject ID 7509
//   - uavcan.node.GetInfo: Service ID 430
//   - uavcan.node.ExecuteCommand: Service ID 431
//   - uavcan.time.Synchronization: Subject ID 7168
//   - uavcan.time.SynchronizedTimestamp: Used as a field type
//   - uavcan.pnp.NodeIDAllocationData: Subject ID 8165
//
// # Usage
//
// These types are designed to work seamlessly with the dsdl package:
//
//
//	type MyMessage struct {
//	    Timestamp uavcan.SynchronizedTimestamp
//	    Velocity  uavcan.unit.VelocityVector3
//	}
//
//	data, err := dsdl.Marshal(myMsg)
//	if err != nil {
//	    // handle error
//	}
//
// # Type Safety
//
// The SI unit types (in uavcan.si.unit and uavcan.si.sample) provide enhanced
// type safety by using explicitly typed alternatives instead of raw scalar types.
// This helps prevent errors from mixing up different physical quantities.
//
// For example, use uavcan.unit.VelocityScalar instead of float32 for velocity
// values to make the intent clear and prevent accidental misuse.
//
// # All Values are Little-Endian
//
// All numeric values are encoded in little-endian format as per the Cyphal standard.
package uavcan
