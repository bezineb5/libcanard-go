// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// an OpenCyphal/CAN (Cyphal) protocol stack.
//
// It currently depends only on go.einride.tech/can for CAN frame handling.
package libcanard

import "go.einride.tech/can"

// Frame is a thin alias for the underlying CAN frame representation.
type Frame = can.Frame

// ValidateFrame checks that f is a valid CAN frame using the can-go primitives.
func ValidateFrame(f Frame) error {
	return f.Validate()
}
