package uavcan

// =============================================================================
// uavcan.register namespace - Register interface
// =============================================================================

// Name represents a register name.
// Maximum length: 255 bytes.
// Allowed characters: a-z, A-Z, 0-9, ., -, _
type RegisterName struct {
	Length uint8
	Value  [255]uint8 // UTF-8 encoded name string
}

// Value represents a register value.
// A register can contain various types of data: scalar values, strings, or byte arrays.
type RegisterValue struct {
	// The type of the value.
	Type RegisterValueType

	// The value data (interpretation depends on Type).
	// Maximum size: 255 bytes for the serialized representation.
	Data []uint8
}

// RegisterValueType represents the type of a register value.
type RegisterValueType uint8

const (
	// RegisterValueTypeEmpty indicates an empty value.
	RegisterValueTypeEmpty RegisterValueType = iota
	// RegisterValueTypeBit indicates a bit value.
	RegisterValueTypeBit
	// RegisterValueTypeInteger8 indicates an 8-bit signed integer.
	RegisterValueTypeInteger8
	// RegisterValueTypeInteger16 indicates a 16-bit signed integer.
	RegisterValueTypeInteger16
	// RegisterValueTypeInteger32 indicates a 32-bit signed integer.
	RegisterValueTypeInteger32
	// RegisterValueTypeInteger64 indicates a 64-bit signed integer.
	RegisterValueTypeInteger64
	// RegisterValueTypeNatural8 indicates an 8-bit unsigned integer.
	RegisterValueTypeNatural8
	// RegisterValueTypeNatural16 indicates a 16-bit unsigned integer.
	RegisterValueTypeNatural16
	// RegisterValueTypeNatural32 indicates a 32-bit unsigned integer.
	RegisterValueTypeNatural32
	// RegisterValueTypeNatural64 indicates a 64-bit unsigned integer.
	RegisterValueTypeNatural64
	// RegisterValueTypeReal32 indicates a 32-bit floating point number.
	RegisterValueTypeReal32
	// RegisterValueTypeReal64 indicates a 64-bit floating point number.
	RegisterValueTypeReal64
	// RegisterValueTypeString indicates a UTF-8 string.
	RegisterValueTypeString
	// RegisterValueTypeByteArray indicates a raw byte array.
	RegisterValueTypeByteArray
)

// AccessRequest is the request to read or write a register.
// Fixed Service ID: 384
type RegisterAccessRequest struct {
	// The name of the register to access.
	Name RegisterName

	// The value to write (for write operations).
	// This field is ignored for read operations.
	Value RegisterValue
}

// AccessResponse contains the result of a register access operation.
type RegisterAccessResponse struct {
	// The name of the register that was accessed.
	Name RegisterName

	// The value that was read or written.
	Value RegisterValue

	// The result of the operation.
	// 0 = Success, non-zero = Error (vendor-specific)
	Status uint8
}

// ListRequest is the request to list available registers.
// Fixed Service ID: 385
type RegisterListRequest struct {
	// The index to start listing from (for pagination).
	Index uint16

	// The maximum number of registers to return.
	MaxCount uint8

	// Reserved for future use.
	_ [1]uint8
}

// ListResponse contains a list of available registers.
type RegisterListResponse struct {
	// The index of the first register in this response.
	Index uint16

	// The total number of registers available.
	TotalCount uint16

	// The list of register names.
	Names []RegisterName
}
