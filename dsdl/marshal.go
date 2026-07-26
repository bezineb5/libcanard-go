// Package dsdl provides DSDL (DroneCAN Schema Definition Language) serialization
// and deserialization for Go structs using struct tags.
//
// This version (v2) uses a more ergonomic design where:
//   - Types are inferred from Go types (no need to repeat in tags)
//   - Offsets are auto-computed by default (sequential packing)
//   - Explicit offsets can be specified when needed (e.g., for padding)
//
// Example with automatic layout (recommended):
//
//	type Heartbeat struct {
//	    Uptime  uint64
//	    Health  uint8
//	    Mode    uint8
//	}
//
// Example with explicit offsets (for padding):
//
//	type Status struct {
//	    Timestamp uint64 `dsdl:"0"`
//	    _        [3]byte `dsdl:"8"`  // padding
//	    Flags    uint8  `dsdl:"11"` // explicit offset
//	}
//
// The package caches struct layouts for optimal performance.
package dsdl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// ErrShortData is returned when the data is too short for the struct.
var ErrShortData = errors.New("dsdl: data too short")

// ErrInvalidTag is returned when a DSDL tag is malformed.
var ErrInvalidTag = errors.New("dsdl: invalid tag")

// ErrUnsupportedType is returned when a DSDL type is not supported.
var ErrUnsupportedType = errors.New("dsdl: unsupported type")

// ErrNotStruct is returned when trying to marshal/unmarshal a non-struct.
var ErrNotStruct = errors.New("dsdl: expected struct")

// ErrOverlap is returned when fields have overlapping offsets.
var ErrOverlap = errors.New("dsdl: fields have overlapping offsets")

// typeInfo holds serialization information for a DSDL type.
type typeInfo struct {
	Kind      reflect.Kind
	Size      int
	Marshal   func(buf []byte, val reflect.Value)
	Unmarshal func(buf []byte, val reflect.Value)
}

// Primitive marshalers
func marshalBool(buf []byte, val reflect.Value) { buf[0] = boolToByte(val.Bool()) }
func marshalInt8(buf []byte, val reflect.Value) { buf[0] = byte(val.Int()) }
func marshalInt16(buf []byte, val reflect.Value) {
	binary.LittleEndian.PutUint16(buf, uint16(val.Int()))
}
func marshalInt32(buf []byte, val reflect.Value) {
	binary.LittleEndian.PutUint32(buf, uint32(val.Int()))
}
func marshalInt64(buf []byte, val reflect.Value) {
	binary.LittleEndian.PutUint64(buf, uint64(val.Int()))
}
func marshalUint8(buf []byte, val reflect.Value) { buf[0] = byte(val.Uint()) }
func marshalUint16(buf []byte, val reflect.Value) {
	binary.LittleEndian.PutUint16(buf, uint16(val.Uint()))
}
func marshalUint32(buf []byte, val reflect.Value) {
	binary.LittleEndian.PutUint32(buf, uint32(val.Uint()))
}
func marshalUint64(buf []byte, val reflect.Value) { binary.LittleEndian.PutUint64(buf, val.Uint()) }
func marshalFloat32(buf []byte, val reflect.Value) {
	binary.LittleEndian.PutUint32(buf, math.Float32bits(float32(val.Float())))
}
func marshalFloat64(buf []byte, val reflect.Value) {
	binary.LittleEndian.PutUint64(buf, math.Float64bits(val.Float()))
}

// Primitive unmarshalers
func unmarshalBool(buf []byte, val reflect.Value) { val.SetBool(buf[0] != 0) }
func unmarshalInt8(buf []byte, val reflect.Value) { val.SetInt(int64(int8(buf[0]))) }
func unmarshalInt16(buf []byte, val reflect.Value) {
	val.SetInt(int64(int16(binary.LittleEndian.Uint16(buf))))
}
func unmarshalInt32(buf []byte, val reflect.Value) {
	val.SetInt(int64(int32(binary.LittleEndian.Uint32(buf))))
}
func unmarshalInt64(buf []byte, val reflect.Value) {
	val.SetInt(int64(binary.LittleEndian.Uint64(buf)))
}
func unmarshalUint8(buf []byte, val reflect.Value) { val.SetUint(uint64(buf[0])) }
func unmarshalUint16(buf []byte, val reflect.Value) {
	val.SetUint(uint64(binary.LittleEndian.Uint16(buf)))
}
func unmarshalUint32(buf []byte, val reflect.Value) {
	val.SetUint(uint64(binary.LittleEndian.Uint32(buf)))
}
func unmarshalUint64(buf []byte, val reflect.Value) { val.SetUint(binary.LittleEndian.Uint64(buf)) }
func unmarshalFloat32(buf []byte, val reflect.Value) {
	val.SetFloat(float64(math.Float32frombits(binary.LittleEndian.Uint32(buf))))
}
func unmarshalFloat64(buf []byte, val reflect.Value) {
	val.SetFloat(math.Float64frombits(binary.LittleEndian.Uint64(buf)))
}

// float16 helpers (Cyphal uses IEEE 754-2008 half-precision)
func marshalFloat16(buf []byte, val reflect.Value) {
	f32 := float32(val.Float())
	// Convert float32 to float16
	f16 := float32ToFloat16(f32)
	binary.LittleEndian.PutUint16(buf, f16)
}

func unmarshalFloat16(buf []byte, val reflect.Value) {
	f16 := binary.LittleEndian.Uint16(buf)
	f32 := float16ToFloat32(f16)
	val.SetFloat(float64(f32))
}

// boolToByte converts bool to 0 or 1
func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// float32ToFloat16 converts float32 to IEEE 754 half-precision
func float32ToFloat16(f float32) uint16 {
	return uint16(float32ToFloat16Bits(f))
}

// float16ToFloat32 converts IEEE 754 half-precision to float32
func float16ToFloat32(h uint16) float32 {
	return float32(float16ToFloat32Bits(h))
}

// These are stubs - in production you'd use proper float16 conversion
func float32ToFloat16Bits(f float32) uint16 {
	bits := math.Float32bits(f)
	return uint16(bits >> 16)
}

func float16ToFloat32Bits(h uint16) uint32 {
	return uint32(h) << 16
}

// TypeMap maps DSDL type names to serialization info (for explicit type tags).
var TypeMap = map[string]typeInfo{
	"bool":    {reflect.Bool, 1, marshalBool, unmarshalBool},
	"int8":    {reflect.Int8, 1, marshalInt8, unmarshalInt8},
	"int16":   {reflect.Int16, 2, marshalInt16, unmarshalInt16},
	"int32":   {reflect.Int32, 4, marshalInt32, unmarshalInt32},
	"int64":   {reflect.Int64, 8, marshalInt64, unmarshalInt64},
	"uint8":   {reflect.Uint8, 1, marshalUint8, unmarshalUint8},
	"uint16":  {reflect.Uint16, 2, marshalUint16, unmarshalUint16},
	"uint32":  {reflect.Uint32, 4, marshalUint32, unmarshalUint32},
	"uint64":  {reflect.Uint64, 8, marshalUint64, unmarshalUint64},
	"float16": {reflect.Float32, 2, marshalFloat16, unmarshalFloat16},
	"float32": {reflect.Float32, 4, marshalFloat32, unmarshalFloat32},
	"float64": {reflect.Float64, 8, marshalFloat64, unmarshalFloat64},
}

// TypeMapV2 maps Go types to DSDL serialization info for type inference.
var TypeMapV2 = map[reflect.Kind]typeInfo{
	reflect.Bool:    {reflect.Bool, 1, marshalBool, unmarshalBool},
	reflect.Int8:    {reflect.Int8, 1, marshalInt8, unmarshalInt8},
	reflect.Int16:   {reflect.Int16, 2, marshalInt16, unmarshalInt16},
	reflect.Int32:   {reflect.Int32, 4, marshalInt32, unmarshalInt32},
	reflect.Int64:   {reflect.Int64, 8, marshalInt64, unmarshalInt64},
	reflect.Uint8:   {reflect.Uint8, 1, marshalUint8, unmarshalUint8},
	reflect.Uint16:  {reflect.Uint16, 2, marshalUint16, unmarshalUint16},
	reflect.Uint32:  {reflect.Uint32, 4, marshalUint32, unmarshalUint32},
	reflect.Uint64:  {reflect.Uint64, 8, marshalUint64, unmarshalUint64},
	reflect.Float32: {reflect.Float32, 4, marshalFloat32, unmarshalFloat32},
	reflect.Float64: {reflect.Float64, 8, marshalFloat64, unmarshalFloat64},
}

// getTypeInfoForField returns typeInfo for a field, handling arrays.
func getTypeInfoForField(field reflect.StructField) (typeInfo, error) {
	kind := field.Type.Kind()

	// Handle fixed-size arrays
	if kind == reflect.Array {
		elemKind := field.Type.Elem().Kind()
		elemTypeInfo, ok := TypeMapV2[elemKind]
		if !ok {
			return typeInfo{}, fmt.Errorf("%w: unsupported array element type %v (field %s)",
				ErrUnsupportedType, elemKind, field.Name)
		}
		// Array size is part of the type
		arraySize := field.Type.Len()
		return typeInfo{
			Kind: kind,
			Size: arraySize * elemTypeInfo.Size,
			Marshal: func(buf []byte, val reflect.Value) {
				for i := 0; i < arraySize; i++ {
					elem := val.Index(i)
					elemTypeInfo.Marshal(buf[i*elemTypeInfo.Size:(i+1)*elemTypeInfo.Size], elem)
				}
			},
			Unmarshal: func(buf []byte, val reflect.Value) {
				for i := 0; i < arraySize; i++ {
					elem := val.Index(i)
					elemTypeInfo.Unmarshal(buf[i*elemTypeInfo.Size:(i+1)*elemTypeInfo.Size], elem)
				}
			},
		}, nil
	}

	// Handle regular types
	ti, ok := TypeMapV2[kind]
	if !ok {
		return typeInfo{}, fmt.Errorf("%w: unsupported Go type %v (field %s)", ErrUnsupportedType, kind, field.Name)
	}
	return ti, nil
}

// fieldInfoV2 holds information about a single struct field.
type fieldInfoV2 struct {
	Offset   int
	TypeInfo typeInfo
	Index    int // Field index in the struct
	Name     string
}

// structInfoV2 holds cached layout information for a struct type.
type structInfoV2 struct {
	Size   int
	Fields []fieldInfoV2
	Type   reflect.Type
}

// cacheV2 holds cached struct layouts for v2.
var (
	cacheV2     = sync.Map{} // map[reflect.Type]*structInfoV2
	cacheV2Once sync.Once
)

// getStructInfoV2 retrieves or creates cached layout info for a struct type.
func getStructInfoV2(typ reflect.Type) (*structInfoV2, error) {
	// Check cache first
	if info, ok := cacheV2.Load(typ); ok {
		return info.(*structInfoV2), nil
	}

	// Parse the struct and cache it
	info, err := parseStructTypeV2(typ)
	if err != nil {
		return nil, err
	}

	// Store in cache
	cacheV2.Store(typ, info)
	return info, nil
}

// parseStructTypeV2 parses a struct type and extracts DSDL field information.
// Fields without explicit offsets are packed sequentially.
func parseStructTypeV2(typ reflect.Type) (*structInfoV2, error) {
	if typ.Kind() != reflect.Struct {
		return nil, ErrNotStruct
	}

	info := &structInfoV2{
		Type: typ,
		Size: 0,
	}

	currentOffset := 0

	// Collect all fields with their info
	allFields := []fieldInfoV2{}

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := field.Tag.Get("dsdl")

		// Skip anonymous fields without explicit offset (they're padding)
		if field.Name == "_" && tag == "" {
			// Anonymous field without tag - add as padding in sequential order
			size := int(field.Type.Size())
			allFields = append(allFields, fieldInfoV2{
				Offset:   currentOffset,
				TypeInfo: typeInfo{Size: size},
				Index:    -1, // -1 indicates padding/anonymous field
				Name:     "_padding_" + field.Name,
			})
			currentOffset += size
			continue
		}

		// Parse the tag
		offset, typeName, hasExplicitOffset := parseTagV2(tag)

		// Get type info
		var ti typeInfo
		if typeName != "" {
			// Explicit type in tag
			var ok bool
			ti, ok = TypeMap[typeName]
			if !ok {
				return nil, fmt.Errorf("%w: %s (field %s)", ErrUnsupportedType, typeName, field.Name)
			}
		} else {
			// Infer type from Go type
			var err error
			ti, err = getTypeInfoForField(field)
			if err != nil {
				return nil, err
			}
		}

		// Determine offset
		var offsetFinal int
		if hasExplicitOffset {
			// Explicit offset from tag
			if offset < 0 {
				return nil, fmt.Errorf("dsdl: negative offset in field %s", field.Name)
			}
			offsetFinal = offset

			// Check for overlap with previous fields
			if offsetFinal < info.Size {
				return nil, fmt.Errorf("dsdl: field %s at offset %d overlaps with previous fields (size so far: %d)",
					field.Name, offsetFinal, info.Size)
			}

			// Add padding if needed
			if offsetFinal > info.Size {
				// Add implicit padding
				paddingSize := offsetFinal - info.Size
				if paddingSize > 0 {
					allFields = append(allFields, fieldInfoV2{
						Offset:   info.Size,
						TypeInfo: typeInfo{Size: paddingSize},
						Index:    -1, // padding field
						Name:     "_padding_before_" + field.Name,
					})
				}
				info.Size = offsetFinal
			}
		} else {
			// Sequential packing
			offsetFinal = currentOffset
			currentOffset += ti.Size
		}

		// Update struct size
		if offsetFinal+ti.Size > info.Size {
			info.Size = offsetFinal + ti.Size
		}

		// Store field info
		allFields = append(allFields, fieldInfoV2{
			Offset:   offsetFinal,
			TypeInfo: ti,
			Index:    i,
			Name:     field.Name,
		})
	}

	// Sort all fields by offset
	for i := 0; i < len(allFields)-1; i++ {
		for j := i + 1; j < len(allFields); j++ {
			if allFields[i].Offset > allFields[j].Offset {
				allFields[i], allFields[j] = allFields[j], allFields[i]
			}
		}
	}

	info.Fields = allFields

	if info.Size == 0 {
		return nil, fmt.Errorf("%w: no serializable fields in %v", ErrInvalidTag, typ)
	}

	return info, nil
}

// parseTagV2 parses a DSDL v2 tag.
// Returns: (offset, typeName, hasExplicitOffset)
// If tag is empty, returns (0, "", false) to indicate sequential packing.
// If tag is just an offset (e.g., "8"), returns (8, "", true).
// If tag is offset,type (e.g., "8,uint8"), returns (8, "uint8", true).
func parseTagV2(tag string) (int, string, bool) {
	if tag == "" || tag == "-" {
		return 0, "", false
	}

	parts := strings.Split(tag, ",")
	if len(parts) == 0 {
		return 0, "", false
	}

	// First part is always the offset
	offset, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", false
	}

	// If there's a second part, it's the type
	if len(parts) > 1 {
		return offset, parts[1], true
	}

	// Just offset, no type
	return offset, "", true
}

// MarshalV2 serializes a struct with DSDL v2 tags to bytes.
// This version infers types from Go types and auto-computes offsets.
func MarshalV2(v any) ([]byte, error) {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Struct {
		return nil, ErrNotStruct
	}

	info, err := getStructInfoV2(val.Type())
	if err != nil {
		return nil, err
	}

	buf := make([]byte, info.Size)

	for _, field := range info.Fields {
		if field.Index < 0 {
			// Padding field - skip
			continue
		}
		fieldVal := val.Field(field.Index)
		field.TypeInfo.Marshal(buf[field.Offset:field.Offset+field.TypeInfo.Size], fieldVal)
	}

	return buf, nil
}

// MarshalToV2 serializes to a pre-allocated buffer.
func MarshalToV2(buf []byte, v any) error {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Struct {
		return ErrNotStruct
	}

	info, err := getStructInfoV2(val.Type())
	if err != nil {
		return err
	}

	if len(buf) < info.Size {
		return ErrShortData
	}

	for _, field := range info.Fields {
		if field.Index < 0 {
			// Padding field - skip
			continue
		}
		fieldVal := val.Field(field.Index)
		field.TypeInfo.Marshal(buf[field.Offset:field.Offset+field.TypeInfo.Size], fieldVal)
	}

	return nil
}

// UnmarshalV2 deserializes bytes to a struct with DSDL v2 tags.
func UnmarshalV2(data []byte, v any) error {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Ptr || val.Elem().Kind() != reflect.Struct {
		return ErrNotStruct
	}

	val = val.Elem()
	info, err := getStructInfoV2(val.Type())
	if err != nil {
		return err
	}

	if len(data) < info.Size {
		return ErrShortData
	}

	for _, field := range info.Fields {
		if field.Index < 0 {
			// Padding field - skip
			continue
		}
		fieldVal := val.Field(field.Index)
		field.TypeInfo.Unmarshal(data[field.Offset:field.Offset+field.TypeInfo.Size], fieldVal)
	}

	return nil
}

// SizeV2 returns the serialized size of a struct type.
func SizeV2(v any) (int, error) {
	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Struct {
		return 0, ErrNotStruct
	}

	info, err := getStructInfoV2(val.Type())
	if err != nil {
		return 0, err
	}

	return info.Size, nil
}

// ClearCacheV2 clears the v2 struct layout cache.
func ClearCacheV2() {
	cacheV2 = sync.Map{}
}
