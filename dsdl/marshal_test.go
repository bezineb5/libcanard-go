package dsdl

import (
	"testing"
)

// TestHeartbeatV2 - same as before but without type repetition in tags
type TestHeartbeatV2 struct {
	Uptime                   uint64 `dsdl:"0"`
	Health                   uint8  `dsdl:"8"`
	Mode                     uint8  `dsdl:"9"`
	VendorSpecificStatusCode uint16 `dsdl:"10"`
}

// TestHeartbeatAuto - fully automatic (no offsets, no types)
type TestHeartbeatAuto struct {
	Uptime                   uint64
	Health                   uint8
	Mode                     uint8
	VendorSpecificStatusCode uint16
}

// TestStatusV2 - mixed explicit and automatic
type TestStatusV2 struct {
	Enabled     bool    `dsdl:"0"`
	Count       uint32  `dsdl:"1"`
	Value       int16   `dsdl:"5"`
	Temperature float32 `dsdl:"7"`
}

// TestStatusAuto - fully automatic
type TestStatusAuto struct {
	Enabled     bool
	Count       uint32
	Value       int16
	Temperature float32
}

// TestWithPadding - explicit offsets for padding
type TestWithPadding struct {
	Field1 uint8   `dsdl:"0"`
	_      [3]byte `dsdl:"1"` // padding
	Field2 uint32  `dsdl:"4"`
}

// TestWithPaddingAuto - padding with anonymous field (sequential)
type TestWithPaddingAuto struct {
	Field1 uint8
	_      [3]byte // anonymous padding field
	Field2 uint32
}

// =====================================================================================================================
//                                         V2 Tests: Explicit Offsets
// =====================================================================================================================

func TestMarshalV2ExplicitOffsets(t *testing.T) {
	hb := TestHeartbeatV2{
		Uptime:                   0x123456789ABCDEF0,
		Health:                   1,
		Mode:                     4,
		VendorSpecificStatusCode: 0xABCD,
	}

	data, err := MarshalV2(hb)
	if err != nil {
		t.Fatalf("MarshalV2 failed: %v", err)
	}

	if len(data) != 12 {
		t.Errorf("Expected 12 bytes, got %d", len(data))
	}

	// Check Uptime (8 bytes, little-endian)
	if data[0] != 0xF0 || data[1] != 0xDE || data[2] != 0xBC || data[3] != 0x9A {
		t.Errorf("Uptime bytes 0-3: got [0x%02x,0x%02x,0x%02x,0x%02x]",
			data[0], data[1], data[2], data[3])
	}
	if data[4] != 0x78 || data[5] != 0x56 || data[6] != 0x34 || data[7] != 0x12 {
		t.Errorf("Uptime bytes 4-7: got [0x%02x,0x%02x,0x%02x,0x%02x]",
			data[4], data[5], data[6], data[7])
	}

	// Check Health
	if data[8] != 1 {
		t.Errorf("Health: got %d, want 1", data[8])
	}

	// Check Mode
	if data[9] != 4 {
		t.Errorf("Mode: got %d, want 4", data[9])
	}

	// Check VendorSpecificStatusCode
	if data[10] != 0xCD || data[11] != 0xAB {
		t.Errorf("VendorSpecificStatusCode: got [0x%02x,0x%02x], want [0xCD,0xAB]", data[10], data[11])
	}
}

func TestUnmarshalV2ExplicitOffsets(t *testing.T) {
	data := []byte{
		0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12, // uptime
		0x01,       // health
		0x04,       // mode
		0xCD, 0xAB, // vendor_specific_status_code
	}

	var hb TestHeartbeatV2
	err := UnmarshalV2(data, &hb)
	if err != nil {
		t.Fatalf("UnmarshalV2 failed: %v", err)
	}

	if hb.Uptime != 0x123456789ABCDEF0 {
		t.Errorf("Uptime: got 0x%016x, want 0x123456789ABCDEF0", hb.Uptime)
	}
	if hb.Health != 1 {
		t.Errorf("Health: got %d, want 1", hb.Health)
	}
	if hb.Mode != 4 {
		t.Errorf("Mode: got %d, want 4", hb.Mode)
	}
	if hb.VendorSpecificStatusCode != 0xABCD {
		t.Errorf("VendorSpecificStatusCode: got 0x%04x, want 0xABCD", hb.VendorSpecificStatusCode)
	}
}

// =====================================================================================================================
//                                         V2 Tests: Automatic Offsets
// =====================================================================================================================

func TestMarshalV2AutomaticOffsets(t *testing.T) {
	hb := TestHeartbeatAuto{
		Uptime:                   0x123456789ABCDEF0,
		Health:                   1,
		Mode:                     4,
		VendorSpecificStatusCode: 0xABCD,
	}

	data, err := MarshalV2(hb)
	if err != nil {
		t.Fatalf("MarshalV2 failed: %v", err)
	}

	// Should be same size as explicit version
	if len(data) != 12 {
		t.Errorf("Expected 12 bytes, got %d", len(data))
	}

	// Check values (same as explicit version)
	if data[0] != 0xF0 || data[1] != 0xDE || data[2] != 0xBC || data[3] != 0x9A {
		t.Errorf("Uptime bytes 0-3: got [0x%02x,0x%02x,0x%02x,0x%02x]",
			data[0], data[1], data[2], data[3])
	}
	if data[8] != 1 {
		t.Errorf("Health: got %d, want 1", data[8])
	}
	if data[9] != 4 {
		t.Errorf("Mode: got %d, want 4", data[9])
	}
	if data[10] != 0xCD || data[11] != 0xAB {
		t.Errorf("VendorSpecificStatusCode: got [0x%02x,0x%02x], want [0xCD,0xAB]", data[10], data[11])
	}
}

func TestUnmarshalV2AutomaticOffsets(t *testing.T) {
	data := []byte{
		0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12, // uptime
		0x01,       // health
		0x04,       // mode
		0xCD, 0xAB, // vendor_specific_status_code
	}

	var hb TestHeartbeatAuto
	err := UnmarshalV2(data, &hb)
	if err != nil {
		t.Fatalf("UnmarshalV2 failed: %v", err)
	}

	if hb.Uptime != 0x123456789ABCDEF0 {
		t.Errorf("Uptime: got 0x%016x, want 0x123456789ABCDEF0", hb.Uptime)
	}
	if hb.Health != 1 {
		t.Errorf("Health: got %d, want 1", hb.Health)
	}
	if hb.Mode != 4 {
		t.Errorf("Mode: got %d, want 4", hb.Mode)
	}
	if hb.VendorSpecificStatusCode != 0xABCD {
		t.Errorf("VendorSpecificStatusCode: got 0x%04x, want 0xABCD", hb.VendorSpecificStatusCode)
	}
}

func TestMarshalV2RoundtripAutomatic(t *testing.T) {
	hb := TestHeartbeatAuto{
		Uptime:                   0x123456789ABCDEF0,
		Health:                   2,
		Mode:                     5,
		VendorSpecificStatusCode: 0x1234,
	}

	data, err := MarshalV2(hb)
	if err != nil {
		t.Fatalf("MarshalV2 failed: %v", err)
	}

	var hb2 TestHeartbeatAuto
	err = UnmarshalV2(data, &hb2)
	if err != nil {
		t.Fatalf("UnmarshalV2 failed: %v", err)
	}

	if hb != hb2 {
		t.Errorf("Roundtrip failed: got %+v, want %+v", hb2, hb)
	}
}

// =====================================================================================================================
//                                         V2 Tests: With Padding
// =====================================================================================================================

func TestMarshalV2WithPadding(t *testing.T) {
	msg := TestWithPadding{
		Field1: 0xAA,
		Field2: 0x12345678,
	}

	data, err := MarshalV2(msg)
	if err != nil {
		t.Fatalf("MarshalV2 failed: %v", err)
	}

	// Field1 at offset 0 (1 byte)
	if data[0] != 0xAA {
		t.Errorf("Field1: got 0x%02x, want 0xAA", data[0])
	}

	// Padding at offset 1 (3 bytes) - should be zero
	if data[1] != 0 || data[2] != 0 || data[3] != 0 {
		t.Errorf("Padding: got [0x%02x,0x%02x,0x%02x], want [0,0,0]", data[1], data[2], data[3])
	}

	// Field2 at offset 4 (4 bytes, little-endian)
	if data[4] != 0x78 || data[5] != 0x56 || data[6] != 0x34 || data[7] != 0x12 {
		t.Errorf("Field2: got [0x%02x,0x%02x,0x%02x,0x%02x], want [0x78,0x56,0x34,0x12]",
			data[4], data[5], data[6], data[7])
	}

	// Total size should be 8
	if len(data) != 8 {
		t.Errorf("Size: got %d, want 8", len(data))
	}
}

func TestMarshalV2WithPaddingAuto(t *testing.T) {
	msg := TestWithPaddingAuto{
		Field1: 0xAA,
		Field2: 0x12345678,
	}

	data, err := MarshalV2(msg)
	if err != nil {
		t.Fatalf("MarshalV2 failed: %v", err)
	}

	// Field1 at offset 0 (1 byte)
	if data[0] != 0xAA {
		t.Errorf("Field1: got 0x%02x, want 0xAA", data[0])
	}

	// Padding at offset 1 (3 bytes) - anonymous field, sequential
	if data[1] != 0 || data[2] != 0 || data[3] != 0 {
		t.Errorf("Padding: got [0x%02x,0x%02x,0x%02x], want [0,0,0]", data[1], data[2], data[3])
	}

	// Field2 at offset 4 (4 bytes, little-endian)
	if data[4] != 0x78 || data[5] != 0x56 || data[6] != 0x34 || data[7] != 0x12 {
		t.Errorf("Field2: got [0x%02x,0x%02x,0x%02x,0x%02x], want [0x78,0x56,0x34,0x12]",
			data[4], data[5], data[6], data[7])
	}

	// Total size should be 8
	if len(data) != 8 {
		t.Errorf("Size: got %d, want 8", len(data))
	}
}

// =====================================================================================================================
//                                         V2 Tests: Mixed Explicit/Automatic
// =====================================================================================================================

func TestMarshalV2Mixed(t *testing.T) {
	status := TestStatusV2{
		Enabled:     true,
		Count:       42,
		Value:       -100,
		Temperature: 23.5,
	}

	data, err := MarshalV2(status)
	if err != nil {
		t.Fatalf("MarshalV2 failed: %v", err)
	}

	if len(data) != 11 {
		t.Errorf("Expected 11 bytes, got %d", len(data))
	}

	// Check bool
	if data[0] != 1 {
		t.Errorf("Enabled: got %d, want 1", data[0])
	}

	// Check count (4 bytes at offset 1)
	if data[1] != 42 || data[2] != 0 || data[3] != 0 || data[4] != 0 {
		t.Errorf("Count: got %v, want [42,0,0,0]", data[1:5])
	}

	// Check value (2 bytes at offset 5, little-endian -100 = 0x9CFF)
	if data[5] != 0x9C || data[6] != 0xFF {
		t.Errorf("Value: got [0x%02x, 0x%02x], want [0x9C, 0xFF]", data[5], data[6])
	}
}

// =====================================================================================================================
//                                         V2 Tests: Errors
// =====================================================================================================================

func TestMarshalV2NonStructError(t *testing.T) {
	_, err := MarshalV2("not a struct")
	if err == nil {
		t.Error("Expected error for non-struct")
	}
	if err != ErrNotStruct {
		t.Errorf("Expected ErrNotStruct, got %v", err)
	}
}

func TestUnmarshalV2NonStructError(t *testing.T) {
	var x int
	err := UnmarshalV2([]byte{0x01}, &x)
	if err == nil {
		t.Error("Expected error for non-struct")
	}
	if err != ErrNotStruct {
		t.Errorf("Expected ErrNotStruct, got %v", err)
	}
}

func TestMarshalV2OverlapError(t *testing.T) {
	// Define a struct with overlapping fields
	type Overlapping struct {
		A uint8  `dsdl:"0"`
		B uint16 `dsdl:"0"` // Overlaps with A
	}

	_, err := MarshalV2(Overlapping{})
	if err == nil {
		t.Error("Expected error for overlapping fields")
	}
}

// =====================================================================================================================
//                                         V2 Tests: Cache Performance
// =====================================================================================================================

func TestCacheV2Performance(t *testing.T) {
	// Clear cache
	ClearCacheV2()

	hb := TestHeartbeatV2{Uptime: 123, Health: 1, Mode: 2}

	// First marshal populates the cache
	_, err := MarshalV2(hb)
	if err != nil {
		t.Fatalf("First marshal failed: %v", err)
	}

	// Subsequent marshal should be faster (cache hit)
	for i := 0; i < 1000; i++ {
		_, err := MarshalV2(hb)
		if err != nil {
			t.Fatalf("Marshal %d failed: %v", i, err)
		}
	}

	// Test with different struct type
	status := TestStatusV2{Enabled: true, Count: 42}
	_, err = MarshalV2(status)
	if err != nil {
		t.Fatalf("Marshal status failed: %v", err)
	}

	// Clear cache and verify it works again
	ClearCacheV2()
	_, err = MarshalV2(hb)
	if err != nil {
		t.Fatalf("Marshal after cache clear failed: %v", err)
	}
}

// =====================================================================================================================
//                                         V2 Benchmarks
// =====================================================================================================================

func BenchmarkMarshalV2FirstTime(b *testing.B) {
	ClearCacheV2()

	hb := TestHeartbeatV2{Uptime: 123, Health: 1, Mode: 2}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ClearCacheV2() // Force cache miss
		MarshalV2(hb)
	}
}

func BenchmarkMarshalV2Cached(b *testing.B) {
	hb := TestHeartbeatV2{Uptime: 123, Health: 1, Mode: 2}

	// Warm up the cache
	MarshalV2(hb)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalV2(hb)
	}
}

func BenchmarkUnmarshalV2(b *testing.B) {
	data := []byte{
		0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12,
		0x01, 0x04, 0xCD, 0xAB,
	}

	// Warm up the cache
	var hb TestHeartbeatV2
	UnmarshalV2(data, &hb)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UnmarshalV2(data, &hb)
	}
}

func BenchmarkMarshalToV2(b *testing.B) {
	hb := TestHeartbeatV2{Uptime: 123, Health: 1, Mode: 2}
	buf := make([]byte, 12)

	// Warm up
	MarshalToV2(buf, hb)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalToV2(buf, hb)
	}
}

func BenchmarkMarshalV2Automatic(b *testing.B) {
	hb := TestHeartbeatAuto{Uptime: 123, Health: 1, Mode: 2}

	// Warm up the cache
	MarshalV2(hb)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MarshalV2(hb)
	}
}
