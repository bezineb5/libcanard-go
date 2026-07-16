package libcanard

// This file migrates tests/src/test_intrusive_rx.c (the C "rx_parse" white-box test suite) to Go.
//
// Like the C suite it is an in-package white-box test, so it calls the unexported rxParse directly.
// Every CAN ID and expected value is a hardcoded literal copied from the C original (the test never
// reuses implementation expressions to derive expected results), so it serves as an independent
// oracle for the frame parser.
//
// The C frame_t fields (kind, port_id, dst, src, transfer_id, priority, start, end, toggle, payload)
// map onto the Go rxFrameParsed struct. The C payload is (data pointer, size); in Go it is a []byte
// sub-slice of the input (tail byte removed), so we compare the underlying backing-array pointer via
// unsafe.SliceData.

import (
	"testing"
	"unsafe"
)

// frameMatch declares which fields of a parsed frame to assert. A nil pointer means "don't check".
type frameMatch struct {
	kind   *Kind
	portID *uint32
	dst    *uint8
	src    *uint8
	prio   *Prio
	tid    *uint8
	start  *bool
	end    *bool
	toggle *bool
	payLen *int
	data   []byte // if non-nil: assert the payload shares its backing array (tail byte removed)
}

func mKind(k Kind) *Kind    { return &k }
func mU32(v uint32) *uint32 { return &v }
func mU8(v uint8) *uint8    { return &v }
func mPrio(p Prio) *Prio    { return &p }
func mBool(v bool) *bool    { return &v }
func mInt(v int) *int       { return &v }

func wantMask(t *testing.T, got, want byte) {
	t.Helper()
	if got != want {
		t.Fatalf("mask=%d, want %d", got, want)
	}
}

func checkFrame(t *testing.T, label string, got *rxFrameParsed, m frameMatch) {
	t.Helper()
	if m.kind != nil && *m.kind != got.kind {
		t.Fatalf("%s: kind=%v want %v", label, got.kind, *m.kind)
	}
	if m.portID != nil && *m.portID != uint32(got.portID) {
		t.Fatalf("%s: portID=%d want %d", label, got.portID, *m.portID)
	}
	if m.dst != nil && *m.dst != got.dst {
		t.Fatalf("%s: dst=%d want %d", label, got.dst, *m.dst)
	}
	if m.src != nil && *m.src != got.src {
		t.Fatalf("%s: src=%d want %d", label, got.src, *m.src)
	}
	if m.prio != nil && *m.prio != got.priority {
		t.Fatalf("%s: priority=%v want %v", label, got.priority, *m.prio)
	}
	if m.tid != nil && *m.tid != got.transferID {
		t.Fatalf("%s: transferID=%d want %d", label, got.transferID, *m.tid)
	}
	if m.start != nil && *m.start != got.start {
		t.Fatalf("%s: start=%v want %v", label, got.start, *m.start)
	}
	if m.end != nil && *m.end != got.end {
		t.Fatalf("%s: end=%v want %v", label, got.end, *m.end)
	}
	if m.toggle != nil && *m.toggle != got.toggle {
		t.Fatalf("%s: toggle=%v want %v", label, got.toggle, *m.toggle)
	}
	if m.payLen != nil && *m.payLen != len(got.payload) {
		t.Fatalf("%s: payload size=%d want %d", label, len(got.payload), *m.payLen)
	}
	if m.data != nil {
		if unsafe.SliceData(got.payload) != unsafe.SliceData(m.data) {
			t.Fatalf("%s: payload backing array does not match input frame data", label)
		}
	}
}

// Test 1: Empty payload returns 0 and zeroes both outputs.
func TestRxParseEmptyPayload(t *testing.T) {
	mask, v0, v1 := rxParse(0x00000000, nil)
	wantMask(t, mask, 0)
	checkFrame(t, "v0", v0, frameMatch{portID: mU32(0), src: mU8(0)})
	checkFrame(t, "v1", v1, frameMatch{portID: mU32(0), src: mU8(0)})
}

// Test 2: v1.1 message golden values.
func TestRxParseV11MessageGolden(t *testing.T) {
	// Mid: prio=3, subject=1234, src=42. CAN ID = 0x0C04D2AA.
	{
		d := []byte{0x11, 0x22, 0xE7}
		mask, _, v1 := rxParse(0x0C04D2AA, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindMessage16b), portID: mU32(1234), dst: mU8(NodeIDAnonymous), src: mU8(42),
			prio: mPrio(PrioHigh), tid: mU8(7), start: mBool(true), end: mBool(true), toggle: mBool(true),
			payLen: mInt(2), data: d,
		})
	}
	// Min: prio=0, subject=0, src=0. CAN ID = 0x00000080.
	{
		d := []byte{0xE0}
		mask, _, v1 := rxParse(0x00000080, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindMessage16b), portID: mU32(0), dst: mU8(NodeIDAnonymous), src: mU8(0),
			prio: mPrio(PrioExceptional), tid: mU8(0),
		})
	}
	// Max: prio=7, subject=65535, src=127. CAN ID = 0x1CFFFFFF.
	{
		d := []byte{0xFF}
		mask, _, v1 := rxParse(0x1CFFFFFF, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindMessage16b), portID: mU32(0xFFFF), dst: mU8(NodeIDAnonymous), src: mU8(127),
			prio: mPrio(PrioOptional), tid: mU8(31),
		})
	}
}

// Test 3: v1.0 message golden values.
func TestRxParseV10MessageGolden(t *testing.T) {
	// Normal: prio=4, subject=42, src=11, reserved=11b. CAN ID = 0x10602A0B.
	{
		d := []byte{0xAA, 0xE5}
		mask, _, v1 := rxParse(0x10602A0B, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindMessage13b), portID: mU32(42), dst: mU8(NodeIDAnonymous), src: mU8(11),
			prio: mPrio(PrioNominal), tid: mU8(5),
		})
	}
	// Max subject: prio=0, subject=8191, src=1. CAN ID = 0x007FFF01.
	{
		d := []byte{0xE0}
		mask, _, v1 := rxParse(0x007FFF01, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindMessage13b), portID: mU32(8191), src: mU8(1),
			prio: mPrio(PrioExceptional), tid: mU8(0),
		})
	}
	// Anonymous: prio=2, subject=100, bit24=1, src=0x55. CAN ID = 0x09606455.
	{
		d := []byte{0xE3}
		mask, _, v1 := rxParse(0x09606455, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindMessage13b), portID: mU32(100), src: mU8(NodeIDAnonymous),
			prio: mPrio(PrioFast), tid: mU8(3),
		})
	}
}

// Test 4: v1.0 service golden values.
func TestRxParseV10ServiceGolden(t *testing.T) {
	// Request: prio=4, svc_id=430, dst=24, src=11. CAN ID = 0x136B8C0B.
	{
		d := []byte{0xBB, 0xE1}
		mask, _, v1 := rxParse(0x136B8C0B, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindRequest), portID: mU32(430), dst: mU8(24), src: mU8(11),
			prio: mPrio(PrioNominal), tid: mU8(1),
		})
	}
	// Response min: prio=0, svc_id=0, dst=1, src=127. CAN ID = 0x020000FF.
	{
		d := []byte{0xE0}
		mask, _, v1 := rxParse(0x020000FF, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindResponse), portID: mU32(0), dst: mU8(1), src: mU8(127),
			prio: mPrio(PrioExceptional), tid: mU8(0),
		})
	}
	// Request max: prio=7, svc_id=511, dst=127, src=126. CAN ID = 0x1F7FFFFE.
	{
		d := []byte{0xFF}
		mask, _, v1 := rxParse(0x1F7FFFFE, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindRequest), portID: mU32(511), dst: mU8(127), src: mU8(126),
			prio: mPrio(PrioOptional), tid: mU8(31),
		})
	}
}

// Test 5: v0.1 message golden values.
func TestRxParseV0MessageGolden(t *testing.T) {
	// Normal: prio=4, type_id=0x040A, src=1. CAN ID = 0x13040A01.
	{
		d := []byte{0x55, 0xC2}
		mask, v0, _ := rxParse(0x13040A01, d)
		wantMask(t, mask, 1)
		checkFrame(t, "v0", v0, frameMatch{
			kind: mKind(KindV0Message), portID: mU32(0x040A), dst: mU8(NodeIDAnonymous), src: mU8(1),
			prio: mPrio(PrioNominal), tid: mU8(2),
		})
	}
	// Anonymous: src=0, only the 2 low DTID bits survive. CAN ID = 0x13040A00.
	{
		d := []byte{0xC0}
		mask, v0, _ := rxParse(0x13040A00, d)
		wantMask(t, mask, 1)
		checkFrame(t, "v0", v0, frameMatch{
			kind: mKind(KindV0Message), portID: mU32(2), src: mU8(NodeIDAnonymous),
			tid: mU8(0),
		})
	}
	// Max: prio=7, type_id=0xFFFF, src=127. CAN ID = 0x1FFFFF7F.
	{
		d := []byte{0xDF}
		mask, v0, _ := rxParse(0x1FFFFF7F, d)
		wantMask(t, mask, 1)
		checkFrame(t, "v0", v0, frameMatch{
			kind: mKind(KindV0Message), portID: mU32(0xFFFF), src: mU8(127),
			prio: mPrio(PrioOptional), tid: mU8(31),
		})
	}
}

// Test 6 (regression CN-03): v0 anonymous carries only the 2 low DTID bits.
func TestRxParseV0AnonymousDtidMask(t *testing.T) {
	d := []byte{0x77, 0xC3}
	mask, v0, _ := rxParse(0x106AF100, d)
	wantMask(t, mask, 1)
	checkFrame(t, "v0", v0, frameMatch{
		kind: mKind(KindV0Message), src: mU8(NodeIDAnonymous), portID: mU32(1), tid: mU8(3),
	})
}

// Test 7: v0.1 service golden values.
func TestRxParseV0ServiceGolden(t *testing.T) {
	// Request: prio=4, type_id=0x37, dst=24, src=11. CAN ID = 0x1337988B.
	{
		d := []byte{0x42, 0xC4}
		mask, v0, _ := rxParse(0x1337988B, d)
		wantMask(t, mask, 1)
		checkFrame(t, "v0", v0, frameMatch{
			kind: mKind(KindV0Request), portID: mU32(0x37), dst: mU8(24), src: mU8(11),
			prio: mPrio(PrioNominal), tid: mU8(4),
		})
	}
	// Response: prio=0, type_id=1, dst=1, src=2. CAN ID = 0x03010182.
	{
		d := []byte{0xC0}
		mask, v0, _ := rxParse(0x03010182, d)
		wantMask(t, mask, 1)
		checkFrame(t, "v0", v0, frameMatch{
			kind: mKind(KindV0Response), portID: mU32(1), dst: mU8(1), src: mU8(2),
			prio: mPrio(PrioExceptional), tid: mU8(0),
		})
	}
}

// Test 8: v1.0 frames with reserved bit 23 set are rejected.
func TestRxParseV10ReservedBit23Reject(t *testing.T) {
	// v1.0 service: 0x136B8C0B | 0x00800000 = 0x13EB8C0B.
	{
		d := []byte{0xE0}
		mask, _, _ := rxParse(0x13EB8C0B, d)
		wantMask(t, mask, 0)
	}
	// v1.0 message: 0x10602A0B | 0x00800000 = 0x10E02A0B.
	{
		d := []byte{0xE0}
		mask, _, _ := rxParse(0x10E02A0B, d)
		wantMask(t, mask, 0)
	}
}

// Test 9: v0 service with src=0 or dst=0 is rejected.
func TestRxParseV0ServiceZeroNodeReject(t *testing.T) {
	// src=0: CAN ID 0x13379880.
	{
		d := []byte{0xC0}
		mask, _, _ := rxParse(0x13379880, d)
		wantMask(t, mask, 0)
	}
	// dst=0: CAN ID 0x1337808B.
	{
		d := []byte{0xC0}
		mask, _, _ := rxParse(0x1337808B, d)
		wantMask(t, mask, 0)
	}
}

// Test 10: Version detection via SOT+toggle in the tail byte.
func TestRxParseVersionDetection(t *testing.T) {
	const canID = uint32(0x00002A01) // valid for both versions (message, bit7=0)
	// SOT=1 toggle=1 -> v1 only.
	{
		d := []byte{0xE0}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 2)
	}
	// SOT=1 toggle=0 -> v0 only.
	{
		d := []byte{0xC0}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 1)
	}
	// SOT=0 toggle=0 -> both (8 bytes needed).
	{
		d := []byte{1, 2, 3, 4, 5, 6, 7, 0x00}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 3)
	}
	// SOT=0 toggle=1 -> both (8 bytes needed).
	{
		d := []byte{1, 2, 3, 4, 5, 6, 7, 0x20}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 3)
	}
}

// Test 11: Payload pointer and size are forwarded correctly.
func TestRxParsePayloadHandling(t *testing.T) {
	const canID = uint32(0x00000080) // v1.1 message, prio=0, subject=0, src=0
	// Size 1 (tail byte only -> effective payload is 0 bytes).
	{
		d := []byte{0xE0}
		_, _, v1 := rxParse(canID, d)
		checkFrame(t, "v1", v1, frameMatch{payLen: mInt(0), data: d})
	}
	// Size 2.
	{
		d := []byte{0xAA, 0xE0}
		_, _, v1 := rxParse(canID, d)
		checkFrame(t, "v1", v1, frameMatch{payLen: mInt(1), data: d})
	}
	// Size 8 (classic CAN).
	{
		d := []byte{1, 2, 3, 4, 5, 6, 7, 0xE0}
		_, _, v1 := rxParse(canID, d)
		checkFrame(t, "v1", v1, frameMatch{payLen: mInt(7), data: d})
	}
	// Size 64 (CAN FD).
	{
		d := make([]byte, 64)
		for i := range d {
			d[i] = 0xAA
		}
		d[63] = 0xE0
		_, _, v1 := rxParse(canID, d)
		checkFrame(t, "v1", v1, frameMatch{payLen: mInt(63), data: d})
	}
}

// Test 12: Exhaustive tail-byte field mapping and TID boundary values.
func TestRxParseTailByteExhaustive(t *testing.T) {
	const canID = uint32(0x00002A01)
	checks := []struct {
		data   []byte
		start  bool
		end    bool
		toggle bool
		kind   Kind
		tid    uint8
	}{
		{[]byte{1, 2, 3, 4, 5, 6, 7, 0x00}, false, false, false, KindMessage13b, 0}, // SOT=0 EOT=0 toggle=0 -> v1
		{[]byte{1, 2, 3, 4, 5, 6, 7, 0x20}, false, false, true, KindMessage13b, 0},  // SOT=0 EOT=0 toggle=1 -> v1
		{[]byte{0x42, 0x40}, false, true, false, KindMessage13b, 0},                 // SOT=0 EOT=1 toggle=0
		{[]byte{0x42, 0x60}, false, true, true, KindMessage13b, 0},                  // SOT=0 EOT=1 toggle=1
		{[]byte{1, 2, 3, 4, 5, 6, 7, 0x80}, true, false, false, KindV0Message, 0},   // SOT=1 EOT=0 toggle=0 -> v0
		{[]byte{1, 2, 3, 4, 5, 6, 7, 0xA0}, true, false, true, KindMessage13b, 0},   // SOT=1 EOT=0 toggle=1 -> v1
		{[]byte{0x42, 0xC0}, true, true, false, KindV0Message, 0},                   // SOT=1 EOT=1 toggle=0 -> v0
		{[]byte{0x42, 0xE0}, true, true, true, KindMessage13b, 0},                   // SOT=1 EOT=1 toggle=1 -> v1
		{[]byte{0x42, 0xE0}, true, true, true, KindMessage13b, 0},                   // tid=0
		{[]byte{0x42, 0xE1}, true, true, true, KindMessage13b, 1},                   // tid=1
		{[]byte{0x42, 0xEF}, true, true, true, KindMessage13b, 15},                  // tid=15
		{[]byte{0x42, 0xF0}, true, true, true, KindMessage13b, 16},                  // tid=16
		{[]byte{0x42, 0xFF}, true, true, true, KindMessage13b, 31},                  // tid=31
	}
	for _, c := range checks {
		// The C suite for Test 11 checks only the start/end/toggle/tid fields of the selected
		// frame and never the version mask; for SOT=0 cases BOTH versions parse (mask=3), while
		// for SOT=1 cases exactly one does. We mirror the C assertions and do not check the mask.
		_, v0, v1 := rxParse(canID, c.data)
		if c.kind == KindV0Message {
			checkFrame(t, "v0", v0, frameMatch{start: mBool(c.start), end: mBool(c.end), toggle: mBool(c.toggle), tid: mU8(c.tid)})
		} else {
			checkFrame(t, "v1", v1, frameMatch{start: mBool(c.start), end: mBool(c.end), toggle: mBool(c.toggle), tid: mU8(c.tid)})
		}
	}
}

// Test 13: Non-first frames where the same CAN ID produces valid but different results for v0 and v1.
func TestRxParseCrossVersionAmbiguity(t *testing.T) {
	nf := byte(0x05) // SOT=0 EOT=0 toggle=0 tid=5
	// v1.1 message CAN ID 0x0C04D2AA simultaneously parses as v0 service.
	{
		d := []byte{1, 2, 3, 4, 5, 6, 7, nf}
		mask, v0, v1 := rxParse(0x0C04D2AA, d)
		wantMask(t, mask, 3)
		checkFrame(t, "v1", v1, frameMatch{kind: mKind(KindMessage16b), portID: mU32(1234), src: mU8(42)})
		checkFrame(t, "v0", v0, frameMatch{kind: mKind(KindV0Request), portID: mU32(4), dst: mU8(82), src: mU8(42)})
	}
	// All-ones 0x1FFFFFFF: v1 rejected (bit23 in service path), v0 rejected (src==dst).
	{
		d := []byte{1, 2, 3, 4, 5, 6, 7, nf}
		mask, _, _ := rxParse(0x1FFFFFFF, d)
		wantMask(t, mask, 0)
	}
	// All-zeros 0x00000000: v1 parses as v1.0 message; v0 anonymous rejected for non-first frame.
	{
		d := []byte{1, 2, 3, 4, 5, 6, 7, nf}
		mask, _, v1 := rxParse(0x00000000, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{kind: mKind(KindMessage13b), portID: mU32(0), src: mU8(0)})
	}
}

// Test 14: Adjacent bit fields do not bleed into each other.
func TestRxParseBitFieldBoundaries(t *testing.T) {
	// v1.0 service: max svc_id=511 with dst=0 and src=1. CAN ID = 0x027FC001.
	{
		d := []byte{0xE0}
		mask, _, v1 := rxParse(0x027FC001, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{kind: mKind(KindResponse), portID: mU32(511), dst: mU8(0), src: mU8(1)})
	}
	// v1.0 service: max dst=127 with svc_id=0 and src=0. CAN ID = 0x02003F80.
	{
		d := []byte{0xE0}
		mask, _, v1 := rxParse(0x02003F80, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{kind: mKind(KindResponse), portID: mU32(0), dst: mU8(127), src: mU8(0)})
	}
	// v0 service: max type_id=0xFF with dst=1, req=0. CAN ID = 0x03FF0182.
	{
		d := []byte{0xC0}
		mask, v0, _ := rxParse(0x03FF0182, d)
		wantMask(t, mask, 1)
		checkFrame(t, "v0", v0, frameMatch{kind: mKind(KindV0Response), portID: mU32(0xFF), dst: mU8(1), src: mU8(2)})
	}
}

// Test 15: v1.1 does NOT reject bit 23 (unlike v1.0).
func TestRxParseV11AcceptsBit23(t *testing.T) {
	// Subject 0x8000 sets bit 23. CAN ID = 0x00800080.
	{
		d := []byte{0xE5}
		mask, _, v1 := rxParse(0x00800080, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{kind: mKind(KindMessage16b), portID: mU32(0x8000)})
	}
	// Max subject 0xFFFF also sets bit 23. CAN ID = 0x00FFFF80.
	{
		d := []byte{0xE0}
		mask, _, v1 := rxParse(0x00FFFF80, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{kind: mKind(KindMessage16b), portID: mU32(0xFFFF)})
	}
}

// Test 16: v1.0 message reserved bits 22:21 are masked out.
func TestRxParseV10MessageIgnoresReservedBits(t *testing.T) {
	d := []byte{0xE0}
	// bits 22:21 = 00: CAN ID = 0x00002A01.
	rxParse(0x00002A01, d)
	mask, _, v1 := rxParse(0x00002A01, d)
	wantMask(t, mask, 2)
	checkFrame(t, "v1", v1, frameMatch{kind: mKind(KindMessage13b), portID: mU32(42)})
	// bits 22:21 = 11: CAN ID = 0x00602A01.
	mask, _, v1 = rxParse(0x00602A01, d)
	wantMask(t, mask, 2)
	checkFrame(t, "v1", v1, frameMatch{kind: mKind(KindMessage13b), portID: mU32(42)})
}

// Test 17: Non-first frame produces valid distinct results for both v0 and v1 simultaneously.
func TestRxParseNonFirstDualOutput(t *testing.T) {
	d := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x05}
	mask, v0, v1 := rxParse(0x0C04D2AA, d)
	wantMask(t, mask, 3)
	checkFrame(t, "v1", v1, frameMatch{
		kind: mKind(KindMessage16b), portID: mU32(1234), dst: mU8(NodeIDAnonymous), src: mU8(42),
		prio: mPrio(PrioHigh), tid: mU8(5), start: mBool(false), end: mBool(false), toggle: mBool(false),
		payLen: mInt(7), data: d,
	})
	checkFrame(t, "v0", v0, frameMatch{
		kind: mKind(KindV0Request), portID: mU32(4), dst: mU8(82), src: mU8(42),
		prio: mPrio(PrioHigh), tid: mU8(5), start: mBool(false), end: mBool(false), toggle: mBool(false),
		payLen: mInt(7), data: d,
	})
}

// Test 18: Payload validation -- exercises the payload_ok computation.
func TestRxParsePayloadValidation(t *testing.T) {
	const canID = uint32(0x00002A01)
	// Non-last frame under MTU -> rejected.
	{
		d := []byte{1, 2, 3, 4, 5, 6, 0x05}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 0)
	}
	// Non-last frame at exact MTU -> accepted.
	{
		d := []byte{1, 2, 3, 4, 5, 6, 7, 0x05}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 3)
	}
	// Last frame of multi-frame with empty payload -> rejected.
	{
		d := []byte{0x40}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 0)
	}
	// Single-frame with empty payload -> accepted.
	{
		d := []byte{0xC0}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 1)
	}
}

// Test 19: Anonymous multi-frame transfers are rejected.
func TestRxParseAnonymousMultiFrameReject(t *testing.T) {
	// v1.0 anonymous multi-frame -> v1 rejected; v0 excluded (toggle=1). CAN ID = 0x09606455.
	{
		d := []byte{1, 2, 3, 4, 5, 6, 7, 0xA0}
		mask, _, _ := rxParse(0x09606455, d)
		wantMask(t, mask, 0)
	}
	// v0 anonymous multi-frame -> v0 rejected; v1 excluded (toggle=0). CAN ID = 0x13040A00.
	{
		d := []byte{1, 2, 3, 4, 5, 6, 7, 0x80}
		mask, _, _ := rxParse(0x13040A00, d)
		wantMask(t, mask, 0)
	}
	// v1.0 anonymous single-frame -> accepted (sanity check).
	{
		d := []byte{0xE0}
		mask, _, _ := rxParse(0x09606455, d)
		wantMask(t, mask, 2)
	}
	// v0 anonymous single-frame -> accepted (sanity check).
	{
		d := []byte{0xC0}
		mask, _, _ := rxParse(0x13040A00, d)
		wantMask(t, mask, 1)
	}
}

// Test 20: One-byte frame (tail byte only).
func TestRxParseOneByteTailOnly(t *testing.T) {
	// v1.1 message CAN ID = 0x00000080, tail 0xE0 -> v1 single-frame.
	{
		d := []byte{0xE0}
		mask, _, v1 := rxParse(0x00000080, d)
		wantMask(t, mask, 2)
		checkFrame(t, "v1", v1, frameMatch{
			kind: mKind(KindMessage16b), payLen: mInt(0), data: d,
			start: mBool(true), end: mBool(true), toggle: mBool(true), tid: mU8(0),
		})
	}
	// v0 single-frame: tail 0xC0 -> v0 only. CAN ID = 0x00002A01.
	{
		d := []byte{0xC0}
		mask, v0, _ := rxParse(0x00002A01, d)
		wantMask(t, mask, 1)
		checkFrame(t, "v0", v0, frameMatch{
			kind: mKind(KindV0Message), payLen: mInt(0), data: d,
			start: mBool(true), end: mBool(true), toggle: mBool(false), tid: mU8(0),
		})
	}
}

// Test 21: 64-byte CAN FD frame with v1.1 message CAN ID. Tail byte at position 63.
func TestRxParseMaxFdFrame(t *testing.T) {
	d := make([]byte, 64)
	for i := range d {
		d[i] = 0xBB
	}
	d[63] = 0xEF
	mask, _, v1 := rxParse(0x0813888A, d)
	wantMask(t, mask, 2)
	checkFrame(t, "v1", v1, frameMatch{
		kind: mKind(KindMessage16b), payLen: mInt(63), data: d, tid: mU8(15),
		src: mU8(10), portID: mU32(5000), prio: mPrio(PrioFast),
	})
}

// Test 22: v1.0 service with src==dst (self-addressing) must be rejected.
func TestRxParseV1ServiceSelfAddressing(t *testing.T) {
	// Request: prio=0, svc_id=1, dst=42, src=42. CAN ID = 0x0300552A.
	{
		d := []byte{0xE0}
		mask, _, _ := rxParse(0x0300552A, d)
		wantMask(t, mask, 0)
	}
	// Response: prio=4, svc_id=100, dst=10, src=10. CAN ID = 0x1219050A.
	{
		d := []byte{0xE3}
		mask, _, _ := rxParse(0x1219050A, d)
		wantMask(t, mask, 0)
	}
}

// Test 23: v0 service with src==dst (self-addressing) must be rejected.
func TestRxParseV0ServiceSelfAddressing(t *testing.T) {
	// Request: prio=4, type_id=0x37, dst=11, src=11. CAN ID = 0x13378B8B.
	{
		d := []byte{0xC0}
		mask, _, _ := rxParse(0x13378B8B, d)
		wantMask(t, mask, 0)
	}
}

// Test 24: v0 service with src=0 must be rejected.
func TestRxParseV0ServiceZeroSrc(t *testing.T) {
	// Request: prio=4, type_id=0x37, dst=24, src=0. CAN ID = 0x13379880.
	{
		d := []byte{0xC0}
		mask, _, _ := rxParse(0x13379880, d)
		wantMask(t, mask, 0)
	}
}

// Test 25: v0 service with dst=0 must be rejected.
func TestRxParseV0ServiceZeroDst(t *testing.T) {
	// Request: prio=4, type_id=0x37, dst=0, src=11. CAN ID = 0x1337808B.
	{
		d := []byte{0xC0}
		mask, _, _ := rxParse(0x1337808B, d)
		wantMask(t, mask, 0)
	}
}

// Test 26: Middle frame (SOT=0, EOT=0) with only 1 byte (tail only). Payload=0. Rejected.
func TestRxParseNonStartNonEndEmpty(t *testing.T) {
	d := []byte{0x05}
	mask, _, _ := rxParse(0x00002A01, d)
	wantMask(t, mask, 0)
}

// Test 27: Continuation frame (non-last) that is too short is rejected; exact MTU accepted.
func TestRxParseNonLastShortFrame(t *testing.T) {
	const canID = uint32(0x00002A01)
	// 6 bytes.
	{
		d := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0x03}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 0)
	}
	// 7 bytes.
	{
		d := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x03}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 0)
	}
	// 8 bytes: at MTU.
	{
		d := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x03}
		mask, _, _ := rxParse(canID, d)
		wantMask(t, mask, 3)
	}
}

// Test 28: v1 service with self-addressing, non-SOT tail so both versions attempted.
func TestRxParseV1SvcSelfAddrDual(t *testing.T) {
	// CAN ID 0x0300552A, non-SOT tail (EOT=1, SOT=0, toggle=0). v1 rejected; v0 sees message.
	d := []byte{0xAA, 0x40}
	mask, _, _ := rxParse(0x0300552A, d)
	wantMask(t, mask, 1)
}

// Test 29: v1.1 message (16-bit subject) with bit 24 set -- rejected.
func TestRxParseV11MsgBit24Reject(t *testing.T) {
	// CAN ID = 0x01000081.
	{
		d := []byte{0xE0}
		mask, _, _ := rxParse(0x01000081, d)
		wantMask(t, mask, 0)
	}
	// Non-SOT tail so v0 also enters; v0 svc rejected (dst=0).
	{
		d := []byte{0xBB, 0x60}
		mask, _, _ := rxParse(0x01000081, d)
		wantMask(t, mask, 0)
	}
}

// Test 30: v1.0 anonymous multi-frame -- rejected on the v1 path; v0 accepted.
func TestRxParseV10AnonMultiframeReject(t *testing.T) {
	// CAN ID 0x09606455, EOT=1 SOT=0 toggle=1 -> v0 message accepted.
	d := []byte{0xAA, 0x60}
	mask, _, _ := rxParse(0x09606455, d)
	wantMask(t, mask, 1)
}

// Test 31: v0 service with dst==0, src==0, or src==dst -- v0 rejected; v1 accepted (dual tail).
func TestRxParseV0SvcNodeIDRejectDual(t *testing.T) {
	// src==dst: CAN ID 0x13378B8B.
	{
		d := []byte{0xAA, 0x40}
		mask, _, _ := rxParse(0x13378B8B, d)
		wantMask(t, mask, 2)
	}
	// dst=0: CAN ID 0x1337808B.
	{
		d := []byte{0xAA, 0x40}
		mask, _, _ := rxParse(0x1337808B, d)
		wantMask(t, mask, 2)
	}
	// src=0: CAN ID 0x13379880.
	{
		d := []byte{0xAA, 0x40}
		mask, _, _ := rxParse(0x13379880, d)
		wantMask(t, mask, 2)
	}
}

// Test 32: v0 anonymous multi-frame message -- rejected; v1 accepted (dual tail).
func TestRxParseV0AnonMultiframeReject(t *testing.T) {
	// CAN ID 0x13040A00, SOT=0 EOT=1 toggle=0 -> v1 svc accepted.
	d := []byte{0xBB, 0x40}
	mask, _, _ := rxParse(0x13040A00, d)
	wantMask(t, mask, 2)
}
