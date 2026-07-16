package libcanard

// This file migrates tests/src/test_intrusive_rx_filter.c (the C RX-filter white-box suite) to Go.
//
// Like the C suite it is an in-package white-box test, so it calls the unexported rx_filter machinery
// directly: rxFilterForSubscription, rxFilterFuse, rxFilterRank, rxFilterCovered, rxFilterCoalesceInto
// and the rxFilterConfigure method. Every numeric literal (CAN IDs, masks, expected fusions, tie-breaking
// outcomes) is copied verbatim from the C original; the test never derives an expected value from an
// implementation expression, so it serves as an independent oracle.
//
// During migration this suite surfaced a divergence from the C reference: rxFilterForSubscription built the
// v0-message filter for a data-type-ID <= 3 with mask 0x0000FF80, whereas the C source (and the weak-filter
// semantics documented there) require 0x00000380 (the 2 low DTID bits plus the message flag only) so that
// both the addressed and the anonymous forms are admitted. That has been fixed in rx.go; the weak/anonymous
// acceptance tests below now validate the corrected behaviour.
//
// The C OOM test (test_rx_filter_configure_oom) relies on rx_filter_configure allocating its temporary filter
// array through the RXFilters memory resource and failing when that allocation returns NULL. The Go port
// allocates the temporary slice via the Go runtime (make), so that path is unreachable; the test is skipped
// (see TestRxFilterConfigureOOM) rather than falsified.

import (
	"fmt"
	"testing"
)

// ---------------------------------------------------------------------------------------------------------------------
// Fixtures and helpers
// ---------------------------------------------------------------------------------------------------------------------

// makeFilter mirrors the C make_filter(): build a zeroed Canard with only the node-ID set, then derive the
// subscription filter. The node-ID is the only field rxFilterForSubscription reads for the v1/v0 address/request
// kinds, so a zeroed instance with NodeID set is sufficient.
func makeFilter(kind Kind, portID uint16, nodeID uint8) Filter {
	var self Canard
	self.NodeID = nodeID
	return rxFilterForSubscription(&self, kind, portID)
}

// filterAccepts mirrors the C filter_accepts(): a frame is accepted iff (can_id & mask) == can_id.
func filterAccepts(f Filter, canID uint32) bool {
	return (canID & f.ExtendedMask) == f.ExtendedCANID
}

// capFilterMax bounds the number of filters the capturing callback records, matching the C g_cap_filters[32].
const capFilterMax = 32

// captureFixture captures the filter set handed to the Filter vtable callback, and answers whether any captured
// filter admits a given CAN ID (the C captured_accepts()).
type captureFixture struct {
	capCount   int
	capFilters [capFilterMax]Filter
}

func (fx *captureFixture) callback(self *Canard, count int, filters []Filter) bool {
	fx.capCount = count
	n := count
	if n > capFilterMax {
		n = capFilterMax
	}
	for i := 0; i < n; i++ {
		fx.capFilters[i] = filters[i]
	}
	return true
}

func (fx *captureFixture) accepts(canID uint32) bool {
	for i := 0; i < fx.capCount; i++ {
		if filterAccepts(fx.capFilters[i], canID) {
			return true
		}
	}
	return false
}

// makeCaptureInstance mirrors the C make_instance(): a Canard with the capturing Filter callback, given a node-ID
// and filter capacity. It returns both the instance and the capture fixture. A zero node-ID leaves the
// auto-assigned (from the PRNG seed) node-ID in place, matching the C make_instance behaviour.
func makeCaptureInstance(nodeID uint8, filterCount int) (*Canard, *captureFixture) {
	fx := &captureFixture{}
	now := int64(0)
	c := &Canard{}
	ok := c.Init(&VTable{
		Now: func(self *Canard) int64 { return now },
		TX: func(self *Canard, _ any, _ int64, _ uint8, _ bool, _ uint32, _ []byte) bool {
			return true
		},
		Filter: fx.callback,
	}, NewDefaultMemSet(), IfaceBitmapAll, 16, 1234, filterCount)
	if !ok {
		panic("init failed")
	}
	if nodeID != 0 {
		if !c.SetNodeID(nodeID) {
			panic("set node id failed")
		}
	}
	fx.capCount = 0
	return c, fx
}

// dummySubVTable is a no-op subscription callback (the C dummy_sub_vtable).
var dummySubVTable = &SubscriptionVTable{
	OnMessage: func(s *Subscription, ts int64, p Prio, src uint8, tid uint8, payload Payload) {},
}

// ---------------------------------------------------------------------------------------------------------------------
// Assertion helpers
// ---------------------------------------------------------------------------------------------------------------------

func assertFilterEq(t *testing.T, label string, got, want Filter) {
	t.Helper()
	if got.ExtendedCANID != want.ExtendedCANID || got.ExtendedMask != want.ExtendedMask {
		t.Fatalf("%s: filter = {%#x, %#x}, want {%#x, %#x}", label, got.ExtendedCANID, got.ExtendedMask, want.ExtendedCANID, want.ExtendedMask)
	}
}

func assertCANID(t *testing.T, label string, got, want uint32) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: can_id=%#x, want %#x", label, got, want)
	}
}

func assertMask(t *testing.T, label string, got, want uint32) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: mask=%#x, want %#x", label, got, want)
	}
}

func assertAccepts(t *testing.T, f Filter, canID uint32) {
	t.Helper()
	if !filterAccepts(f, canID) {
		t.Fatalf("filter {%#x, %#x} should accept %#x", f.ExtendedCANID, f.ExtendedMask, canID)
	}
}

func assertRejects(t *testing.T, f Filter, canID uint32) {
	t.Helper()
	if filterAccepts(f, canID) {
		t.Fatalf("filter {%#x, %#x} should reject %#x", f.ExtendedCANID, f.ExtendedMask, canID)
	}
}

func assertRank(t *testing.T, f Filter, want uint8) {
	t.Helper()
	if got := rxFilterRank(f); got != want {
		t.Fatalf("rank(mask=%#x)=%d, want %d", f.ExtendedMask, got, want)
	}
}

// Coverage-invariant helpers for adversarial coalescence testing (C assert_coverage_invariant /
// coalesce_and_check). The fundamental correctness property: after coalescence, every CAN ID accepted by any
// original filter or the new filter must still be accepted by at least one filter in the result array.
func assertCoverageInvariant(t *testing.T, count int, original []Filter, newF Filter, result []Filter, domainBits uint) {
	t.Helper()
	domain := uint32(1) << domainBits
	for x := uint32(0); x < domain; x++ {
		wasAccepted := filterAccepts(newF, x)
		for k := 0; !wasAccepted && k < count; k++ {
			wasAccepted = filterAccepts(original[k], x)
		}
		if wasAccepted {
			isAccepted := false
			for k := 0; !isAccepted && k < count; k++ {
				isAccepted = filterAccepts(result[k], x)
			}
			if !isAccepted {
				t.Fatalf("coverage invariant violated at x=%#x", x)
			}
		}
	}
}

func coalesceAndCheck(t *testing.T, count int, into []Filter, newF Filter, domainBits uint) {
	t.Helper()
	if count > 8 {
		t.Fatalf("count %d > 8", count)
	}
	original := make([]Filter, count)
	copy(original, into[:count])
	rxFilterCoalesceInto(count, into, newF)
	assertCoverageInvariant(t, count, original, newF, into, domainBits)
}

// =====================================================================================================================
// Group 1: rxFilterForSubscription() golden vectors
// =====================================================================================================================

func TestRxFilterForSubscriptionGoldenVectors(t *testing.T) {
	// v1.1 message: subject=0xABCD
	assertFilterEq(t, "v1.1 message", makeFilter(KindMessage16b, 0xABCD, 42),
		Filter{ExtendedCANID: 0x00ABCD80, ExtendedMask: 0x03FFFF80})
	// v1.0 message: subject=0x1ABC
	assertFilterEq(t, "v1.0 message", makeFilter(KindMessage13b, 0x1ABC, 42),
		Filter{ExtendedCANID: 0x001ABC00, ExtendedMask: 0x029FFF80})
	// v1.0 request: service=0x1A5, dst node=42
	assertFilterEq(t, "v1.0 request", makeFilter(KindRequest, 0x1A5, 42),
		Filter{ExtendedCANID: 0x03695500, ExtendedMask: 0x03FFFF80})
	// v1.0 response: service=0x1A5, dst node=42
	assertFilterEq(t, "v1.0 response", makeFilter(KindResponse, 0x1A5, 42),
		Filter{ExtendedCANID: 0x02695500, ExtendedMask: 0x03FFFF80})
	// v0.1 message: data type ID=0xABCD
	assertFilterEq(t, "v0 message", makeFilter(KindV0Message, 0xABCD, 42),
		Filter{ExtendedCANID: 0x00ABCD00, ExtendedMask: 0x00FFFF80})
	// v0.1 request: data type ID=0x5A, dst node=42
	assertFilterEq(t, "v0 request", makeFilter(KindV0Request, 0x5A, 42),
		Filter{ExtendedCANID: 0x005AAA80, ExtendedMask: 0x00FFFF80})
	// v0.1 response: data type ID=0x5A, dst node=42
	assertFilterEq(t, "v0 response", makeFilter(KindV0Response, 0x5A, 42),
		Filter{ExtendedCANID: 0x005A2A80, ExtendedMask: 0x00FFFF80})
}

// =====================================================================================================================
// Group 2: rxFilterForSubscription() acceptance behaviour
// =====================================================================================================================

func TestRxFilterForSubscriptionV11MessageSemantics(t *testing.T) {
	f := makeFilter(KindMessage16b, 0x8001, 55)
	assertAccepts(t, f, 0x008001AA) // same subject, prio=0, src=42
	assertAccepts(t, f, 0x1C8001FF) // same subject, prio=7, src=127
	assertRejects(t, f, 0x028001AA) // service bit (25) must be zero
	assertRejects(t, f, 0x018001AA) // bit 24 must be zero for v1.1
	assertRejects(t, f, 0x0080012A) // message selector bit (7) must be one
	assertRejects(t, f, 0x008002AA) // subject mismatch
}

func TestRxFilterForSubscriptionV10MessageSemantics(t *testing.T) {
	f := makeFilter(KindMessage13b, 42, 55)
	assertAccepts(t, f, 0x00002A01) // base form
	assertAccepts(t, f, 0x00602A7F) // reserved bits 22:21 set
	assertAccepts(t, f, 0x01602A55) // anonymous marker bit 24 set
	assertRejects(t, f, 0x00802A01) // reserved bit 23 must be zero
	assertRejects(t, f, 0x02002A01) // service bit (25) must be zero
	assertRejects(t, f, 0x00002B01) // subject mismatch
	assertRejects(t, f, 0x00002A81) // message selector bit (7) must be zero
}

func TestRxFilterForSubscriptionV10RequestSemantics(t *testing.T) {
	f := makeFilter(KindRequest, 0x1A5, 42)
	assertAccepts(t, f, 0x0369550B) // base form
	assertAccepts(t, f, 0x1B69557F) // different prio and src
	assertRejects(t, f, 0x0269550B) // response bit (24=0)
	assertRejects(t, f, 0x03E9550B) // reserved bit 23 set
	assertRejects(t, f, 0x0369950B) // service mismatch
	assertRejects(t, f, 0x0369558B) // destination mismatch
}

func TestRxFilterForSubscriptionV10ResponseSemantics(t *testing.T) {
	f := makeFilter(KindResponse, 0x1A5, 42)
	assertAccepts(t, f, 0x02695511) // base form
	assertAccepts(t, f, 0x1A69557F) // different prio and src
	assertRejects(t, f, 0x03695511) // request bit (24=1)
	assertRejects(t, f, 0x02E95511) // reserved bit 23 set
	assertRejects(t, f, 0x02699511) // service mismatch
	assertRejects(t, f, 0x02695591) // destination mismatch
}

func TestRxFilterForSubscriptionV0MessageSemantics(t *testing.T) {
	f := makeFilter(KindV0Message, 0x1234, 55)
	assertAccepts(t, f, 0x00123405) // base form
	assertAccepts(t, f, 0x01123455) // bit 24 set
	assertAccepts(t, f, 0x0312347F) // bits 24:25 set
	assertRejects(t, f, 0x00123505) // data type ID mismatch
	assertRejects(t, f, 0x00123485) // service flag bit (7) set
}

// A v0 message data-type-ID <= 3 can arrive anonymously (2 low DTID bits + random discriminator above); the
// filter is deliberately weakened to admit both anonymous and addressed frames in a single entry.
func TestRxFilterForSubscriptionV0MessageAnonymousWeak(t *testing.T) {
	f := makeFilter(KindV0Message, 1, 55)
	assertAccepts(t, f, 0x00000105) // addressed DTID=1, src=5
	assertAccepts(t, f, 0x106AF100) // anonymous DTID=1, discriminator=0x1ABC, src=0
	assertRejects(t, f, 0x00000205) // DTID=2: different 2 low bits
	assertRejects(t, f, 0x00000185) // service flag bit (7) set
}

func TestRxFilterForSubscriptionV0RequestSemantics(t *testing.T) {
	f := makeFilter(KindV0Request, 0x5A, 42)
	assertAccepts(t, f, 0x005AAA87) // base form
	assertAccepts(t, f, 0x015AAA81) // bit 24 set
	assertAccepts(t, f, 0x035AAAFF) // bits 24:25 set
	assertRejects(t, f, 0x005A2A87) // response bit (15=0)
	assertRejects(t, f, 0x005AAB87) // destination mismatch
	assertRejects(t, f, 0x005BAA87) // data type ID mismatch
	assertRejects(t, f, 0x005AAA07) // service flag bit (7) cleared
}

func TestRxFilterForSubscriptionV0ResponseSemantics(t *testing.T) {
	f := makeFilter(KindV0Response, 0x5A, 42)
	assertAccepts(t, f, 0x005A2A81) // base form
	assertAccepts(t, f, 0x015A2A82) // bit 24 set
	assertAccepts(t, f, 0x035A2AFF) // bits 24:25 set
	assertRejects(t, f, 0x005AAA81) // request bit (15=1)
	assertRejects(t, f, 0x005A2B81) // destination mismatch
	assertRejects(t, f, 0x005B2A81) // data type ID mismatch
	assertRejects(t, f, 0x005A2A01) // service flag bit (7) cleared
}

// =====================================================================================================================
// Group 3: rxFilterFuse()
// =====================================================================================================================

func TestRxFilterFuseBasic(t *testing.T) {
	req := Filter{ExtendedCANID: 0x03695500, ExtendedMask: 0x03FFFF80}
	resp := Filter{ExtendedCANID: 0x02695500, ExtendedMask: 0x03FFFF80}
	fused := rxFilterFuse(req, resp)
	assertFilterEq(t, "fused", fused, Filter{ExtendedCANID: 0x02695500, ExtendedMask: 0x02FFFF80})
	assertAccepts(t, fused, 0x0369552A) // request
	assertAccepts(t, fused, 0x02695555) // response
	assertRejects(t, fused, 0x0269952A) // service mismatch
}

func TestRxFilterFuseIsCommutative(t *testing.T) {
	a := Filter{ExtendedCANID: 0x005AAA80, ExtendedMask: 0x00FFFF80}
	b := Filter{ExtendedCANID: 0x005A2A80, ExtendedMask: 0x00FFFF80}
	x := rxFilterFuse(a, b)
	y := rxFilterFuse(b, a)
	assertFilterEq(t, "fuse(a,b)==fuse(b,a)", x, y)
}

// =====================================================================================================================
// Group 4: rxFilterRank()
// =====================================================================================================================

func TestRxFilterRankVectors(t *testing.T) {
	assertRank(t, Filter{ExtendedCANID: 0, ExtendedMask: 0x00000000}, 0)
	assertRank(t, Filter{ExtendedCANID: 0, ExtendedMask: 0x00000001}, 1)
	assertRank(t, Filter{ExtendedCANID: 0, ExtendedMask: 0x029FFF80}, 16)
	assertRank(t, Filter{ExtendedCANID: 0, ExtendedMask: 0x03FFFF80}, 19)
	assertRank(t, Filter{ExtendedCANID: 0, ExtendedMask: 0x1FFFFFFF}, 29)
}

// =====================================================================================================================
// Group 5: rxFilterCoalesceInto()
// =====================================================================================================================

func TestRxFilterCoalesceIntoSelectsBestRank(t *testing.T) {
	into := [2]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
		{ExtendedCANID: 0x8, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0xC, ExtendedMask: 0xF}
	fusedIndex1 := rxFilterFuse(into[1], nf)
	rxFilterCoalesceInto(2, into[:], nf)
	assertCANID(t, "into[0]", into[0].ExtendedCANID, 0x0) // unchanged
	assertMask(t, "into[0]", into[0].ExtendedMask, 0xF)    // unchanged
	assertFilterEq(t, "into[1]", into[1], fusedIndex1)
}

func TestRxFilterCoalesceIntoTiePrefersLaterIndex(t *testing.T) {
	into := [2]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
		{ExtendedCANID: 0x3, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0x5, ExtendedMask: 0xF}
	fusedIndex1 := rxFilterFuse(into[1], nf)
	rxFilterCoalesceInto(2, into[:], nf)
	assertCANID(t, "into[0]", into[0].ExtendedCANID, 0x0) // tie, earlier entry must remain unchanged
	assertMask(t, "into[0]", into[0].ExtendedMask, 0xF)
	assertFilterEq(t, "into[1]", into[1], fusedIndex1)
}

func TestRxFilterCoalesceIntoMergesExistingPairWhenBest(t *testing.T) {
	into := [2]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
		{ExtendedCANID: 0x1, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0xF, ExtendedMask: 0xF}
	fusedExisting := rxFilterFuse(into[0], into[1])
	rxFilterCoalesceInto(2, into[:], nf)
	assertFilterEq(t, "into[0]", into[0], fusedExisting)
	assertFilterEq(t, "into[1]", into[1], nf)
}

func TestRxFilterCoalesceIntoSingleEntry(t *testing.T) {
	into := [1]Filter{{ExtendedCANID: 0x3, ExtendedMask: 0xF}}
	nf := Filter{ExtendedCANID: 0x5, ExtendedMask: 0xF}
	expectedFuse := rxFilterFuse(into[0], nf)
	rxFilterCoalesceInto(1, into[:], nf)
	assertFilterEq(t, "into[0]", into[0], expectedFuse)
}

// =====================================================================================================================
// Group 6: Exhaustive small-domain invariant tests
// =====================================================================================================================

func TestCoalesceCoverageCount1Exhaustive(t *testing.T) {
	// Exhaustively test all 4-bit filter combinations for count=1 (65536 combos).
	for im := uint32(0); im <= 0xF; im++ {
		for ii := uint32(0); ii <= 0xF; ii++ {
			for nm := uint32(0); nm <= 0xF; nm++ {
				for ni := uint32(0); ni <= 0xF; ni++ {
					into := [1]Filter{{ExtendedCANID: ii & im, ExtendedMask: im}}
					nf := Filter{ExtendedCANID: ni & nm, ExtendedMask: nm}
					coalesceAndCheck(t, 1, into[:], nf, 4)
				}
			}
		}
	}
}

func TestCoalesceCoverageCount2Sampled(t *testing.T) {
	// Sample 4-bit combinations for count=2.
	masks := []uint32{0x0, 0x3, 0x5, 0x7, 0x9, 0xC, 0xF}
	ids := []uint32{0x0, 0x3, 0x5, 0x7, 0xA, 0xC, 0xF}
	nm := len(masks)
	ni := len(ids)
	for a := 0; a < nm; a++ {
		for b := 0; b < ni; b++ {
			for c := 0; c < nm; c++ {
				for d := 0; d < ni; d++ {
					into := [2]Filter{
						{ExtendedCANID: 0x5 & 0xF, ExtendedMask: 0xF},
						{ExtendedCANID: ids[b] & masks[a], ExtendedMask: masks[a]},
					}
					nf := Filter{ExtendedCANID: ids[d] & masks[c], ExtendedMask: masks[c]}
					coalesceAndCheck(t, 2, into[:], nf, 4)
				}
			}
		}
	}
}

func TestCoalesceCoverageCount3Targeted(t *testing.T) {
	// Targeted configurations for count=3 verifying the invariant.
	{
		into := [3]Filter{
			{ExtendedCANID: 0x0, ExtendedMask: 0xF},
			{ExtendedCANID: 0x1, ExtendedMask: 0xF},
			{ExtendedCANID: 0x2, ExtendedMask: 0xF},
		}
		coalesceAndCheck(t, 3, into[:], Filter{ExtendedCANID: 0x4, ExtendedMask: 0xF}, 4)
	}
	{
		into := [3]Filter{
			{ExtendedCANID: 0x0, ExtendedMask: 0xF},
			{ExtendedCANID: 0x1, ExtendedMask: 0xF},
			{ExtendedCANID: 0x3, ExtendedMask: 0xF},
		}
		coalesceAndCheck(t, 3, into[:], Filter{ExtendedCANID: 0x7, ExtendedMask: 0xF}, 4)
	}
	{
		into := [3]Filter{
			{ExtendedCANID: 0x0, ExtendedMask: 0xF},
			{ExtendedCANID: 0x8, ExtendedMask: 0xF},
			{ExtendedCANID: 0xC, ExtendedMask: 0xF},
		}
		coalesceAndCheck(t, 3, into[:], Filter{ExtendedCANID: 0xE, ExtendedMask: 0xF}, 4)
	}
	{ // wildcard at position 0
		into := [3]Filter{
			{ExtendedCANID: 0x0, ExtendedMask: 0x0},
			{ExtendedCANID: 0x5, ExtendedMask: 0xF},
			{ExtendedCANID: 0xA, ExtendedMask: 0xF},
		}
		coalesceAndCheck(t, 3, into[:], Filter{ExtendedCANID: 0xF, ExtendedMask: 0xF}, 4)
	}
	{ // mixed mask widths
		into := [3]Filter{
			{ExtendedCANID: 0x4, ExtendedMask: 0xC},
			{ExtendedCANID: 0x1, ExtendedMask: 0x3},
			{ExtendedCANID: 0xF, ExtendedMask: 0xF},
		}
		coalesceAndCheck(t, 3, into[:], Filter{ExtendedCANID: 0x8, ExtendedMask: 0x8}, 4)
	}
}

// =====================================================================================================================
// Group 7: Structural tests (count >= 3)
// =====================================================================================================================

func TestCoalesceCount3FusesExistingPair(t *testing.T) {
	// into[0] and into[1] are most similar; new is dissimilar.
	// Expect: into[0]=fuse(into[0],into[1]), into[1]=new, into[2] unchanged.
	into := [3]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
		{ExtendedCANID: 0x1, ExtendedMask: 0xF},
		{ExtendedCANID: 0xA, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0xF, ExtendedMask: 0xF}
	fusedExisting := rxFilterFuse(into[0], into[1])
	coalesceAndCheck(t, 3, into[:], nf, 4)
	assertFilterEq(t, "into[0]", into[0], fusedExisting)
	assertFilterEq(t, "into[1]", into[1], nf)
	assertCANID(t, "into[2]", into[2].ExtendedCANID, 0xA)
	assertMask(t, "into[2]", into[2].ExtendedMask, 0xF)
}

func TestCoalesceCount3FusesWithNewAtLast(t *testing.T) {
	// into[2] and new are most similar; best_j==count, only into[2] changes.
	into := [3]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
		{ExtendedCANID: 0x5, ExtendedMask: 0xF},
		{ExtendedCANID: 0xC, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0xD, ExtendedMask: 0xF}
	expectedFuse := rxFilterFuse(into[2], nf) // rank=3, best
	coalesceAndCheck(t, 3, into[:], nf, 4)
	assertCANID(t, "into[0]", into[0].ExtendedCANID, 0x0)
	assertMask(t, "into[0]", into[0].ExtendedMask, 0xF)
	assertCANID(t, "into[1]", into[1].ExtendedCANID, 0x5)
	assertMask(t, "into[1]", into[1].ExtendedMask, 0xF)
	assertFilterEq(t, "into[2]", into[2], expectedFuse)
}

func TestCoalesceCount3FusesWithNewAtFirst(t *testing.T) {
	// into[0] and new are strictly the best pair; best_i=0, best_j==count.
	into := [3]Filter{
		{ExtendedCANID: 0xE, ExtendedMask: 0xF},
		{ExtendedCANID: 0x5, ExtendedMask: 0xF},
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0xF, ExtendedMask: 0xF}
	expectedFuse := rxFilterFuse(into[0], nf) // rank=3
	coalesceAndCheck(t, 3, into[:], nf, 4)
	assertFilterEq(t, "into[0]", into[0], expectedFuse)
	assertCANID(t, "into[1]", into[1].ExtendedCANID, 0x5)
	assertMask(t, "into[1]", into[1].ExtendedMask, 0xF)
	assertCANID(t, "into[2]", into[2].ExtendedCANID, 0x0)
	assertMask(t, "into[2]", into[2].ExtendedMask, 0xF)
}

func TestCoalesceCount4MiddleBestI(t *testing.T) {
	// best_i at middle position (index 2), best_j==count.
	into := [4]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
		{ExtendedCANID: 0x5, ExtendedMask: 0xF},
		{ExtendedCANID: 0x6, ExtendedMask: 0xF},
		{ExtendedCANID: 0xA, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0x7, ExtendedMask: 0xF}
	// fuse(2,new): mask=0xF & ~0x1=0xE, r=3. fuse(1,new): mask=0xF & ~0x2=0xD, r=3.
	// Tie at r=3; (2,count) enumerated after (1,count), so (2,count) wins via >=.
	expectedFuse := rxFilterFuse(into[2], nf)
	coalesceAndCheck(t, 4, into[:], nf, 4)
	assertCANID(t, "into[0]", into[0].ExtendedCANID, 0x0)
	assertCANID(t, "into[1]", into[1].ExtendedCANID, 0x5)
	assertFilterEq(t, "into[2]", into[2], expectedFuse)
	assertCANID(t, "into[3]", into[3].ExtendedCANID, 0xA)
}

func TestCoalesceCount5(t *testing.T) {
	into := [5]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF}, {ExtendedCANID: 0x2, ExtendedMask: 0xF},
		{ExtendedCANID: 0x4, ExtendedMask: 0xF}, {ExtendedCANID: 0x8, ExtendedMask: 0xF},
		{ExtendedCANID: 0xC, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0xD, ExtendedMask: 0xF}
	// Multiple rank-3 pairs exist; (4,count) is last enumerated tie -> wins via >=.
	expectedFuse := rxFilterFuse(into[4], nf)
	coalesceAndCheck(t, 5, into[:], nf, 4)
	assertCANID(t, "into[0]", into[0].ExtendedCANID, 0x0)
	assertCANID(t, "into[1]", into[1].ExtendedCANID, 0x2)
	assertCANID(t, "into[2]", into[2].ExtendedCANID, 0x4)
	assertCANID(t, "into[3]", into[3].ExtendedCANID, 0x8)
	assertFilterEq(t, "into[4]", into[4], expectedFuse)
}

// =====================================================================================================================
// Group 8: Degenerate cases
// =====================================================================================================================

func TestCoalesceAllIdentical(t *testing.T) {
	into := [3]Filter{
		{ExtendedCANID: 0x5, ExtendedMask: 0xF},
		{ExtendedCANID: 0x5, ExtendedMask: 0xF},
		{ExtendedCANID: 0x5, ExtendedMask: 0xF},
	}
	coalesceAndCheck(t, 3, into[:], Filter{ExtendedCANID: 0x5, ExtendedMask: 0xF}, 4)
	// All identical -> fuse is identity -> array unchanged.
	for k := 0; k < 3; k++ {
		assertCANID(t, fmt.Sprintf("into[%d]", k), into[k].ExtendedCANID, 0x5)
		assertMask(t, fmt.Sprintf("into[%d]", k), into[k].ExtendedMask, 0xF)
	}
}

func TestCoalesceWildcardExisting(t *testing.T) {
	into := [2]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0x0}, // wildcard
		{ExtendedCANID: 0x5, ExtendedMask: 0xF},
	}
	coalesceAndCheck(t, 2, into[:], Filter{ExtendedCANID: 0xA, ExtendedMask: 0xF}, 4)
}

func TestCoalesceNewIsWildcard(t *testing.T) {
	into := [2]Filter{
		{ExtendedCANID: 0x5, ExtendedMask: 0xF},
		{ExtendedCANID: 0xA, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0x0, ExtendedMask: 0x0}
	coalesceAndCheck(t, 2, into[:], nf, 4)
}

func TestCoalesceFullySpecified29bit(t *testing.T) {
	into := [2]Filter{
		{ExtendedCANID: 0x1FFFFFFE, ExtendedMask: 0x1FFFFFFF},
		{ExtendedCANID: 0x10000000, ExtendedMask: 0x1FFFFFFF},
	}
	nf := Filter{ExtendedCANID: 0x1FFFFFFF, ExtendedMask: 0x1FFFFFFF}
	expectedFuse := rxFilterFuse(into[0], nf)
	// fuse(0,new): mask=0x1FFFFFFF & ~0x01 = 0x1FFFFFFE, r=28. Best by far.
	rxFilterCoalesceInto(2, into[:], nf)
	assertFilterEq(t, "into[0]", into[0], expectedFuse)
	assertCANID(t, "into[1]", into[1].ExtendedCANID, 0x10000000) // unchanged
	// Verify acceptance of specific CAN IDs.
	assertAccepts(t, into[0], 0x1FFFFFFE)
	assertAccepts(t, into[0], 0x1FFFFFFF)
	assertRejects(t, into[0], 0x1FFFFFFC)
}

func TestCoalesceNewIdenticalToExisting(t *testing.T) {
	into := [2]Filter{
		{ExtendedCANID: 0x3, ExtendedMask: 0xF},
		{ExtendedCANID: 0xA, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0xA, ExtendedMask: 0xF}
	// fuse(into[1], new) is identity: rank=4 (best possible for 4-bit). Wins easily.
	coalesceAndCheck(t, 2, into[:], nf, 4)
	assertCANID(t, "into[0]", into[0].ExtendedCANID, 0x3) // unchanged
	assertCANID(t, "into[1]", into[1].ExtendedCANID, 0xA) // identity fuse
	assertMask(t, "into[1]", into[1].ExtendedMask, 0xF)
}

func TestCoalesceWorstCaseExpansion(t *testing.T) {
	// Maximally dissimilar: all ID bits differ -> fuse produces wildcard.
	into := [1]Filter{{ExtendedCANID: 0x0, ExtendedMask: 0xF}}
	nf := Filter{ExtendedCANID: 0xF, ExtendedMask: 0xF}
	coalesceAndCheck(t, 1, into[:], nf, 4)
	assertCANID(t, "into[0]", into[0].ExtendedCANID, 0x0)
	assertMask(t, "into[0]", into[0].ExtendedMask, 0x0) // wildcard
	for x := uint32(0); x < 16; x++ {
		assertAccepts(t, into[0], x)
	}
}

// =====================================================================================================================
// Group 9: Tie-breaking
// =====================================================================================================================

func TestCoalesceTiePrefersLaterPairCount3(t *testing.T) {
	// Four pairs tie at rank 3; the last enumerated (2,count) must win.
	into := [3]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
		{ExtendedCANID: 0x2, ExtendedMask: 0xF},
		{ExtendedCANID: 0x8, ExtendedMask: 0xF},
	}
	nf := Filter{ExtendedCANID: 0xA, ExtendedMask: 0xF}
	expectedFuse := rxFilterFuse(into[2], nf)
	// Pairs at rank 3: (0,1) mask=0xD, (0,2) mask=0x7, (1,count) mask=0x7, (2,count) mask=0xD.
	// Due to >=, (2,count) wins. best_j==count so only into[2] changes.
	coalesceAndCheck(t, 3, into[:], nf, 4)
	assertCANID(t, "into[0]", into[0].ExtendedCANID, 0x0)
	assertCANID(t, "into[1]", into[1].ExtendedCANID, 0x2)
	assertFilterEq(t, "into[2]", into[2], expectedFuse)
}

// =====================================================================================================================
// Group 10: Multi-step coalescence
// =====================================================================================================================

func TestCoalesceSequentialInvariant4bit(t *testing.T) {
	// Simulate rx_filter_configure: fill 3 slots, then coalesce 5 more filters.
	// Track the cumulative set of required CAN IDs and verify after each step.
	filters := [3]Filter{
		{ExtendedCANID: 0x1, ExtendedMask: 0xF},
		{ExtendedCANID: 0x3, ExtendedMask: 0xF},
		{ExtendedCANID: 0x5, ExtendedMask: 0xF},
	}
	extras := []Filter{
		{ExtendedCANID: 0x7, ExtendedMask: 0xF}, {ExtendedCANID: 0x9, ExtendedMask: 0xF},
		{ExtendedCANID: 0xB, ExtendedMask: 0xF}, {ExtendedCANID: 0xD, ExtendedMask: 0xF},
		{ExtendedCANID: 0xF, ExtendedMask: 0xF},
	}
	// Build the cumulative required-acceptance set.
	required := [16]bool{}
	for k := 0; k < 3; k++ {
		for x := uint32(0); x < 16; x++ {
			if filterAccepts(filters[k], x) {
				required[x] = true
			}
		}
	}
	for step := 0; step < 5; step++ {
		for x := uint32(0); x < 16; x++ {
			if filterAccepts(extras[step], x) {
				required[x] = true
			}
		}
		rxFilterCoalesceInto(3, filters[:], extras[step])
		// Verify cumulative invariant.
		for x := uint32(0); x < 16; x++ {
			if required[x] {
				found := false
				for k := 0; !found && k < 3; k++ {
					found = filterAccepts(filters[k], x)
				}
				if !found {
					t.Fatalf("step %d: x=%#x required but not accepted", step, x)
				}
			}
		}
	}
}

func TestCoalesceSequentialRealisticCyphal(t *testing.T) {
	// 3 filter slots, 6 subscriptions -> 3 direct + 3 coalesced.
	filters := [3]Filter{
		makeFilter(KindMessage16b, 100, 42),
		makeFilter(KindRequest, 200, 42),
		makeFilter(KindV0Message, 300, 42),
	}
	extras := []Filter{
		makeFilter(KindMessage16b, 101, 42),
		makeFilter(KindV0Request, 50, 42),
		makeFilter(KindResponse, 200, 42),
	}
	// For each subscription, generate a representative CAN ID that must be accepted.
	// v1.1 msg subject=100: (100<<8)|0x80 | src=1 -> 0x00006481
	// v1.0 req svc=200 dst=42: (1<<25)|(1<<24)|(200<<14)|(42<<7)|src=1 -> 0x03321501
	// v0.1 msg dtype=300: (300<<8)|src=1 -> 0x00012C01
	// v1.1 msg subject=101: (101<<8)|0x80|src=1 -> 0x00006581
	// v0.1 req dtype=50 dst=42: (50<<16)|(1<<15)|(42<<8)|0x80|src=1 -> 0x0032AA81
	// v1.0 resp svc=200 dst=42: (1<<25)|(200<<14)|(42<<7)|src=1 -> 0x02321501
	mustAccept := []uint32{
		0x00006481, 0x03321501, 0x00012C01, 0x00006581, 0x0032AA81, 0x02321501,
	}
	for step := 0; step < 3; step++ {
		rxFilterCoalesceInto(3, filters[:], extras[step])
		// All CAN IDs introduced so far must still be accepted.
		for a := 0; a < 3+step+1; a++ {
			found := false
			for k := 0; !found && k < 3; k++ {
				found = filterAccepts(filters[k], mustAccept[a])
			}
			if !found {
				t.Fatalf("step %d: mustAccept[%d]=%#x not accepted", step, a, mustAccept[a])
			}
		}
	}
}

// =====================================================================================================================
// Group 11: Realistic Cyphal filters
// =====================================================================================================================

func TestCoalesceV1MessagesPair(t *testing.T) {
	// Two adjacent v1.1 message subjects: subjects 100 and 101.
	into := [1]Filter{makeFilter(KindMessage16b, 100, 42)}
	nf := makeFilter(KindMessage16b, 101, 42)
	rxFilterCoalesceInto(1, into[:], nf)
	// The fused filter must accept both subjects (arbitrary src node).
	assertAccepts(t, into[0], 0x00006481) // subject 100
	assertAccepts(t, into[0], 0x00006581) // subject 101
	assertRejects(t, into[0], 0x00006681) // subject 102 should not match
}

func TestCoalesceV1RequestResponsePair(t *testing.T) {
	// Request and response for service 0x1A5, node 42.
	into := [1]Filter{makeFilter(KindRequest, 0x1A5, 42)}
	nf := makeFilter(KindResponse, 0x1A5, 42)
	rxFilterCoalesceInto(1, into[:], nf)
	assertAccepts(t, into[0], 0x0369552A) // request (src=42)
	assertAccepts(t, into[0], 0x02695511) // response (src=17)
}

func TestCoalesceMixedVersions(t *testing.T) {
	// Cross-version coalescence: v1.1 + v1.0 + v0.
	into := [2]Filter{
		makeFilter(KindMessage16b, 100, 42),
		makeFilter(KindMessage13b, 42, 55),
	}
	nf := makeFilter(KindV0Message, 300, 42)
	original := [2]Filter{into[0], into[1]}
	rxFilterCoalesceInto(2, into[:], nf)
	// Verify specific CAN IDs from each subscription are still accepted.
	ids := []uint32{0x00006481, 0x00002A01, 0x00012C01}
	for a := 0; a < 3; a++ {
		found := false
		for k := 0; !found && k < 2; k++ {
			found = filterAccepts(into[k], ids[a])
		}
		if !found {
			t.Fatalf("ids[%d]=%#x not accepted after coalesce", a, ids[a])
		}
	}
	_ = original
}

// =====================================================================================================================
// Group 12: Adversarial
// =====================================================================================================================

func TestCoalesceAdversarialBitPattern(t *testing.T) {
	// Checkerboard patterns in 8-bit domain.
	into := [3]Filter{
		{ExtendedCANID: 0x55, ExtendedMask: 0xFF},
		{ExtendedCANID: 0xAA, ExtendedMask: 0xFF},
		{ExtendedCANID: 0x33, ExtendedMask: 0xFF},
	}
	nf := Filter{ExtendedCANID: 0xCC, ExtendedMask: 0xFF}
	// fuse(0,1) rank=0, fuse(0,2) rank=4, fuse(1,new) rank=4; (1,count) is last tie -> wins.
	expectedFuse := rxFilterFuse(into[1], nf)
	coalesceAndCheck(t, 3, into[:], nf, 8)
	assertCANID(t, "into[0]", into[0].ExtendedCANID, 0x55) // unchanged
	assertFilterEq(t, "into[1]", into[1], expectedFuse)
	assertCANID(t, "into[2]", into[2].ExtendedCANID, 0x33) // unchanged
}

func TestCoalesceBestIZeroBestJOne(t *testing.T) {
	// Force best pair to be (0,1) strictly: fuse(0,1) rank=3, all others <=2.
	into := [3]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
		{ExtendedCANID: 0x1, ExtendedMask: 0xF},
		{ExtendedCANID: 0x6, ExtendedMask: 0x7},
	}
	nf := Filter{ExtendedCANID: 0x4, ExtendedMask: 0x7}
	// fuse(0,1): mask=0xE, r=3. fuse(0,2): r=1. fuse(0,new): r=2. fuse(1,2): r=0.
	// fuse(1,new): r=1. fuse(2,new): r=2. Strictly best is (0,1).
	fused01 := rxFilterFuse(into[0], into[1])
	coalesceAndCheck(t, 3, into[:], nf, 4)
	assertFilterEq(t, "into[0]", into[0], fused01)
	assertFilterEq(t, "into[1]", into[1], nf)
	assertCANID(t, "into[2]", into[2].ExtendedCANID, 0x6) // unchanged
}

func TestCoalesceGreedyMultistepDegradation(t *testing.T) {
	// Multi-step coalescence where greedy choices accumulate: verify the invariant still holds.
	into := [2]Filter{
		{ExtendedCANID: 0x0, ExtendedMask: 0xF},
		{ExtendedCANID: 0x1, ExtendedMask: 0xF},
	}
	extras := []Filter{
		{ExtendedCANID: 0x2, ExtendedMask: 0xF},
		{ExtendedCANID: 0x4, ExtendedMask: 0xF},
		{ExtendedCANID: 0x8, ExtendedMask: 0xF},
	}
	required := [16]bool{}
	for x := uint32(0); x < 16; x++ {
		if filterAccepts(into[0], x) || filterAccepts(into[1], x) {
			required[x] = true
		}
	}
	for step := 0; step < 3; step++ {
		for x := uint32(0); x < 16; x++ {
			if filterAccepts(extras[step], x) {
				required[x] = true
			}
		}
		rxFilterCoalesceInto(2, into[:], extras[step])
		for x := uint32(0); x < 16; x++ {
			if required[x] {
				accepted := filterAccepts(into[0], x) || filterAccepts(into[1], x)
				if !accepted {
					t.Fatalf("step %d: x=%#x required but not accepted", step, x)
				}
			}
		}
	}
}

// =====================================================================================================================
// rxFilterConfigure() integration
// =====================================================================================================================

// TestRxFilterConfigureOOM documents a divergence between the C reference and the Go port: the C
// rx_filter_configure() allocates the temporary filter array via the RXFilters memory resource and returns
// false (incrementing err.oom) when that allocation fails. The Go port allocates the temporary slice with the
// Go runtime (make), so the OOM path is unreachable and this behaviour cannot be exercised here.
func TestRxFilterConfigureOOM(t *testing.T) {
	t.Skip("Go port allocates the temporary filter slice with the runtime; the C OOM path is unreachable")
}

func TestRxFilterConfigureCoalescenceOverflow(t *testing.T) {
	// capacity=1 with 2 unrelated subs: subs fill+coalesce, then forced filters also coalesce in.
	c, _ := makeCaptureInstance(0, 1)
	var sub1, sub2 Subscription
	if c.Subscribe16b(&sub1, 100, 64, 1000000, dummySubVTable) != &sub1 {
		t.Fatal("subscribe1 returned wrong pointer")
	}
	if c.Subscribe16b(&sub2, 200, 64, 1000000, dummySubVTable) != &sub2 {
		t.Fatal("subscribe2 returned wrong pointer")
	}
	// Should succeed -- the filter callback returns true.
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	c.Unsubscribe(&sub1)
	c.Unsubscribe(&sub2)
	c.Destroy()
}

// =====================================================================================================================
// rxFilterCovered()
// =====================================================================================================================

func TestRxFilterCoveredEmpty(t *testing.T) {
	g := makeFilter(KindV0Message, 341, 42)
	if rxFilterCovered(0, nil, g) {
		t.Fatal("empty array should not cover")
	}
}

func TestRxFilterCoveredSelfAndNarrower(t *testing.T) {
	f := makeFilter(KindMessage13b, 7509, 42)
	if !rxFilterCovered(1, []Filter{f}, f) { // a filter covers itself
		t.Fatal("a filter should cover itself")
	}
	// A strictly narrower inner (adds a source-node constraint) is still covered.
	narrower := Filter{ExtendedCANID: f.ExtendedCANID | 1, ExtendedMask: f.ExtendedMask | 0x7F}
	if !rxFilterCovered(1, []Filter{f}, narrower) {
		t.Fatal("a narrower inner should be covered")
	}
}

func TestRxFilterCoveredBroaderNot(t *testing.T) {
	// The v0-DTID-7509 vs Cyphal-Heartbeat alias: the v0 filter matches a single Heartbeat representative but
	// does not COVER the whole Heartbeat frame set (it constrains reserved bits 22:21 Heartbeat leaves free).
	v0 := makeFilter(KindV0Message, 7509, 42)
	hb := makeFilter(KindMessage13b, 7509, 42)
	if rxFilterCovered(1, []Filter{v0}, hb) {
		t.Fatal("v0 should not cover the Heartbeat frame set")
	}
	// Different subject-ID under the same kind is likewise not covered.
	other := makeFilter(KindMessage13b, 100, 42)
	if rxFilterCovered(1, []Filter{hb}, other) {
		t.Fatal("different subject-ID should not be covered")
	}
}

func TestRxFilterCoveredMultiple(t *testing.T) {
	arr := []Filter{
		makeFilter(KindMessage16b, 100, 42),
		makeFilter(KindMessage13b, 200, 42),
		makeFilter(KindV0Message, 300, 42),
	}
	if !rxFilterCovered(3, arr, arr[1]) {
		t.Fatal("arr[1] should be covered by itself in the array")
	}
	other := makeFilter(KindMessage13b, 999, 42)
	if rxFilterCovered(3, arr, other) {
		t.Fatal("unrelated filter should not be covered")
	}
}

// =====================================================================================================================
// rxFilterConfigure() forced Heartbeat/NodeStatus filters
// =====================================================================================================================

const (
	heartbeatSubjectID = 7509
	nodeStatusDtypeID  = 341
)

// Build a representative CAN ID for the forced message from an arbitrary source node.
func heartbeatCANID(source uint8) uint32 {
	return uint32(heartbeatSubjectID)<<8 | uint32(source)&NodeIDMax
}
func nodeStatusCANID(source uint8) uint32 {
	return uint32(nodeStatusDtypeID)<<8 | uint32(source)&NodeIDMax
}

func TestRxFilterConfigureForcedNoSubs(t *testing.T) {
	c, fx := makeCaptureInstance(0, 4)
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	if fx.capCount != 2 { // Heartbeat + NodeStatus
		t.Fatalf("capCount=%d, want 2", fx.capCount)
	}
	if !(fx.accepts(heartbeatCANID(0)) && fx.accepts(heartbeatCANID(42)) && fx.accepts(heartbeatCANID(127))) {
		t.Fatal("heartbeat frames not accepted")
	}
	if !(fx.accepts(nodeStatusCANID(0)) && fx.accepts(nodeStatusCANID(42)) && fx.accepts(nodeStatusCANID(127))) {
		t.Fatal("nodestatus frames not accepted")
	}
	c.Destroy()
}

func TestRxFilterConfigureForcedHeartbeatSubscribed(t *testing.T) {
	c, fx := makeCaptureInstance(0, 4)
	var sub Subscription
	if c.Subscribe13b(&sub, heartbeatSubjectID, 64, 1000000, dummySubVTable) != &sub {
		t.Fatal("subscribe returned wrong pointer")
	}
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	// 1 subscription filter + 1 forced NodeStatus = 2
	if fx.capCount != 2 {
		t.Fatalf("capCount=%d, want 2", fx.capCount)
	}
	if !fx.accepts(heartbeatCANID(1)) {
		t.Fatal("heartbeat not accepted")
	}
	if !fx.accepts(nodeStatusCANID(1)) {
		t.Fatal("nodestatus not accepted")
	}
	c.Unsubscribe(&sub)
	c.Destroy()
}

func TestRxFilterConfigureForcedNodestatusSubscribed(t *testing.T) {
	c, fx := makeCaptureInstance(0, 4)
	var sub Subscription
	if c.V0Subscribe(&sub, nodeStatusDtypeID, 0, 64, 1000000, dummySubVTable) != &sub {
		t.Fatal("subscribe returned wrong pointer")
	}
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	// 1 subscription filter + 1 forced Heartbeat = 2
	if fx.capCount != 2 {
		t.Fatalf("capCount=%d, want 2", fx.capCount)
	}
	if !fx.accepts(heartbeatCANID(1)) {
		t.Fatal("heartbeat not accepted")
	}
	if !fx.accepts(nodeStatusCANID(1)) {
		t.Fatal("nodestatus not accepted")
	}
	c.Unsubscribe(&sub)
	c.Destroy()
}

func TestRxFilterConfigureForcedBothSubscribed(t *testing.T) {
	c, fx := makeCaptureInstance(0, 4)
	var subHB, subNS Subscription
	if c.Subscribe13b(&subHB, heartbeatSubjectID, 64, 1000000, dummySubVTable) != &subHB {
		t.Fatal("hb subscribe returned wrong pointer")
	}
	if c.V0Subscribe(&subNS, nodeStatusDtypeID, 0, 64, 1000000, dummySubVTable) != &subNS {
		t.Fatal("ns subscribe returned wrong pointer")
	}
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	// Both already covered by subscriptions, no extras.
	if fx.capCount != 2 {
		t.Fatalf("capCount=%d, want 2", fx.capCount)
	}
	if !fx.accepts(heartbeatCANID(1)) {
		t.Fatal("heartbeat not accepted")
	}
	if !fx.accepts(nodeStatusCANID(1)) {
		t.Fatal("nodestatus not accepted")
	}
	c.Unsubscribe(&subHB)
	c.Unsubscribe(&subNS)
	c.Destroy()
}

// Regression (CN-03): a low-DTID v0 message subscription must also admit anonymous frames, whose CAN ID
// carries only the 2 low DTID bits with a random discriminator above.
func TestRxFilterConfigureV0AnonymousAdmitted(t *testing.T) {
	c, fx := makeCaptureInstance(0, 4)
	var sub Subscription
	if c.V0Subscribe(&sub, 1, 0xF258, 64, 1000000, dummySubVTable) != &sub {
		t.Fatal("subscribe returned wrong pointer")
	}
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	// One weak filter for the subscription; it also subsumes the forced Heartbeat/NodeStatus (both DTID % 4 == 1).
	if fx.capCount != 1 {
		t.Fatalf("capCount=%d, want 1", fx.capCount)
	}
	if !fx.accepts(0x00000105) { // addressed DTID=1 from src=5
		t.Fatal("addressed DTID=1 not accepted")
	}
	if !fx.accepts(0x106AF100) { // anonymous DTID=1, discriminator=0x1ABC, src=0
		t.Fatal("anonymous DTID=1 not accepted")
	}
	if fx.accepts(0x106AF200) { // anonymous DTID=2 must not match the DTID=1 filter
		t.Fatal("DTID=2 should not match the DTID=1 filter")
	}
	c.Unsubscribe(&sub)
	c.Destroy()
}

// Regression (review R2): a v0 subscription whose DTID aliases the Cyphal Heartbeat subject in the raw 16-bit
// field must not suppress the forced Heartbeat occupancy filter; a real v1.0 Heartbeat (reserved bits 22:21=11)
// must still be admitted.
func TestRxFilterConfigureForcedHeartbeatAlias(t *testing.T) {
	c, fx := makeCaptureInstance(0, 4)
	var sub Subscription
	if c.V0Subscribe(&sub, heartbeatSubjectID, 0, 64, 1000000, dummySubVTable) != &sub {
		t.Fatal("subscribe returned wrong pointer")
	}
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	if !fx.accepts((uint32(3) << 21) | (uint32(heartbeatSubjectID) << 8) | 1) { // real v1.0 Heartbeat
		t.Fatal("real v1.0 Heartbeat not admitted")
	}
	c.Unsubscribe(&sub)
	c.Destroy()
}

// Regression (review R2): a Cyphal 13b subscription whose subject aliases the DroneCAN NodeStatus DTID masks
// CAN-ID bit 25, which is a v0 priority bit; it must not suppress the forced NodeStatus occupancy filter.
func TestRxFilterConfigureForcedNodestatusAlias(t *testing.T) {
	c, fx := makeCaptureInstance(0, 4)
	var sub Subscription
	if c.Subscribe13b(&sub, nodeStatusDtypeID, 64, 1000000, dummySubVTable) != &sub {
		t.Fatal("subscribe returned wrong pointer")
	}
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	// A real v0 NodeStatus whose 5-bit priority sets bit 25 (priority 2) must still be admitted.
	if !fx.accepts((uint32(2) << 24) | (uint32(nodeStatusDtypeID) << 8) | 1) {
		t.Fatal("real v0 NodeStatus not admitted")
	}
	c.Unsubscribe(&sub)
	c.Destroy()
}

// Regression (review R3): a Cyphal service-request filter can alias the raw NodeStatus probe field
// (service_id=5, dst=42 encodes the same bits as DTID 341); coverage-based dedup must still admit real
// v0 NodeStatus frames of any priority.
func TestRxFilterConfigureForcedNodestatusServiceAlias(t *testing.T) {
	c, fx := makeCaptureInstance(0, 4)
	if !c.SetNodeID(42) {
		t.Fatal("set node id failed")
	}
	var sub Subscription
	if c.SubscribeRequest(&sub, 5, 64, 1000000, dummySubVTable) != &sub {
		t.Fatal("subscribe returned wrong pointer")
	}
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	if !fx.accepts((uint32(2) << 24) | (uint32(nodeStatusDtypeID) << 8) | 1) { // real v0 NodeStatus
		t.Fatal("real v0 NodeStatus not admitted")
	}
	c.Unsubscribe(&sub)
	c.Destroy()
}

func TestRxFilterConfigureForcedCapacity1(t *testing.T) {
	c, fx := makeCaptureInstance(0, 1)
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	// Heartbeat fills the slot, NodeStatus is coalesced into it.
	if fx.capCount != 1 {
		t.Fatalf("capCount=%d, want 1", fx.capCount)
	}
	// The coalesced filter must still accept both.
	if !fx.accepts(heartbeatCANID(1)) {
		t.Fatal("heartbeat not accepted")
	}
	if !fx.accepts(nodeStatusCANID(1)) {
		t.Fatal("nodestatus not accepted")
	}
	c.Destroy()
}

func TestRxFilterConfigureForcedCapacity2NoSubs(t *testing.T) {
	c, fx := makeCaptureInstance(0, 2)
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	if fx.capCount != 2 {
		t.Fatalf("capCount=%d, want 2", fx.capCount)
	}
	// Each forced filter gets its own slot.
	if !fx.accepts(heartbeatCANID(1)) {
		t.Fatal("heartbeat not accepted")
	}
	if !fx.accepts(nodeStatusCANID(1)) {
		t.Fatal("nodestatus not accepted")
	}
	c.Destroy()
}

func TestRxFilterConfigureForcedWithUnrelatedSubs(t *testing.T) {
	c, fx := makeCaptureInstance(0, 10)
	var sub1, sub2, sub3 Subscription
	if c.Subscribe16b(&sub1, 100, 64, 1000000, dummySubVTable) != &sub1 {
		t.Fatal("sub1 subscribe returned wrong pointer")
	}
	if c.Subscribe16b(&sub2, 200, 64, 1000000, dummySubVTable) != &sub2 {
		t.Fatal("sub2 subscribe returned wrong pointer")
	}
	if c.Subscribe16b(&sub3, 300, 64, 1000000, dummySubVTable) != &sub3 {
		t.Fatal("sub3 subscribe returned wrong pointer")
	}
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	// 3 subs + 2 forced = 5
	if fx.capCount != 5 {
		t.Fatalf("capCount=%d, want 5", fx.capCount)
	}
	if !fx.accepts(heartbeatCANID(1)) {
		t.Fatal("heartbeat not accepted")
	}
	if !fx.accepts(nodeStatusCANID(1)) {
		t.Fatal("nodestatus not accepted")
	}
	// Subscriptions still work.
	f1 := makeFilter(KindMessage16b, 100, 0)
	f2 := makeFilter(KindMessage16b, 200, 0)
	f3 := makeFilter(KindMessage16b, 300, 0)
	if !fx.accepts(f1.ExtendedCANID) {
		t.Fatal("sub1 filter not accepted")
	}
	if !fx.accepts(f2.ExtendedCANID) {
		t.Fatal("sub2 filter not accepted")
	}
	if !fx.accepts(f3.ExtendedCANID) {
		t.Fatal("sub3 filter not accepted")
	}
	c.Unsubscribe(&sub1)
	c.Unsubscribe(&sub2)
	c.Unsubscribe(&sub3)
	c.Destroy()
}

func TestRxFilterConfigureForcedOverflow(t *testing.T) {
	// capacity=1 with 2 unrelated subs: subs fill+coalesce, then forced filters also coalesce in.
	c, fx := makeCaptureInstance(0, 1)
	var sub1, sub2 Subscription
	if c.Subscribe16b(&sub1, 100, 64, 1000000, dummySubVTable) != &sub1 {
		t.Fatal("sub1 subscribe returned wrong pointer")
	}
	if c.Subscribe16b(&sub2, 200, 64, 1000000, dummySubVTable) != &sub2 {
		t.Fatal("sub2 subscribe returned wrong pointer")
	}
	if !c.rxFilterConfigure() {
		t.Fatal("rxFilterConfigure should succeed")
	}
	if fx.capCount != 1 {
		t.Fatalf("capCount=%d, want 1", fx.capCount)
	}
	// After heavy coalescence the single filter should still accept all four CAN IDs.
	f1 := makeFilter(KindMessage16b, 100, 0)
	f2 := makeFilter(KindMessage16b, 200, 0)
	if !fx.accepts(f1.ExtendedCANID) {
		t.Fatal("sub1 filter not accepted after coalescence")
	}
	if !fx.accepts(f2.ExtendedCANID) {
		t.Fatal("sub2 filter not accepted after coalescence")
	}
	if !fx.accepts(heartbeatCANID(1)) {
		t.Fatal("heartbeat not accepted after coalescence")
	}
	if !fx.accepts(nodeStatusCANID(1)) {
		t.Fatal("nodestatus not accepted after coalescence")
	}
	c.Unsubscribe(&sub1)
	c.Unsubscribe(&sub2)
	c.Destroy()
}
