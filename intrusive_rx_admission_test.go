package libcanard

// This file migrates tests/src/test_intrusive_rx_admission.c (the C RX-admission white-box suite) to Go.
//
// It is an in-package white-box test, calling the unexported rxSessionSolveAdmission,
// rxSessionRecordAdmission and rxSlotNew directly. The admission formula for start-of-transfer
// frames is a majority vote:
//
//	(fresh && affine) || (affine && stale) || (stale && fresh)
//
// where:
//
//	fresh  = (transfer_id != last_admitted_transfer_id) || (priority != last_admitted_priority)
//	affine = (ses.iface_index == iface_index)
//	stale  = (ts - transfer_id_timeout) > last_admission_ts
//
// Continuation frames require an exact slot match: priority, transfer_id, iface_index, expected_toggle.
//
// During migration this suite surfaced a divergence from the C reference: rxSessionRecordAdmission and
// rxSlotNew did not mask iface_index with (1<<IfaceCount)-1 as the C code does. That has been fixed in
// rx.go; the masking assertions in testRecordAdmission/Continuation* now validate the corrected behavior.

import (
	"testing"
)

const mega = 1_000_000

// admissionFixture mirrors the C fixture_t: a subscription + session wired together, with an empty
// slot table and BIG_BANG as the initial last-admission timestamp.
type admissionFixture struct {
	canard Canard
	sub    Subscription
	ses    rxSession
}

func admissionFixtureInit(t *testing.T, tidTimeout int64) *admissionFixture {
	t.Helper()
	f := &admissionFixture{}
	f.sub.Owner = &f.canard
	f.sub.TransferIDTimeout = tidTimeout
	f.sub.Extent = 64
	f.sub.Kind = KindMessage16b
	f.sub.CRCSeed = crcInitial
	f.ses.owner = &f.sub
	f.ses.lastAdmissionTs = bigBang
	return f
}

// solveStart calls the solver for a start-of-transfer frame.
func (f *admissionFixture) solveStart(ts int64, prio Prio, tid, iface uint8) bool {
	return rxSessionSolveAdmission(&f.ses, ts, prio, true, true, tid, iface)
}

// solveCont calls the solver for a continuation frame.
func (f *admissionFixture) solveCont(ts int64, prio Prio, toggle bool, tid, iface uint8) bool {
	return rxSessionSolveAdmission(&f.ses, ts, prio, false, toggle, tid, iface)
}

// record records admission into the session.
func (f *admissionFixture) record(prio Prio, tid uint8, ts int64, iface uint8) {
	rxSessionRecordAdmission(&f.ses, prio, tid, ts, iface)
}

// freeSlots destroys any created slots (the Go port uses GC, so this just mirrors the C teardown).
func (f *admissionFixture) freeSlots() {
	for i := 0; i < PrioCount; i++ {
		if f.ses.slots[i] != nil {
			rxSlotDestroy(&f.sub, f.ses.slots[i])
			f.ses.slots[i] = nil
		}
	}
}

// Group 1: Start-frame truth table — exhaustive {fresh, affine, stale} combinations.
// Baseline: last_tid=5, last_prio=nominal(4), iface=0, last_ts=1e6, timeout=2e6.
func TestAdmissionStartTruthTable(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.lastAdmittedTransferID = 5
	f.ses.lastAdmittedPriority = uint8(PrioNominal)
	f.ses.ifaceIndex = 0
	f.ses.lastAdmissionTs = 1 * mega

	if f.solveStart(2*mega, PrioNominal, 5, 1) { // F,F,F
		t.Fatal("row0: F,F,F should reject")
	}
	if f.solveStart(4*mega, PrioNominal, 5, 1) { // F,F,T
		t.Fatal("row1: F,F,T should reject")
	}
	if f.solveStart(2*mega, PrioNominal, 5, 0) { // F,T,F
		t.Fatal("row2: F,T,F should reject")
	}
	if !f.solveStart(4*mega, PrioNominal, 5, 0) { // F,T,T
		t.Fatal("row3: F,T,T should admit")
	}
	if f.solveStart(2*mega, PrioNominal, 6, 1) { // T,F,F
		t.Fatal("row4: T,F,F should reject")
	}
	if !f.solveStart(4*mega, PrioNominal, 6, 1) { // T,F,T
		t.Fatal("row5: T,F,T should admit")
	}
	if !f.solveStart(2*mega, PrioNominal, 6, 0) { // T,T,F
		t.Fatal("row6: T,T,F should admit")
	}
	if !f.solveStart(4*mega, PrioNominal, 6, 0) { // T,T,T
		t.Fatal("row7: T,T,T should admit")
	}
	f.freeSlots()
}

// Group 2: Fresh condition variants — what makes a frame "fresh".
func TestAdmissionFreshVariants(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.lastAdmittedTransferID = 10
	f.ses.lastAdmittedPriority = uint8(PrioHigh)
	f.ses.ifaceIndex = 0
	f.ses.lastAdmissionTs = 1 * mega

	if !f.solveStart(2*mega, PrioHigh, 11, 0) { // fresh via TID only
		t.Fatal("fresh via TID only should admit")
	}
	if !f.solveStart(2*mega, PrioLow, 10, 0) { // fresh via priority only
		t.Fatal("fresh via priority only should admit")
	}
	if !f.solveStart(2*mega, PrioLow, 11, 0) { // fresh via both
		t.Fatal("fresh via both should admit")
	}
	if f.solveStart(2*mega, PrioHigh, 10, 0) { // not fresh
		t.Fatal("not fresh should reject")
	}

	f.ses.lastAdmittedTransferID = 31
	if !f.solveStart(2*mega, PrioHigh, 0, 0) { // last=31, incoming=0 -> fresh
		t.Fatal("TID boundary 31->0 should admit")
	}
	f.ses.lastAdmittedTransferID = 0
	if !f.solveStart(2*mega, PrioHigh, 31, 0) { // last=0, incoming=31 -> fresh
		t.Fatal("TID boundary 0->31 should admit")
	}
	f.freeSlots()
}

// Group 3: Stale boundary — strict inequality and zero timeout edge cases.
func TestAdmissionStaleBoundary(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.lastAdmittedTransferID = 5
	f.ses.lastAdmittedPriority = uint8(PrioNominal)
	f.ses.ifaceIndex = 0
	f.ses.lastAdmissionTs = 1 * mega

	// Exact boundary: ts-timeout == last_ts -> NOT stale.
	if f.solveStart(3*mega, PrioNominal, 5, 0) {
		t.Fatal("exact stale boundary should reject")
	}
	// One tick past.
	if !f.solveStart((3*mega)+1, PrioNominal, 5, 0) {
		t.Fatal("one tick past stale boundary should admit")
	}

	// Zero timeout: ts == last_ts -> not stale.
	f.sub.TransferIDTimeout = 0
	if f.solveStart(1*mega, PrioNominal, 5, 0) {
		t.Fatal("zero-timeout ts==last_ts should reject")
	}
	// Zero timeout: ts == last_ts+1 -> stale.
	if !f.solveStart((1*mega)+1, PrioNominal, 5, 0) {
		t.Fatal("zero-timeout ts==last_ts+1 should admit")
	}
	f.freeSlots()
}

// Group 4: Continuation frames — exact slot match required, timeout irrelevant.
func TestAdmissionContinuationFrames(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	f.ses.lastAdmissionTs = 1 * mega

	// No slot at requested priority -> reject.
	if f.solveCont(2*mega, PrioNominal, true, 5, 0) {
		t.Fatal("continuation with no slot should reject")
	}

	// Create a slot at prio=nominal with tid=5, iface=0, toggle=1 (v1 protocol).
	f.ses.slots[PrioNominal] = rxSlotNew(&f.sub, 1*mega, 5, 0)
	if f.ses.slots[PrioNominal] == nil {
		t.Fatal("rxSlotNew returned nil")
	}

	if !f.solveCont(2*mega, PrioNominal, true, 5, 0) { // all fields match
		t.Fatal("continuation exact match should admit")
	}
	if f.solveCont(2*mega, PrioNominal, true, 6, 0) { // TID mismatch
		t.Fatal("continuation TID mismatch should reject")
	}
	if f.solveCont(2*mega, PrioNominal, true, 5, 1) { // iface mismatch
		t.Fatal("continuation iface mismatch should reject")
	}
	if f.solveCont(2*mega, PrioNominal, false, 5, 0) { // toggle mismatch
		t.Fatal("continuation toggle mismatch should reject")
	}
	if f.solveCont(2*mega, PrioHigh, true, 5, 0) { // different priority (no slot there)
		t.Fatal("continuation at absent priority should reject")
	}
	// Timeout irrelevant: exact slot match admitted even long after timeout.
	if !f.solveCont(100*mega, PrioNominal, true, 5, 0) {
		t.Fatal("continuation should ignore timeout")
	}
	f.freeSlots()
}

// Group 5: First frame to new session — BIG_BANG initial state.
func TestAdmissionFirstFrameBigBang(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)

	if !f.solveStart(1*mega, PrioNominal, 5, 0) { // fresh (5!=0), affine
		t.Fatal("first frame should admit")
	}
	if !f.solveStart(1*mega, PrioExceptional, 0, 0) { // matches zeroed state but stale(BIG_BANG) -> affine&&stale
		t.Fatal("zeroed-match frame should admit via stale")
	}
	if f.solveStart(1*mega, PrioExceptional, 0, 1) { // fresh=false, affine=false, stale -> only 1/3
		t.Fatal("zeroed-match on wrong iface should reject")
	}
	if !f.solveStart(1*mega, PrioNominal, 1, 1) { // different iface but different TID -> 2/3
		t.Fatal("different TID on different iface should admit")
	}
	f.freeSlots()
}

// Group 6a: Preemption — TID rollover under priority preemption.
func TestAdmissionPreemptionTidRollover(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	ts := int64(1 * mega)

	if !f.solveStart(ts, PrioSlow, 0, 0) { // admit low-prio TID=0
		t.Fatal("step1 should admit")
	}
	f.record(PrioSlow, 0, ts, 0)
	if f.ses.lastAdmittedTransferID != 0 || f.ses.lastAdmittedPriority != uint8(PrioSlow) {
		t.Fatalf("state mismatch after step1: tid=%d prio=%d", f.ses.lastAdmittedTransferID, f.ses.lastAdmittedPriority)
	}

	ts += mega / 10
	if !f.solveStart(ts, PrioImmediate, 1, 0) { // high-prio TID=1 preempts
		t.Fatal("step2 should admit")
	}
	f.record(PrioImmediate, 1, ts, 0)

	ts += mega / 10
	if !f.solveStart(ts, PrioImmediate, 2, 0) { // high-prio TID=2
		t.Fatal("step3 should admit")
	}
	f.record(PrioImmediate, 2, ts, 0)

	ts += mega / 10
	if !f.solveStart(ts, PrioImmediate, 0, 0) { // high-prio TID=0 (wraps to original low TID)
		t.Fatal("step4 should admit (fresh because 0!=2)")
	}
	f.record(PrioImmediate, 0, ts, 0)

	ts += mega / 10
	if !f.solveStart(ts, PrioSlow, 0, 0) { // late low-prio TID=0: prio differs -> fresh
		t.Fatal("step5 should admit (priority differs)")
	}
	f.freeSlots()
}

// Group 6b: Deep nesting — all 8 priority levels with the same TID.
func TestAdmissionPreemptionDeepNesting(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	ts := int64(1 * mega)

	for prio := 0; prio < PrioCount; prio++ {
		if !f.solveStart(ts, Prio(prio), 10, 0) {
			t.Fatalf("prio %d should admit", prio)
		}
		f.record(Prio(prio), 10, ts, 0)
		ts += mega / 10
	}
	if f.ses.lastAdmittedTransferID != 10 {
		t.Fatalf("tid=%d, want 10", f.ses.lastAdmittedTransferID)
	}
	if f.ses.lastAdmittedPriority != uint8(PrioOptional) {
		t.Fatalf("prio=%d, want %d", f.ses.lastAdmittedPriority, PrioOptional)
	}
	if !f.solveStart(ts, PrioExceptional, 10, 0) { // prio=0 != 7
		t.Fatal("return to prio 0 should admit")
	}
	f.freeSlots()
}

// Group 6c: Same TID returns on original priority after preemption.
func TestAdmissionPreemptionReturnToOriginalPriority(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	ts := int64(1 * mega)

	if !f.solveStart(ts, PrioNominal, 5, 0) {
		t.Fatal("original should admit")
	}
	f.record(PrioNominal, 5, ts, 0)

	ts += mega / 10
	if !f.solveStart(ts, PrioImmediate, 6, 0) { // preempt
		t.Fatal("preempt should admit")
	}
	f.record(PrioImmediate, 6, ts, 0)

	ts += mega / 10
	if !f.solveStart(ts, PrioNominal, 6, 0) { // TID matches but prio differs -> fresh
		t.Fatal("return on original priority should admit")
	}
	f.freeSlots()
}

// Group 7: record_admission — masking and overwrite behavior.
func TestAdmissionRecordAdmission(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)

	f.record(PrioHigh, 17, 5*mega, 0)
	if f.ses.lastAdmissionTs != 5*mega {
		t.Fatalf("ts=%d, want %d", f.ses.lastAdmissionTs, 5*mega)
	}
	if f.ses.lastAdmittedTransferID != 17 {
		t.Fatalf("tid=%d, want 17", f.ses.lastAdmittedTransferID)
	}
	if f.ses.lastAdmittedPriority != uint8(PrioHigh) {
		t.Fatalf("prio=%d, want %d", f.ses.lastAdmittedPriority, PrioHigh)
	}
	if f.ses.ifaceIndex != 0 {
		t.Fatalf("iface=%d, want 0", f.ses.ifaceIndex)
	}

	// Overwrite replaces all fields.
	f.record(PrioOptional, 31, 10*mega, 1)
	if f.ses.lastAdmissionTs != 10*mega || f.ses.lastAdmittedTransferID != 31 ||
		f.ses.lastAdmittedPriority != uint8(PrioOptional) || f.ses.ifaceIndex != 1 {
		t.Fatalf("overwrite failed: ts=%d tid=%d prio=%d iface=%d",
			f.ses.lastAdmissionTs, f.ses.lastAdmittedTransferID, f.ses.lastAdmittedPriority, f.ses.ifaceIndex)
	}

	// Masking: out-of-range values are truncated (tid & 0x1F, prio & 0x07, iface & 0x01).
	rxSessionRecordAdmission(&f.ses, Prio(7), 0xFF, 99*mega, 0xFF)
	if f.ses.lastAdmissionTs != 99*mega {
		t.Fatalf("ts=%d, want %d", f.ses.lastAdmissionTs, 99*mega)
	}
	if f.ses.lastAdmittedTransferID != 31 { // 0xFF & 0x1F
		t.Fatalf("tid=%d, want 31", f.ses.lastAdmittedTransferID)
	}
	if f.ses.lastAdmittedPriority != 7 { // max valid priority
		t.Fatalf("prio=%d, want 7", f.ses.lastAdmittedPriority)
	}
	if f.ses.ifaceIndex != 1 { // 0xFF & 0x01
		t.Fatalf("iface=%d, want 1", f.ses.ifaceIndex)
	}
	f.freeSlots()
}

// Group 8a: Normal TID progression 0->1->...->31->0 rollover on a single interface.
func TestAdmissionIntegrationTidProgression(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	ts := int64(1 * mega)

	if !f.solveStart(ts, PrioNominal, 0, 0) { // first frame on fresh session
		t.Fatal("first frame should admit")
	}
	f.record(PrioNominal, 0, ts, 0)

	for tid := byte(1); tid <= 31; tid++ {
		ts += mega / 10
		if !f.solveStart(ts, PrioNominal, tid, 0) {
			t.Fatalf("tid %d should admit", tid)
		}
		f.record(PrioNominal, tid, ts, 0)
	}
	ts += mega / 10
	if !f.solveStart(ts, PrioNominal, 0, 0) { // rollover 31->0
		t.Fatal("rollover should admit")
	}
	f.record(PrioNominal, 0, ts, 0)
	f.freeSlots()
}

// Group 8b: Duplicate rejection and timeout recovery.
func TestAdmissionIntegrationDuplicateRejection(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	ts0 := int64(1 * mega)

	if !f.solveStart(ts0, PrioNominal, 5, 0) {
		t.Fatal("admit TID=5")
	}
	f.record(PrioNominal, 5, ts0, 0)

	if f.solveStart(ts0, PrioNominal, 5, 0) { // immediate duplicate
		t.Fatal("immediate duplicate should reject")
	}
	if f.solveStart(ts0+mega, PrioNominal, 5, 0) { // still within timeout
		t.Fatal("within-timeout duplicate should reject")
	}
	if f.solveStart(ts0+(2*mega), PrioNominal, 5, 0) { // exact boundary, not stale
		t.Fatal("exact timeout boundary should reject")
	}
	if !f.solveStart(ts0+(2*mega)+1, PrioNominal, 5, 0) { // one tick past timeout
		t.Fatal("past-timeout should admit")
	}
	f.freeSlots()
}

// Group 8c: Interface failover with TID collision — the stale&&!fresh&&!affine corner case.
func TestAdmissionIntegrationIfaceFailoverTidCollision(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	ts0 := int64(1 * mega)

	if !f.solveStart(ts0, PrioHigh, 5, 0) {
		t.Fatal("admit TID=5 prio=high iface=0")
	}
	f.record(PrioHigh, 5, ts0, 0)

	// After timeout, iface=1 sends TID=5 prio=high: stale, !fresh, !affine -> 1/3 reject.
	if f.solveStart(ts0+(3*mega), PrioHigh, 5, 1) {
		t.Fatal("iface failover same TID/prio should reject")
	}
	// Recovery: iface=1 sends TID=6 prio=high: stale, fresh -> 2/3 admit.
	if !f.solveStart(ts0+(3*mega), PrioHigh, 6, 1) {
		t.Fatal("iface failover new TID should admit")
	}
	// Alternative recovery: reset then iface=1 sends TID=5 different priority: fresh -> 2/3 admit.
	f.record(PrioHigh, 5, ts0, 0)
	f.ses.ifaceIndex = 0
	if !f.solveStart(ts0+(3*mega), PrioLow, 5, 1) {
		t.Fatal("iface failover new priority should admit")
	}
	f.freeSlots()
}

// Group 8d: Zero timeout mode — accept distinct transfers, reject same-timestamp duplicates.
func TestAdmissionIntegrationZeroTimeout(t *testing.T) {
	f := admissionFixtureInit(t, 0) // zero timeout
	f.ses.ifaceIndex = 0
	ts0 := int64(1 * mega)

	if !f.solveStart(ts0, PrioNominal, 5, 0) {
		t.Fatal("admit TID=5")
	}
	f.record(PrioNominal, 5, ts0, 0)

	if !f.solveStart(ts0+1, PrioNominal, 5, 0) { // stale, affine -> admit (duplicates tolerated)
		t.Fatal("zero-timeout later duplicate should admit")
	}
	f.record(PrioNominal, 5, ts0+1, 0)

	if f.solveStart(ts0+1, PrioNominal, 5, 0) { // same timestamp: not stale, !fresh -> reject
		t.Fatal("zero-timeout same-timestamp duplicate should reject")
	}
	if f.solveStart(ts0+1, PrioNominal, 5, 1) { // different iface, same ts -> reject
		t.Fatal("zero-timeout different-iface same-ts should reject")
	}
	if !f.solveStart(ts0+2, PrioNominal, 6, 1) { // different iface, different TID, later ts -> 2/3 admit
		t.Fatal("zero-timeout different-iface different-TID should admit")
	}
	f.freeSlots()
}

// Group 8e: Documented limitation — duplicate-after-preemption (known false admission).
func TestAdmissionLimitationDuplicateAfterPreemption(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	ts := int64(1 * mega)

	if !f.solveStart(ts, PrioSlow, 1, 0) { // original low-prio TID=1
		t.Fatal("original should admit")
	}
	f.record(PrioSlow, 1, ts, 0)

	ts += 1
	if !f.solveStart(ts, PrioImmediate, 2, 0) { // high-prio preempts
		t.Fatal("preempt should admit")
	}
	f.record(PrioImmediate, 2, ts, 0)

	ts += 1
	// Duplicate of original low-prio TID=1: design accepts this as fresh (priority changed). Known limitation.
	if !f.solveStart(ts, PrioSlow, 1, 0) {
		t.Fatal("duplicate-after-preemption should be (falsely) admitted per design")
	}
	f.freeSlots()
}

// Group 9a: Continuation frame with no active slot is rejected.
func TestAdmissionContinuationNoSlotRejected(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	f.ses.lastAdmissionTs = 1 * mega

	if f.solveCont(2*mega, PrioNominal, true, 5, 0) {
		t.Fatal("no slot at nominal should reject")
	}
	if f.solveCont(2*mega, PrioExceptional, false, 0, 0) {
		t.Fatal("no slot at exceptional should reject")
	}
	if f.solveCont(2*mega, PrioOptional, true, 31, 1) {
		t.Fatal("no slot at optional should reject")
	}

	f.ses.slots[PrioHigh] = rxSlotNew(&f.sub, 1*mega, 5, 0)
	if f.solveCont(2*mega, PrioNominal, true, 5, 0) { // no slot at nominal
		t.Fatal("continuation at absent priority should reject")
	}
	if !f.solveCont(2*mega, PrioHigh, true, 5, 0) { // slot at high matches
		t.Fatal("continuation at present priority should admit")
	}
	f.freeSlots()
}

// Group 9b: Slot exists with TID=5; continuation with TID=6 rejected.
func TestAdmissionContinuationWrongTidRejected(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	f.ses.lastAdmissionTs = 1 * mega

	f.ses.slots[PrioNominal] = rxSlotNew(&f.sub, 1*mega, 5, 0)
	if f.ses.slots[PrioNominal] == nil {
		t.Fatal("rxSlotNew returned nil")
	}
	if !f.solveCont(2*mega, PrioNominal, true, 5, 0) { // correct TID
		t.Fatal("correct TID should admit")
	}
	if f.solveCont(2*mega, PrioNominal, true, 6, 0) { // wrong TID
		t.Fatal("wrong TID should reject")
	}
	if f.solveCont(2*mega, PrioNominal, true, 0, 0) { // wrong TID
		t.Fatal("TID 0 should reject")
	}
	if f.solveCont(2*mega, PrioNominal, true, 31, 0) { // wrong TID
		t.Fatal("TID 31 should reject")
	}
	f.freeSlots()
}

// Group 9c: Slot on iface 0; continuation on iface 1 rejected.
func TestAdmissionContinuationWrongIfaceRejected(t *testing.T) {
	f := admissionFixtureInit(t, 2*mega)
	f.ses.ifaceIndex = 0
	f.ses.lastAdmissionTs = 1 * mega

	f.ses.slots[PrioNominal] = rxSlotNew(&f.sub, 1*mega, 10, 0)
	if f.ses.slots[PrioNominal] == nil {
		t.Fatal("rxSlotNew returned nil")
	}
	if !f.solveCont(2*mega, PrioNominal, true, 10, 0) { // correct iface
		t.Fatal("correct iface should admit")
	}
	if f.solveCont(2*mega, PrioNominal, true, 10, 1) { // wrong iface
		t.Fatal("wrong iface should reject")
	}
	f.freeSlots()
}

// Group 9d: With zero timeout, duplicate TIDs accepted as long as timestamps advance.
func TestAdmissionZeroTimeoutDuplicatesAccepted(t *testing.T) {
	f := admissionFixtureInit(t, 0) // zero timeout
	f.ses.ifaceIndex = 0
	ts0 := int64(1 * mega)

	if !f.solveStart(ts0, PrioNominal, 5, 0) { // fresh via stale(BIG_BANG)
		t.Fatal("admit TID=5")
	}
	f.record(PrioNominal, 5, ts0, 0)

	if !f.solveStart(ts0+1, PrioNominal, 5, 0) { // stale, affine -> admit
		t.Fatal("one tick later should admit")
	}
	f.record(PrioNominal, 5, ts0+1, 0)
	if !f.solveStart(ts0+2, PrioNominal, 5, 0) { // stale again
		t.Fatal("another tick should admit")
	}
	f.record(PrioNominal, 5, ts0+2, 0)
	if f.solveStart(ts0+2, PrioNominal, 5, 0) { // same timestamp -> reject
		t.Fatal("same-timestamp duplicate should reject")
	}
	f.freeSlots()
}
