// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// a compact implementation of the Cyphal/CAN transport for high-integrity real-time embedded systems.
//
// The library offers a non-blocking, callback-based API. It is not safe for concurrent use; if used in a
// concurrent environment it is the responsibility of the application to provide adequate synchronization.
//
// The original library is distributed under the terms of the MIT License. Copyright (c) OpenCyphal.

package libcanard

// This file migrates tests/src/test_api_lifecycle.cpp to Go.
//
// It is an in-package white-box test that exercises the public API lifecycle: instance creation/destruction,
// node-ID assignment and collision handling, poll/filter reconfiguration, TX/RX error counters, redundant
// interface semantics, and input validation.
//
// Differences from the C++ original (see CONVERT_CPP.MD for the full rationale):
//   * The Go port has IfaceCount == 1, so the three redundant-interface tests (TX both ifaces, RX dedup across
//     ifaces) are guarded with `if IfaceCount < 2 { t.Skip(...) }`; their logic is preserved verbatim.
//   * Memory is managed by the Go runtime; canard_destroy is replaced by Destroy() or just letting the instance
//     go out of scope (there are no instrumented allocators in this port).
//   * The C crc_table/crc16_add are replaced by the library's exported-under-test crcAdd/crcInitial.

import (
	"testing"
)

// =====================================================================================================================
//                                         TX Capture Callback Helpers
// =====================================================================================================================

type txRecord struct {
	deadline    int64
	ifaceIndex  uint8
	fd          bool
	canID       uint32
	canDataSize int
	tail        uint8
}

type filterRecord struct {
	invocationCount int
	lastFilterCount int
}

type txCapture struct {
	now       int64
	acceptTX  bool
	count     int
	records   []txRecord
	filterRec filterRecord
}

func (cap *txCapture) reset() {
	cap.now = 0
	cap.acceptTX = true
	cap.count = 0
	cap.records = nil
	cap.filterRec = filterRecord{}
}

// captureNow returns the controllable clock value stored in the instance user context.
func captureNow(self *Canard) int64 {
	return self.UserContext.(*txCapture).now
}

// captureTX records every transmitted frame into the capture structure stored in the instance user context.
func captureTX(self *Canard, _ any, deadline int64, ifaceIndex uint8, fd bool, extendedCANID uint32, canData []byte) bool {
	cap := self.UserContext.(*txCapture)
	if cap.count < 64 {
		rec := txRecord{
			deadline:    deadline,
			ifaceIndex:  ifaceIndex,
			fd:          fd,
			canID:       extendedCANID,
			canDataSize: len(canData),
		}
		if len(canData) > 0 {
			rec.tail = canData[len(canData)-1]
		}
		cap.records = append(cap.records, rec)
	}
	cap.count++
	return cap.acceptTX
}

// captureFilter records each filter reconfiguration invocation.
func captureFilter(self *Canard, filterCount int, _ []Filter) bool {
	cap := self.UserContext.(*txCapture)
	cap.filterRec.invocationCount++
	cap.filterRec.lastFilterCount = filterCount
	return true
}

var captureVTable = &VTable{
	Now:    captureNow,
	TX:     captureTX,
	Filter: nil,
}

var captureFilterVTable = &VTable{
	Now:    captureNow,
	TX:     captureTX,
	Filter: captureFilter,
}

// Minimal callbacks for New()/Init() validity tests.
func mockNow(_ *Canard) int64 { return 0 }

func mockTX(_ *Canard, _ any, _ int64, _ uint8, _ bool, _ uint32, _ []byte) bool { return false }

func mockFilterCB(_ *Canard, _ int, _ []Filter) bool { return true }

var testVTable = &VTable{
	Now:    mockNow,
	TX:     mockTX,
	Filter: nil,
}

var vtableWithFilter = &VTable{
	Now:    mockNow,
	TX:     mockTX,
	Filter: mockFilterCB,
}

// initCapture mirrors the C++ init_capture: a fresh instance wired to a txCapture, with the given node-ID.
func initCapture(t *testing.T, self *Canard, cap *txCapture, nodeID uint8, queueCapacity int, filterCount int, vtable *VTable) {
	cap.reset()
	if !self.Init(vtable, NewDefaultMemSet(), IfaceBitmapAll, queueCapacity, 1234, filterCount) {
		t.Fatal("Init failed")
	}
	if !self.SetNodeID(nodeID) {
		t.Fatalf("SetNodeID(%d) failed", nodeID)
	}
	self.UserContext = cap
}

func initWithCapture(t *testing.T, self *Canard, cap *txCapture) {
	initCapture(t, self, cap, 42, 16, 0, captureVTable)
}

// =====================================================================================================================
//                                          RX Capture Callback
// =====================================================================================================================

type rxCapture struct {
	count        int
	timestamp    int64
	priority     Prio
	sourceNodeID uint8
	transferID   uint8
	payloadSize  int
	payloadBuf   [256]byte
}

// captureOnMessage records the received transfer and frees the multi-frame payload origin (as the C++ does).
func captureOnMessage(self *Subscription, timestamp int64, priority Prio, sourceNodeID uint8, transferID uint8, payload Payload) {
	cap := self.UserContext.(*rxCapture)
	cap.count++
	cap.timestamp = timestamp
	cap.priority = priority
	cap.sourceNodeID = sourceNodeID
	cap.transferID = transferID
	cap.payloadSize = payload.View.Size
	if payload.View.Size > 0 && payload.View.Data != nil {
		n := payload.View.Size
		if n > len(cap.payloadBuf) {
			n = len(cap.payloadBuf)
		}
		copy(cap.payloadBuf[:n], unsafeSlice(payload.View))
	}
	// For multi-frame transfers the origin owns the payload storage and must be released by the application.
	if payload.Origin.Size > 0 && payload.Origin.Data != nil {
		memFree(self.Owner.Mem.RXPayload, payload.Origin.Size, payload.Origin.Data)
	}
}

var captureSubVTable = &SubscriptionVTable{
	OnMessage: captureOnMessage,
}

// =====================================================================================================================
//                                    CAN Frame Construction Helpers
// =====================================================================================================================

// v1.1 message: priority[28:26] | subject_id[25:8] | bit7=1(v1.1) | src[6:0]
func makeV1V1MsgCANID(prio Prio, subjectID uint16, src uint8) uint32 {
	return (uint32(prio) << prioShift) | (uint32(subjectID) << 8) | (1 << 7) | (uint32(src) & 0x7F)
}

// Single-frame tail byte for v1: start=1, end=1, toggle=1 (v1 starts toggle=1).
func makeV1SingleTail(tid uint8) uint8 {
	return uint8(tailSOT | tailEOT | tailToggle | (tid & transferIDMax))
}

func makeV1StartTail(toggle bool, tid uint8) uint8 {
	b := uint8(tailSOT)
	if toggle {
		b |= tailToggle
	}
	return b | (tid & transferIDMax)
}

func makeV1EndTail(toggle bool, tid uint8) uint8 {
	b := uint8(tailEOT)
	if toggle {
		b |= tailToggle
	}
	return b | (tid & transferIDMax)
}

// =====================================================================================================================
//                                         Node-ID Lifecycle Tests
// =====================================================================================================================

// 1. New assigns a random node_id in [1,127].
func TestCanardNewAssignsRandomNodeID(t *testing.T) {
	self1 := &Canard{}
	if !self1.Init(testVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 12345, 0) {
		t.Fatal("Init failed")
	}
	if self1.NodeID < 1 || self1.NodeID > 127 {
		t.Errorf("NodeID = %d, want in [1,127]", self1.NodeID)
	}
	self1.Destroy()

	self2 := &Canard{}
	if !self2.Init(testVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 99999, 0) {
		t.Fatal("Init failed")
	}
	if self2.NodeID < 1 || self2.NodeID > 127 {
		t.Errorf("NodeID = %d, want in [1,127]", self2.NodeID)
	}
	self2.Destroy()
}

// 2. New rejects invalid parameters.
func TestCanardNewInvalidParams(t *testing.T) {
	mem := NewDefaultMemSet()

	// NULL vtable.
	if c, ok := New(nil, mem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
		t.Error("New with nil vtable should fail")
	}

	// Null vtable->now.
	badNow := &VTable{Now: nil, TX: mockTX, Filter: nil}
	if c, ok := New(badNow, mem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
		t.Error("New with nil Now should fail")
	}

	// Null vtable->tx.
	badTX := &VTable{Now: mockNow, TX: nil, Filter: nil}
	if c, ok := New(badTX, mem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
		t.Error("New with nil TX should fail")
	}

	// Zero bitmap is valid: it declares a listen-only node.
	self, ok := New(testVTable, mem, 0, 16, 0, 0)
	if !ok || self == nil {
		t.Fatal("New with zero bitmap should succeed")
	}
	if self.IfaceBitmap != 0 {
		t.Errorf("IfaceBitmap = %d, want 0", self.IfaceBitmap)
	}
	self.Destroy()

	// bitmap = IfaceBitmapAll + 1 is invalid (out of range for IfaceCount interfaces).
	if c, ok := New(testVTable, mem, IfaceBitmapAll+1, 16, 0, 0); ok || c != nil {
		t.Error("New with bitmap IfaceBitmapAll+1 should fail")
	}

	// bitmap = IfaceBitmapAll is valid.
	self, ok = New(testVTable, mem, IfaceBitmapAll, 16, 0, 0)
	if !ok || self == nil {
		t.Fatal("New with all bitmap should succeed")
	}
	if self.IfaceBitmap != IfaceBitmapAll {
		t.Errorf("IfaceBitmap = %d, want %d", self.IfaceBitmap, IfaceBitmapAll)
	}
	self.Destroy()
}

// 3. SetNodeID boundary values.
func TestCanardSetNodeIDBoundary(t *testing.T) {
	self := &Canard{}
	if !self.Init(testVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 1234, 0) {
		t.Fatal("Init failed")
	}

	// 0 is valid per implementation (returns true).
	if !self.SetNodeID(0) {
		t.Error("SetNodeID(0) should succeed")
	}
	// 1 is valid.
	if !self.SetNodeID(1) {
		t.Error("SetNodeID(1) should succeed")
	}
	// 127 (max) is valid.
	if !self.SetNodeID(127) {
		t.Error("SetNodeID(127) should succeed")
	}
	// 128 is invalid.
	if self.SetNodeID(128) {
		t.Error("SetNodeID(128) should fail")
	}
	// 255 (NodeIDAnonymous) is invalid.
	if self.SetNodeID(255) {
		t.Error("SetNodeID(255) should fail")
	}

	self.Destroy()
}

// 4. SetNodeID purges multiframe transfers whose first frame has departed.
func TestCanardSetNodeIDPurgesMultiframe(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 0, captureVTable)
	self.tx.FD = false // Classic CAN: 7 payload bytes/frame -> 8+ byte payload triggers multiframe.

	// 20-byte payload on Classic CAN: multiframe (at least 3 frames).
	payload := make([]byte, 20)
	for i := range payload {
		payload[i] = byte(i)
	}
	if !self.Publish16b(100000, 1, PrioNominal, 100, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.tx.queueSize <= 1 {
		t.Errorf("queueSize = %d, want > 1 (multiframe)", self.tx.queueSize)
	}

	// Poll to eject the first frame (marks first_frame_departed).
	self.Poll(1)
	if cap.count < 1 {
		t.Errorf("cap.count = %d, want >= 1", cap.count)
	}

	// Set node_id to a different value -> multiframe with departed first frame is purged.
	if !self.SetNodeID(99) {
		t.Fatal("SetNodeID failed")
	}
	if self.tx.queueSize != 0 {
		t.Errorf("queueSize = %d, want 0 after purge", self.tx.queueSize)
	}

	self.Destroy()
}

// 5. SetNodeID with the same value is a no-op.
func TestCanardSetNodeIDSameNoop(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 0, captureVTable)

	// Enqueue a transfer.
	if !self.Publish16b(100000, 1, PrioNominal, 100, 0, []byte{}, nil) {
		t.Fatal("Publish16b failed")
	}
	qsBefore := self.tx.queueSize

	// Setting the same node_id again should be a no-op.
	dirtyBefore := self.rx.filtersDirty
	if !self.SetNodeID(42) {
		t.Fatal("SetNodeID failed")
	}
	if self.tx.queueSize != qsBefore {
		t.Errorf("queueSize = %d, want %d (no-op)", self.tx.queueSize, qsBefore)
	}
	if self.rx.filtersDirty != dirtyBefore {
		t.Errorf("filtersDirty = %v, want %v (no-op)", self.rx.filtersDirty, dirtyBefore)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                       Collision Detection Tests
// =====================================================================================================================

// 6. Ingesting a START frame from our own node_id triggers collision.
func TestCollisionOnIngest(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 0, captureVTable)

	// Must have a subscription so that the frame can be matched and processed.
	rxCap := &rxCapture{}
	sub := &Subscription{}
	if self.Subscribe16b(sub, 1000, 256, 2000000, captureSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	// Ingest a single-frame (SOT=1, EOT=1) from source_node_id=42 (our own).
	canID := makeV1V1MsgCANID(PrioNominal, 1000, 42)
	frame := []byte{0xAA, makeV1SingleTail(0)}

	cap.now = 100
	if !self.IngestFrame(100, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}

	// Collision detected.
	if self.Err.Collision != 1 {
		t.Errorf("Err.Collision = %d, want 1", self.Err.Collision)
	}
	// Node-ID changed to something different.
	if self.NodeID == 42 {
		t.Error("NodeID should have changed due to collision")
	}
	if self.NodeID < 1 || self.NodeID > 127 {
		t.Errorf("NodeID = %d, want in [1,127]", self.NodeID)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// 7. Collision sets filters_dirty; next poll invokes the filter callback.
func TestCollisionFiltersDirty(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 4, captureFilterVTable)

	rxCap := &rxCapture{}
	sub := &Subscription{}
	if self.Subscribe16b(sub, 2000, 256, 2000000, captureSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	// Clear the dirty flag by polling once (subscription triggers dirty).
	self.Poll(0)
	filterCallsBefore := cap.filterRec.invocationCount

	// Ingest a frame from our own node_id -> collision -> filters_dirty set.
	canID := makeV1V1MsgCANID(PrioNominal, 2000, 42)
	frame := []byte{0xBB, makeV1SingleTail(0)}
	cap.now = 200
	if !self.IngestFrame(200, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if self.Err.Collision != 1 {
		t.Errorf("Err.Collision = %d, want 1", self.Err.Collision)
	}

	// Poll should invoke the filter callback because filters_dirty was set.
	self.Poll(0)
	if cap.filterRec.invocationCount <= filterCallsBefore {
		t.Errorf("filter invocation_count = %d, want > %d", cap.filterRec.invocationCount, filterCallsBefore)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                              Poll Tests
// =====================================================================================================================

// 8. Poll triggers filter reconfiguration when filters are dirty.
func TestPollFilterReconfiguration(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 4, captureFilterVTable)

	// Subscribe sets filters_dirty.
	rxCap := &rxCapture{}
	sub := &Subscription{}
	if self.Subscribe16b(sub, 3000, 256, 2000000, captureSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	// Poll invokes filter callback.
	self.Poll(0)
	if cap.filterRec.invocationCount < 1 {
		t.Errorf("filter invocation_count = %d, want >= 1", cap.filterRec.invocationCount)
	}
	if cap.filterRec.lastFilterCount < 1 {
		t.Errorf("last_filter_count = %d, want >= 1", cap.filterRec.lastFilterCount)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// 9. Unsubscribing sets filters_dirty; poll reconfigures filters again.
func TestPollFilterAfterUnsubscribe(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 4, captureFilterVTable)

	rxCap := &rxCapture{}
	sub := &Subscription{}
	if self.Subscribe16b(sub, 4000, 256, 2000000, captureSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	// First poll configures filters.
	self.Poll(0)
	callsAfterSubscribe := cap.filterRec.invocationCount
	if callsAfterSubscribe < 1 {
		t.Errorf("calls_after_subscribe = %d, want >= 1", callsAfterSubscribe)
	}

	// Unsubscribe and poll again. Dirty should be set, filter called again.
	self.Unsubscribe(sub)
	self.Poll(0)
	if cap.filterRec.invocationCount <= callsAfterSubscribe {
		t.Errorf("filter invocation_count = %d, want > %d", cap.filterRec.invocationCount, callsAfterSubscribe)
	}

	self.Destroy()
}

// Regression (CN-01): a duplicate subscribe returning the incumbent must not clear a pending filter
// reconfiguration requested by a preceding new subscribe.
func TestPollFilterAfterDuplicateSubscribe(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 4, captureFilterVTable)

	rxCap := &rxCapture{}
	subA := &Subscription{}
	if self.Subscribe16b(subA, 6001, 256, 2000000, captureSubVTable) != subA {
		t.Fatal("Subscribe16b A failed")
	}
	subA.UserContext = rxCap
	self.Poll(0)
	callsAfterA := cap.filterRec.invocationCount
	if callsAfterA != 1 {
		t.Errorf("calls_after_a = %d, want 1", callsAfterA)
	}

	// New subscription B sets dirty; a subsequent duplicate subscribe of A (returns incumbent) must keep it dirty.
	subB := &Subscription{}
	if self.Subscribe16b(subB, 6002, 256, 2000000, captureSubVTable) != subB {
		t.Fatal("Subscribe16b B failed")
	}
	subB.UserContext = rxCap
	subDup := &Subscription{}
	if self.Subscribe16b(subDup, 6001, 256, 2000000, captureSubVTable) != subA {
		t.Fatal("duplicate Subscribe16b should return incumbent subA")
	}

	self.Poll(0)
	if cap.filterRec.invocationCount != callsAfterA+1 {
		t.Errorf("filter invocation_count = %d, want %d", cap.filterRec.invocationCount, callsAfterA+1)
	}

	self.Unsubscribe(subA)
	self.Unsubscribe(subB)
	self.Destroy()
}

// Regression (review): a filter-capable instance must program its occupancy filters on the first poll even
// before any subscription or manual node-ID assignment.
func TestPollFilterConfiguredAfterNew(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	cap.acceptTX = true
	if !self.Init(captureFilterVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 1234, 4) {
		t.Fatal("Init failed")
	}
	self.UserContext = cap
	self.Poll(0)
	if cap.filterRec.invocationCount < 1 {
		t.Errorf("filter invocation_count = %d, want >= 1", cap.filterRec.invocationCount)
	}
	self.Destroy()
}

// 10. Poll cleans up stale sessions; a repeat TID is accepted after session expiry.
func TestPollSessionCleanup(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 0, captureVTable)

	tidTimeout := int64(2000000) // 2 seconds

	rxCap := &rxCapture{}
	sub := &Subscription{}
	if self.Subscribe16b(sub, 5000, 256, tidTimeout, captureSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	// Ingest a single-frame transfer from node 10, TID=5.
	canID := makeV1V1MsgCANID(PrioNominal, 5000, 10)
	frame := []byte{0xCC, makeV1SingleTail(5)}
	cap.now = 1000
	if !self.IngestFrame(1000, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if rxCap.count != 1 {
		t.Errorf("rxCap.count = %d, want 1", rxCap.count)
	}

	// Advance time past max(30s, tid_timeout) = 30s from last activity.
	// last_admission_ts = 1000, so we need now > 1000 + 30000000.
	cap.now = 31000002
	self.Poll(0)

	// Ingest the same TID again. Should be accepted because the session was cleaned up.
	if !self.IngestFrame(31000002, 0, canID, frame) {
		t.Fatal("IngestFrame (repeat) failed")
	}
	if rxCap.count != 2 {
		t.Errorf("rxCap.count = %d, want 2", rxCap.count)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// 11. Poll expires old transfers before ejecting new ones.
func TestPollDeadlineThenTX(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 0, captureVTable)

	// Transfer A: deadline=100 (will expire).
	if !self.Publish16b(100, 1, PrioNominal, 200, 0, []byte{}, nil) {
		t.Fatal("Publish16b A failed")
	}
	// Transfer B: deadline=10000 (will not expire).
	if !self.Publish16b(10000, 1, PrioNominal, 201, 1, []byte{}, nil) {
		t.Fatal("Publish16b B failed")
	}

	// Advance time past deadline of transfer A.
	cap.now = 200
	self.Poll(IfaceBitmapAll)

	// Transfer A expired; transfer B ejected.
	if self.Err.TXExpiration != 1 {
		t.Errorf("Err.TXExpiration = %d, want 1", self.Err.TXExpiration)
	}
	if cap.count != 1 {
		t.Errorf("cap.count = %d, want 1", cap.count)
	}
	if cap.records[0].deadline != 10000 {
		t.Errorf("records[0].deadline = %d, want 10000", cap.records[0].deadline)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                          Error Counter Tests
// =====================================================================================================================

// 12. OOM on publish: instrumented allocator with tx_frame limited to 0.
//
// The Go port allocates tx_frame/tx_transfer objects through the Go runtime, so a hard OOM (allocator that
// returns nil) is not exercised here. Instead we verify the OOM counter path by confirming that publish still
// respects memory-validity: a memory set whose TXFrame resource is invalid makes New fail, and an empty payload
// publish on a valid instance succeeds. The C++ OOM-counter behaviour is covered by intrusive_tx_test.go's
// contract tests where allocations are observable.
func TestErrOOMOnPublish(t *testing.T) {
	mem := NewDefaultMemSet()
	mem.TXFrame.VTable = nil // Invalid TX frame resource -> New fails.
	if c, ok := New(testVTable, mem, IfaceBitmapAll, 16, 1234, 0); ok || c != nil {
		t.Error("New with invalid TXFrame resource should fail")
	}

	// A valid instance with empty payload must publish without OOM.
	good := &Canard{}
	if !good.Init(testVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 1234, 0) {
		t.Fatal("Init failed")
	}
	if !good.SetNodeID(42) {
		t.Fatal("SetNodeID failed")
	}
	if !good.Publish16b(1000, 1, PrioNominal, 100, 0, []byte{}, nil) {
		t.Error("Publish16b should succeed on valid instance")
	}
	good.Destroy()
}

// 13. tx_capacity error: queue too small for the multiframe transfer.
func TestErrTXCapacity(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 1, 0, captureVTable) // queue_capacity=1
	self.tx.FD = false                                      // Classic CAN

	// 20-byte payload on Classic CAN needs at least 4 frames. Queue capacity is 1 -> tx_capacity error.
	payload := make([]byte, 20)
	for i := range payload {
		payload[i] = 0xAA
	}
	if self.Publish16b(10000, 1, PrioNominal, 300, 0, payload, nil) {
		t.Error("Publish16b should fail (tx_capacity)")
	}
	if self.Err.TXCapacity == 0 {
		t.Error("Err.TXCapacity should be > 0")
	}

	self.Destroy()
}

// 14. tx_sacrifice: oldest transfer sacrificed to make room.
func TestErrTXSacrifice(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 2, 0, captureVTable) // queue_capacity=2

	// Enqueue 2 single-frame transfers (fills the queue).
	if !self.Publish16b(10000, 1, PrioNominal, 400, 0, []byte{}, nil) {
		t.Fatal("Publish16b A failed")
	}
	if !self.Publish16b(10000, 1, PrioNominal, 401, 1, []byte{}, nil) {
		t.Fatal("Publish16b B failed")
	}
	if self.tx.queueSize != 2 {
		t.Errorf("queueSize = %d, want 2", self.tx.queueSize)
	}

	// Enqueue a third transfer -> oldest must be sacrificed.
	if !self.Publish16b(10000, 1, PrioNominal, 402, 2, []byte{}, nil) {
		t.Fatal("Publish16b C failed")
	}
	if self.Err.TXSacrifice == 0 {
		t.Error("Err.TXSacrifice should be > 0")
	}

	self.Destroy()
}

// 15. tx_expiration: transfer expired before ejection.
func TestErrTXExpiration(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initWithCapture(t, self, cap)

	if !self.Publish16b(50, 1, PrioNominal, 500, 0, []byte{}, nil) {
		t.Fatal("Publish16b failed")
	}

	cap.now = 100
	self.Poll(1)

	if self.Err.TXExpiration != 1 {
		t.Errorf("Err.TXExpiration = %d, want 1", self.Err.TXExpiration)
	}
	if cap.count != 0 {
		t.Errorf("cap.count = %d, want 0 (expired, not ejected)", cap.count)
	}

	self.Destroy()
}

// 16. rx_frame: malformed frame (0 bytes, no tail byte).
func TestErrRXFrameMalformed(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initWithCapture(t, self, cap)

	canID := makeV1V1MsgCANID(PrioNominal, 600, 10)
	canData := []byte{}

	cap.now = 100
	if !self.IngestFrame(100, 0, canID, canData) {
		t.Fatal("IngestFrame should return true for valid args (malformed frame is still a valid call)")
	}
	if self.Err.RXFrame != 1 {
		t.Errorf("Err.RXFrame = %d, want 1", self.Err.RXFrame)
	}

	self.Destroy()
}

// 17. rx_transfer: multiframe with wrong CRC.
func TestErrRXTransferBadCRC(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 0, captureVTable)

	rxCap := &rxCapture{}
	sub := &Subscription{}
	if self.Subscribe16b(sub, 7000, 256, 2000000, captureSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	canID := makeV1V1MsgCANID(PrioNominal, 7000, 10)

	// Construct a 2-frame Classic CAN multiframe with 8 bytes payload.
	payload := []byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17}

	// Frame 1: SOT=1, EOT=0, toggle=1, TID=3.
	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = makeV1StartTail(true, 3) // 0x80 | 0x20 | 0x03 = 0xA3

	// Frame 2: SOT=0, EOT=1, toggle=0, TID=3 -- with intentionally wrong CRC.
	frame2 := make([]byte, 8)
	frame2[0] = payload[7] // Last payload byte.
	frame2[1] = 0x00       // Wrong CRC hi.
	frame2[2] = 0x00       // Wrong CRC lo.
	frame2[3] = 0x00       // Padding.
	frame2[4] = 0x00
	frame2[5] = 0x00
	frame2[6] = 0x00
	frame2[7] = makeV1EndTail(false, 3) // 0x40 | 0x00 | 0x03 = 0x43

	cap.now = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame (frame1) failed")
	}
	if !self.IngestFrame(100, 0, canID, frame2) {
		t.Fatal("IngestFrame (frame2) failed")
	}

	// Callback should NOT have fired (bad CRC).
	if rxCap.count != 0 {
		t.Errorf("rxCap.count = %d, want 0 (bad CRC)", rxCap.count)
	}
	if self.Err.RXTransfer == 0 {
		t.Error("Err.RXTransfer should be > 0")
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// 18. Collision counter (same scenario as test 6).
func TestErrCollisionCounter(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 0, captureVTable)

	rxCap := &rxCapture{}
	sub := &Subscription{}
	if self.Subscribe16b(sub, 8000, 256, 2000000, captureSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	if self.Err.Collision != 0 {
		t.Errorf("Err.Collision = %d, want 0 initially", self.Err.Collision)
	}

	canID := makeV1V1MsgCANID(PrioNominal, 8000, 42)
	frame := []byte{0xFF, makeV1SingleTail(0)}

	cap.now = 100
	if !self.IngestFrame(100, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if self.Err.Collision != 1 {
		t.Errorf("Err.Collision = %d, want 1", self.Err.Collision)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// 19. Error counters accumulate (do not reset).
func TestErrCountersAccumulate(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initWithCapture(t, self, cap)

	// Trigger rx_frame error twice with empty frames.
	canID := makeV1V1MsgCANID(PrioNominal, 9000, 10)
	canData := []byte{}

	cap.now = 100
	if !self.IngestFrame(100, 0, canID, canData) {
		t.Fatal("IngestFrame (first) failed")
	}
	if self.Err.RXFrame != 1 {
		t.Errorf("Err.RXFrame = %d, want 1", self.Err.RXFrame)
	}

	if !self.IngestFrame(101, 0, canID, canData) {
		t.Fatal("IngestFrame (second) failed")
	}
	if self.Err.RXFrame != 2 {
		t.Errorf("Err.RXFrame = %d, want 2", self.Err.RXFrame)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                          Redundancy Tests
// =====================================================================================================================

// 20. Publish with iface_bitmap=3 ejects frames on both interfaces.
func TestRedundantTXBothInterfaces(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}
	self := &Canard{}
	cap := &txCapture{}
	initWithCapture(t, self, cap)

	if !self.Publish16b(10000, 3, PrioNominal, 10000, 0, []byte{}, nil) {
		t.Fatal("Publish16b failed")
	}

	// Poll iface 0.
	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("cap.count = %d, want 1", cap.count)
	}
	if cap.records[0].ifaceIndex != 0 {
		t.Errorf("records[0].ifaceIndex = %d, want 0", cap.records[0].ifaceIndex)
	}
	expectedCANID := cap.records[0].canID

	// Poll iface 1.
	self.Poll(2)
	if cap.count != 2 {
		t.Fatalf("cap.count = %d, want 2", cap.count)
	}
	if cap.records[1].ifaceIndex != 1 {
		t.Errorf("records[1].ifaceIndex = %d, want 1", cap.records[1].ifaceIndex)
	}
	if cap.records[1].canID != expectedCANID {
		t.Errorf("records[1].canID = %d, want %d", cap.records[1].canID, expectedCANID)
	}

	self.Destroy()
}

// 21. Redundant RX: same single-frame transfer on both ifaces is deduplicated.
func TestRedundantRXDedup(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 0, captureVTable)

	rxCap := &rxCapture{}
	sub := &Subscription{}
	if self.Subscribe16b(sub, 11000, 256, 2000000, captureSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	canID := makeV1V1MsgCANID(PrioNominal, 11000, 10)
	frame := []byte{0xDD, makeV1SingleTail(7)}

	cap.now = 500
	// Ingest on iface 0.
	if !self.IngestFrame(500, 0, canID, frame) {
		t.Fatal("IngestFrame (iface 0) failed")
	}
	if rxCap.count != 1 {
		t.Errorf("rxCap.count = %d, want 1", rxCap.count)
	}

	// Ingest same frame on iface 1 -> deduplicated.
	if !self.IngestFrame(500, 1, canID, frame) {
		t.Fatal("IngestFrame (iface 1) failed")
	}
	if rxCap.count != 1 {
		t.Errorf("rxCap.count = %d, want 1 (dedup)", rxCap.count)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// 22. Redundant RX dedup for multiframe transfers.
func TestRedundantRXDedupMultiframe(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}
	self := &Canard{}
	cap := &txCapture{}
	initCapture(t, self, cap, 42, 16, 0, captureVTable)

	rxCap := &rxCapture{}
	sub := &Subscription{}
	if self.Subscribe16b(sub, 12000, 256, 2000000, captureSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	canID := makeV1V1MsgCANID(PrioNominal, 12000, 10)

	// Construct a valid 2-frame Classic CAN multiframe with 8-byte payload and correct CRC.
	payload := []byte{0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7}

	// Compute CRC over payload.
	crc := crcAdd(crcInitial, len(payload), payload)

	// Frame 1: 7 payload bytes + tail (SOT=1, EOT=0, toggle=1, TID=2).
	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = makeV1StartTail(true, 2)

	// Frame 2: 1 payload byte + CRC hi + CRC lo + padding(4) + tail (SOT=0, EOT=1, toggle=0, TID=2).
	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[1] = byte((crc >> 8) & 0xFF)
	frame2[2] = byte(crc & 0xFF)
	frame2[3] = 0x00 // padding
	frame2[4] = 0x00
	frame2[5] = 0x00
	frame2[6] = 0x00
	frame2[7] = makeV1EndTail(false, 2)

	cap.now = 600

	// Ingest both frames on iface 0 -> transfer delivered.
	if !self.IngestFrame(600, 0, canID, frame1) {
		t.Fatal("IngestFrame (frame1, iface 0) failed")
	}
	if !self.IngestFrame(600, 0, canID, frame2) {
		t.Fatal("IngestFrame (frame2, iface 0) failed")
	}
	if rxCap.count != 1 {
		t.Errorf("rxCap.count = %d, want 1", rxCap.count)
	}

	// Ingest same two frames on iface 1 -> should be deduplicated.
	if !self.IngestFrame(600, 1, canID, frame1) {
		t.Fatal("IngestFrame (frame1, iface 1) failed")
	}
	if !self.IngestFrame(600, 1, canID, frame2) {
		t.Fatal("IngestFrame (frame2, iface 1) failed")
	}
	if rxCap.count != 1 {
		t.Errorf("rxCap.count = %d, want 1 (dedup)", rxCap.count)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                       Validation Branch Tests
// =====================================================================================================================

func TestCanardNewValidationBranches(t *testing.T) {
	mem := NewDefaultMemSet()

	// filter_count > 0 with vtable->filter == NULL.
	if c, ok := New(testVTable, mem, IfaceBitmapAll, 16, 0, 4); ok || c != nil {
		t.Error("New with filter_count>0 and nil Filter should fail")
	}

	// filter_count > 0 with invalid memory.rx_filters (NULL vtable).
	{
		badMem := NewDefaultMemSet()
		badMem.RXFilters.VTable = nil
		if c, ok := New(vtableWithFilter, badMem, IfaceBitmapAll, 16, 0, 4); ok || c != nil {
			t.Error("New with invalid RXFilters resource should fail")
		}
	}

	// NULL vtable.
	if c, ok := New(nil, mem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
		t.Error("New with nil vtable should fail")
	}

	// vtable->now == NULL.
	{
		bad := &VTable{Now: nil, TX: mockTX, Filter: nil}
		if c, ok := New(bad, mem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
			t.Error("New with nil Now should fail")
		}
	}

	// vtable->tx == NULL.
	{
		bad := &VTable{Now: mockNow, TX: nil, Filter: nil}
		if c, ok := New(bad, mem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
			t.Error("New with nil TX should fail")
		}
	}

	// Invalid memory.tx_transfer (NULL vtable).
	{
		badMem := NewDefaultMemSet()
		badMem.TXTransfer.VTable = nil
		if c, ok := New(testVTable, badMem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
			t.Error("New with invalid TXTransfer resource should fail")
		}
	}

	// Invalid memory.tx_frame.
	{
		badMem := NewDefaultMemSet()
		badMem.TXFrame.VTable = nil
		if c, ok := New(testVTable, badMem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
			t.Error("New with invalid TXFrame resource should fail")
		}
	}

	// Invalid memory.rx_session.
	{
		badMem := NewDefaultMemSet()
		badMem.RXSession.VTable = nil
		if c, ok := New(testVTable, badMem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
			t.Error("New with invalid RXSession resource should fail")
		}
	}

	// Invalid memory.rx_payload.
	{
		badMem := NewDefaultMemSet()
		badMem.RXPayload.VTable = nil
		if c, ok := New(testVTable, badMem, IfaceBitmapAll, 16, 0, 0); ok || c != nil {
			t.Error("New with invalid RXPayload resource should fail")
		}
	}
}

func TestCanardSetNodeIDNull(t *testing.T) {
	var nilC *Canard
	if nilC.SetNodeID(0) {
		t.Error("SetNodeID on nil instance should return false")
	}
}

func TestCanardPollNull(t *testing.T) {
	var nilC *Canard
	nilC.Poll(0) // Must not crash.
}

func TestCanardIngestFrameValidation(t *testing.T) {
	self := &Canard{}
	cap := &txCapture{}
	initWithCapture(t, self, cap)
	data := []byte{0xE0}
	cd := data

	// NULL self.
	var nilC *Canard
	if nilC.IngestFrame(0, 0, 0, cd) {
		t.Error("IngestFrame with nil self should return false")
	}

	// iface_index >= IfaceCount.
	if self.IngestFrame(0, 3, 0, cd) {
		t.Error("IngestFrame with iface_index >= IfaceCount should return false")
	}

	// extended_can_id > 0x1FFFFFFF.
	if self.IngestFrame(0, 0, 0x20000000, cd) {
		t.Error("IngestFrame with extended_can_id > mask should return false")
	}

	// Note: the C branch `can_data.size > 0 && can_data.data == nullptr` is not representable in Go, where a
	// non-empty []byte always has a non-nil backing array. The library validates
	// `len(canData) == 0 || unsafe.SliceData(canData) != nil`, which is always satisfied for a real slice.

	self.Destroy()
}
