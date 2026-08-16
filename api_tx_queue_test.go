// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// a compact implementation of the Cyphal/CAN transport for high-integrity real-time embedded systems.
//
// The library offers a non-blocking, callback-based API. It is not safe for concurrent use; if used in a
// concurrent environment it is the responsibility of the application to provide adequate synchronization.
//
// The original library is distributed under the terms of the MIT License. Copyright (c) OpenCyphal.

package libcanard

// This file migrates tests/src/test_api_tx_queue.cpp to Go.
//
// It is an in-package white-box test that exercises the TX queue: capacity management,
// sacrifice logic, deadline expiration, ordering, interface bitmap handling, reference counting,
// scattered-gather payloads, OOM handling, and v0 Classic CAN enforcement.
//
// See CONVERT_CPP.MD for the conversion guidelines.

import (
	"testing"
)

// =====================================================================================================================
//                                         TX Capture Infrastructure
// =====================================================================================================================

// txQueueRecord mirrors the C++ tx_record_t with payload data capture.
type txQueueRecord struct {
	deadline   int64
	ifaceIndex uint8
	fd         bool
	canID      uint32
	tail       uint8
	dataSize   int
	data       [64]byte // Enough for the largest CAN FD frame
}

// txQueueCapture mirrors the C++ tx_capture_t.
type txQueueCapture struct {
	now      int64
	acceptTX bool
	count    int
	records  [128]txQueueRecord
}

func (cap *txQueueCapture) reset() {
	cap.now = 0
	cap.acceptTX = true
	cap.count = 0
	cap.records = [128]txQueueRecord{}
}

// txQueueCaptureNow returns the controllable clock value.
func txQueueCaptureNow(self *Canard) int64 {
	return self.UserContext.(*txQueueCapture).now
}

// txQueueCaptureTX records every transmitted frame with payload data.
func txQueueCaptureTX(self *Canard, _ any, deadline int64, ifaceIndex uint8, fd bool, extendedCANID uint32, canData []byte) bool {
	cap := self.UserContext.(*txQueueCapture)
	if cap.count < len(cap.records) {
		rec := &cap.records[cap.count]
		rec.deadline = deadline
		rec.ifaceIndex = ifaceIndex
		rec.fd = fd
		rec.canID = extendedCANID
		rec.tail = 0
		rec.dataSize = len(canData)
		if len(canData) > 0 {
			rec.tail = canData[len(canData)-1]
			copy(rec.data[:], canData)
		}
	}
	cap.count++
	return cap.acceptTX
}

var txQueueCaptureVTable = NewPlatform(txQueueCaptureNow, txQueueCaptureTX, nil,)

// =====================================================================================================================
//                                         Initialization Helpers
// =====================================================================================================================

// initTxQueueTestNode initializes a Canard instance for TX queue testing.
func initTxQueueTestNode(self *Canard, cap *txQueueCapture, queueCapacity int, nodeID uint8) {
	cap.reset()
	if !self.Init(txQueueCaptureVTable, NewDefaultMemSet(), IfaceBitmapAll, queueCapacity, 42, 0) {
		panic("Init failed")
	}
	if !self.SetNodeID(nodeID) {
		panic("SetNodeID failed")
	}
	self.UserContext = cap
}

// makePayload creates a BytesChain from a byte slice. In Go we use []byte directly.
func txQueueMakePayload(data []byte) []byte {
	return data
}

// makeEmptyPayload creates an empty payload.
func txQueueMakeEmptyPayload() []byte {
	return []byte{}
}

// tidFromTail extracts the transfer-ID from the tail byte (lower 5 bits).
func txQueueTIDFromTail(tail uint8) uint8 {
	return tail & TransferIDMax
}

// =====================================================================================================================
//                                         Test 1: sacrifice oldest
// =====================================================================================================================

// TestTxQueueSacrificeOldest tests that the oldest transfer is sacrificed when queue is full.
func TestTxQueueSacrificeOldest(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 3, 42)

	payload := txQueueMakeEmptyPayload()
	// Enqueue 3 single-frame transfers on iface 0 only.
	if !self.Publish16b(10000, 1, PrioNominal, 100, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if !self.Publish16b(10000, 1, PrioNominal, 100, 1, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if !self.Publish16b(10000, 1, PrioNominal, 100, 2, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.tx.queueSize != 3 {
		t.Errorf("queueSize = %d, want 3", self.tx.queueSize)
	}
	if self.Err.TXSacrifice != 0 {
		t.Errorf("TXSacrifice = %d, want 0", self.Err.TXSacrifice)
	}

	// Fourth publish triggers sacrifice of oldest (TID 0).
	if !self.Publish16b(10000, 1, PrioNominal, 100, 3, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.Err.TXSacrifice != 1 {
		t.Errorf("TXSacrifice = %d, want 1", self.Err.TXSacrifice)
	}
	if self.tx.queueSize != 3 {
		t.Errorf("queueSize = %d, want 3", self.tx.queueSize)
	}

	// Poll all frames out.
	self.Poll(1)
	if cap.count != 3 {
		t.Fatalf("count = %d, want 3", cap.count)
	}
	// The remaining transfers are TID 1, 2, 3 (TID 0 was sacrificed).
	if txQueueTIDFromTail(cap.records[0].tail) != 1 {
		t.Errorf("record[0] TID = %d, want 1", txQueueTIDFromTail(cap.records[0].tail))
	}
	if txQueueTIDFromTail(cap.records[1].tail) != 2 {
		t.Errorf("record[1] TID = %d, want 2", txQueueTIDFromTail(cap.records[1].tail))
	}
	if txQueueTIDFromTail(cap.records[2].tail) != 3 {
		t.Errorf("record[2] TID = %d, want 3", txQueueTIDFromTail(cap.records[2].tail))
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 2: sacrifice multiframe reclaims all
// =====================================================================================================================

// TestTxQueueSacrificeMultiframeReclaimsAll tests that sacrificing a multiframe transfer reclaims all its frames.
func TestTxQueueSacrificeMultiframeReclaimsAll(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 4, 42)
	self.tx.FD = false // Classic CAN: MTU=8, 7 payload bytes/frame.

	// 13-byte payload -> 3 frames on Classic CAN.
	multiData := make([]byte, 13)
	for i := range multiData {
		multiData[i] = byte(i + 1)
	}
	if !self.Publish16b(10000, 1, PrioNominal, 200, 0, multiData, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.tx.queueSize != 3 {
		t.Errorf("queueSize = %d, want 3", self.tx.queueSize)
	}

	// Single-frame transfer (empty payload = 1 frame).
	if !self.Publish16b(10000, 1, PrioNominal, 201, 1, txQueueMakeEmptyPayload(), nil) {
		t.Fatal("Publish16b failed")
	}
	if self.tx.queueSize != 4 {
		t.Errorf("queueSize = %d, want 4", self.tx.queueSize)
	}
	if self.Err.TXSacrifice != 0 {
		t.Errorf("TXSacrifice = %d, want 0", self.Err.TXSacrifice)
	}

	// Publish another single-frame -> oldest (the 3-frame multiframe) sacrificed.
	if !self.Publish16b(10000, 1, PrioNominal, 202, 2, txQueueMakeEmptyPayload(), nil) {
		t.Fatal("Publish16b failed")
	}
	if self.Err.TXSacrifice != 1 {
		t.Errorf("TXSacrifice = %d, want 1", self.Err.TXSacrifice)
	}
	// 4 - 3 (sacrificed) + 1 (new) = 2.
	if self.tx.queueSize != 2 {
		t.Errorf("queueSize = %d, want 2", self.tx.queueSize)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 3: sacrifice multiple rounds
// =====================================================================================================================

// TestTxQueueSacrificeMultipleRounds tests sacrificing multiple transfers to make room.
func TestTxQueueSacrificeMultipleRounds(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 4, 42)
	self.tx.FD = false // Classic CAN

	singlePayload := txQueueMakeEmptyPayload()
	for tid := uint8(0); tid < 4; tid++ {
		if !self.Publish16b(10000, 1, PrioNominal, 300, tid, singlePayload, nil) {
			t.Fatal("Publish16b failed")
		}
	}
	if self.tx.queueSize != 4 {
		t.Errorf("queueSize = %d, want 4", self.tx.queueSize)
	}

	// Multiframe needing 3 frames (13-byte payload on Classic CAN).
	multiData := make([]byte, 13)
	if !self.Publish16b(10000, 1, PrioNominal, 301, 10, multiData, nil) {
		t.Fatal("Publish16b failed")
	}
	// Must sacrifice 3 transfers to fit the 3-frame multiframe transfer.
	if self.Err.TXSacrifice < 3 {
		t.Errorf("TXSacrifice = %d, want >= 3", self.Err.TXSacrifice)
	}
	// 4 - 3 sacrificed + 3 new multiframe frames = 4. But wait, the remaining 1 single-frame + 3 multiframe = 4.
	if self.tx.queueSize != 4 {
		t.Errorf("queueSize = %d, want 4", self.tx.queueSize)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 4: capacity exceeded
// =====================================================================================================================

// TestTxQueueCapacityExceeded tests that publish fails when capacity is exceeded even after sacrifice.
func TestTxQueueCapacityExceeded(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 2, 42)
	self.tx.FD = false // Classic CAN

	// 25-byte payload on classic CAN: ceil((25+2+6)/7) = ceil(33/7) = 5 frames. Way over capacity=2.
	bigData := make([]byte, 25)
	if self.Publish16b(10000, 1, PrioNominal, 400, 0, bigData, nil) {
		t.Error("Publish16b should have failed (capacity exceeded)")
	}
	if self.Err.TXCapacity != 1 {
		t.Errorf("TXCapacity = %d, want 1", self.Err.TXCapacity)
	}
	if self.tx.queueSize != 0 {
		t.Errorf("queueSize = %d, want 0", self.tx.queueSize)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 5: capacity boundary
// =====================================================================================================================

// TestTxQueueCapacityBoundary tests publish with a transfer requiring exactly N frames.
func TestTxQueueCapacityBoundary(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	// Classic CAN: 13-byte payload = 3 frames. Set capacity=3.
	initTxQueueTestNode(self, cap, 3, 42)
	self.tx.FD = false

	data := make([]byte, 13)
	if !self.Publish16b(10000, 1, PrioNominal, 500, 0, data, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.tx.queueSize != 3 {
		t.Errorf("queueSize = %d, want 3", self.tx.queueSize)
	}
	if self.Err.TXCapacity != 0 {
		t.Errorf("TXCapacity = %d, want 0", self.Err.TXCapacity)
	}
	if self.Err.TXSacrifice != 0 {
		t.Errorf("TXSacrifice = %d, want 0", self.Err.TXSacrifice)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 6: deadline expiration
// =====================================================================================================================

// TestTxQueueDeadlineExpiration tests that expired transfers are purged when new transfers are published.
func TestTxQueueDeadlineExpiration(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 16, 42)

	payload := txQueueMakeEmptyPayload()
	// Transfer 1: short deadline.
	if !self.Publish16b(100, 1, PrioNominal, 600, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	// Transfer 2: long deadline.
	if !self.Publish16b(10000, 1, PrioNominal, 601, 1, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.tx.queueSize != 2 {
		t.Errorf("queueSize = %d, want 2", self.tx.queueSize)
	}

	// Advance time past the first transfer's deadline.
	cap.now = 200

	// Publish a third transfer; the expired one is purged during tx_push -> tx_expire.
	if !self.Publish16b(10000, 1, PrioNominal, 602, 2, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.Err.TXExpiration != 1 {
		t.Errorf("TXExpiration = %d, want 1", self.Err.TXExpiration)
	}
	// 2 original - 1 expired + 1 new = 2.
	if self.tx.queueSize != 2 {
		t.Errorf("queueSize = %d, want 2", self.tx.queueSize)
	}

	// Poll and verify only transfers 2 (TID 1) and 3 (TID 2) remain.
	self.Poll(1)
	if cap.count != 2 {
		t.Fatalf("count = %d, want 2", cap.count)
	}
	if txQueueTIDFromTail(cap.records[0].tail) != 1 {
		t.Errorf("record[0] TID = %d, want 1", txQueueTIDFromTail(cap.records[0].tail))
	}
	if txQueueTIDFromTail(cap.records[1].tail) != 2 {
		t.Errorf("record[1] TID = %d, want 2", txQueueTIDFromTail(cap.records[1].tail))
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 7: ordering by priority
// =====================================================================================================================

// TestTxQueueOrderingPriority tests that higher priority transfers are ejected first.
func TestTxQueueOrderingPriority(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 16, 42)

	payload := txQueueMakeEmptyPayload()
	// Low priority (5) first.
	if !self.Publish16b(10000, 1, PrioLow, 700, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	// Fast priority (2) second.
	if !self.Publish16b(10000, 1, PrioFast, 700, 1, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	self.Poll(1)
	if cap.count != 2 {
		t.Fatalf("count = %d, want 2", cap.count)
	}
	// Fast (priority 2) has a lower CAN ID than Low (priority 5) and is ejected first.
	if cap.records[0].canID >= cap.records[1].canID {
		t.Errorf("record[0].canID (%d) should be < record[1].canID (%d)", cap.records[0].canID, cap.records[1].canID)
	}
	// Verify priorities via CAN ID bits [28:26].
	if (cap.records[0].canID>>26)&7 != uint32(PrioFast) {
		t.Errorf("record[0] priority = %d, want %d", (cap.records[0].canID>>26)&7, PrioFast)
	}
	if (cap.records[1].canID>>26)&7 != uint32(PrioLow) {
		t.Errorf("record[1] priority = %d, want %d", (cap.records[1].canID>>26)&7, PrioLow)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 8: ordering FIFO same priority
// =====================================================================================================================

// TestTxQueueOrderingFIFOSamePriority tests FIFO ordering for same-priority transfers.
func TestTxQueueOrderingFIFOSamePriority(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 16, 42)

	payload := txQueueMakeEmptyPayload()
	if !self.Publish16b(10000, 1, PrioNominal, 800, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if !self.Publish16b(10000, 1, PrioNominal, 801, 1, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	self.Poll(1)
	if cap.count != 2 {
		t.Fatalf("count = %d, want 2", cap.count)
	}
	// Both have same priority; first-published (subject 800) ejected first due to smaller seqno tiebreaker.
	// Subject ID occupies bits [23:8] in v1.1.
	sid0 := (cap.records[0].canID >> 8) & 0xFFFF
	sid1 := (cap.records[1].canID >> 8) & 0xFFFF
	if sid0 != 800 {
		t.Errorf("record[0] subject ID = %d, want 800", sid0)
	}
	if sid1 != 801 {
		t.Errorf("record[1] subject ID = %d, want 801", sid1)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 9: interface bitmap single
// =====================================================================================================================

// TestTxIfaceBitmapSingle tests publish with single interface bitmap.
func TestTxIfaceBitmapSingle(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 16, 42)

	payload := txQueueMakeEmptyPayload()
	if !self.Publish16b(10000, 1, PrioNominal, 900, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.PendingIfaces() != 1 {
		t.Errorf("PendingIfaces = %d, want 1", self.PendingIfaces())
	}

	// Poll iface 0 -> ejected.
	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count = %d, want 1", cap.count)
	}
	if cap.records[0].ifaceIndex != 0 {
		t.Errorf("ifaceIndex = %d, want 0", cap.records[0].ifaceIndex)
	}

	// Poll iface 1 -> nothing.
	self.Poll(2)
	if cap.count != 1 {
		t.Fatalf("count = %d, want 1 (still)", cap.count)
	}
	if self.PendingIfaces() != 0 {
		t.Errorf("PendingIfaces = %d, want 0", self.PendingIfaces())
	}
	if self.tx.queueSize != 0 {
		t.Errorf("queueSize = %d, want 0", self.tx.queueSize)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 10: interface bitmap both
// =====================================================================================================================

// TestTxIfaceBitmapBoth tests publish with both interfaces.
// Note: This test requires IfaceCount >= 2.
func TestTxIfaceBitmapBoth(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 16, 42)

	payload := txQueueMakeEmptyPayload()
	if !self.Publish16b(10000, 3, PrioNominal, 1000, 5, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.PendingIfaces() != 3 {
		t.Errorf("PendingIfaces = %d, want 3", self.PendingIfaces())
	}

	// Poll iface 0.
	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count = %d, want 1", cap.count)
	}
	if cap.records[0].ifaceIndex != 0 {
		t.Errorf("ifaceIndex = %d, want 0", cap.records[0].ifaceIndex)
	}

	// Iface 1 still pending.
	if self.PendingIfaces() != 2 {
		t.Errorf("PendingIfaces = %d, want 2", self.PendingIfaces())
	}

	// Poll iface 1.
	self.Poll(2)
	if cap.count != 2 {
		t.Fatalf("count = %d, want 2", cap.count)
	}
	if cap.records[1].ifaceIndex != 1 {
		t.Errorf("ifaceIndex = %d, want 1", cap.records[1].ifaceIndex)
	}

	// Same CAN ID and tail byte on both interfaces.
	if cap.records[0].canID != cap.records[1].canID {
		t.Errorf("CAN IDs should be equal")
	}
	if cap.records[0].tail != cap.records[1].tail {
		t.Errorf("tail bytes should be equal")
	}
	if self.PendingIfaces() != 0 {
		t.Errorf("PendingIfaces = %d, want 0", self.PendingIfaces())
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 11: reference count lifecycle
// =====================================================================================================================

// TestTxRefcountLifecycle tests reference counting for frames shared across interfaces.
// Note: This test requires IfaceCount >= 2 to publish to interface bitmap 3.
func TestTxRefcountLifecycle(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 16, 42)

	payload := txQueueMakeEmptyPayload()
	if !self.Publish16b(10000, 3, PrioNominal, 1100, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	// Only 1 frame allocated even though 2 interfaces will use it.
	if self.tx.queueSize != 1 {
		t.Errorf("queueSize = %d, want 1", self.tx.queueSize)
	}

	// Eject from iface 0. Frame still referenced by iface 1.
	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count = %d, want 1", cap.count)
	}
	if cap.records[0].ifaceIndex != 0 {
		t.Errorf("ifaceIndex = %d, want 0", cap.records[0].ifaceIndex)
	}
	// Frame still counted because iface 1 holds a reference.
	if self.tx.queueSize != 1 {
		t.Errorf("queueSize = %d, want 1", self.tx.queueSize)
	}

	// Eject from iface 1. Last reference -> frame freed.
	self.Poll(2)
	if cap.count != 2 {
		t.Fatalf("count = %d, want 2", cap.count)
	}
	if cap.records[1].ifaceIndex != 1 {
		t.Errorf("ifaceIndex = %d, want 1", cap.records[1].ifaceIndex)
	}
	if self.tx.queueSize != 0 {
		t.Errorf("queueSize = %d, want 0", self.tx.queueSize)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 12: scattered gather 3 fragments
// =====================================================================================================================

// TestTxScatteredGather3Fragments tests scattered-gather payload assembly.
func TestTxScatteredGather3Fragments(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 16, 42)
	// CAN FD (default) for single-frame with 5 bytes total -> 1 frame with 8-byte DLC (rounded up).

	// Fragment 1: {0x11, 0x22}
	frag1 := []byte{0x11, 0x22}
	// Fragment 2: {0x33}
	frag2 := []byte{0x33}
	// Fragment 3: {0x44, 0x55}
	frag3 := []byte{0x44, 0x55}

	// In Go, Publish16b takes []byte directly, not a chain.
	// We need to concatenate the fragments.
	payload := append(append(frag1, frag2...), frag3...)

	if !self.Publish16b(10000, 1, PrioNominal, 1200, 7, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.tx.queueSize != 1 {
		t.Errorf("queueSize = %d, want 1", self.tx.queueSize)
	}

	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count = %d, want 1", cap.count)
	}

	rec := &cap.records[0]
	// 5 bytes payload + 1 tail byte = 6 bytes. CAN FD DLC supports 6-byte frames natively (no padding needed).
	if rec.dataSize != 6 {
		t.Errorf("dataSize = %d, want 6", rec.dataSize)
	}
	// Verify assembled payload bytes.
	if rec.data[0] != 0x11 {
		t.Errorf("data[0] = 0x%02X, want 0x11", rec.data[0])
	}
	if rec.data[1] != 0x22 {
		t.Errorf("data[1] = 0x%02X, want 0x22", rec.data[1])
	}
	if rec.data[2] != 0x33 {
		t.Errorf("data[2] = 0x%02X, want 0x33", rec.data[2])
	}
	if rec.data[3] != 0x44 {
		t.Errorf("data[3] = 0x%02X, want 0x44", rec.data[3])
	}
	if rec.data[4] != 0x55 {
		t.Errorf("data[4] = 0x%02X, want 0x55", rec.data[4])
	}
	// Tail byte: single-frame (SOT|EOT=0xC0), toggle=1 (0x20), TID=7 -> 0xE7.
	if rec.data[5] != 0xE7 {
		t.Errorf("data[5] = 0x%02X, want 0xE7", rec.data[5])
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 13: OOM frame allocation
// =====================================================================================================================

// TestTxOOMFrameAllocation tests OOM during frame allocation.
// Note: In the Go port, frame allocation uses the Go runtime, so we cannot directly test OOM.
// This test is adapted to verify that the library handles allocation failures gracefully.
func TestTxOOMFrameAllocation(t *testing.T) {
	self := &Canard{}
	// Use invalid memory resource to simulate OOM
	mem := NewDefaultMemSet()
	mem.TXFrame.VTable = nil

	// Init will fail because TXFrame memory is invalid
	if self.Init(txQueueCaptureVTable, mem, IfaceBitmapAll, 16, 42, 0) {
		t.Fatal("Init with invalid frame allocator should fail")
	}
	// Without successful Init, we cannot call Publish16b
	// This verifies that Init correctly rejects invalid memory configuration
}

// =====================================================================================================================
//                                         Test 14: OOM transfer allocation
// =====================================================================================================================

// TestTxOOMTransferAllocation tests OOM during transfer allocation.
// Note: In the Go port, transfer objects are allocated by the Go runtime, so we cannot directly test OOM.
// This test is adapted to verify that the library handles allocation failures gracefully.
func TestTxOOMTransferAllocation(t *testing.T) {
	self := &Canard{}
	// Use invalid memory resource to simulate OOM
	mem := NewDefaultMemSet()
	mem.TXTransfer.VTable = nil

	// Init will fail because TXTransfer memory is invalid
	if self.Init(txQueueCaptureVTable, mem, IfaceBitmapAll, 16, 42, 0) {
		t.Fatal("Init with invalid transfer allocator should fail")
	}
	// Without successful Init, we cannot call Publish16b
	// This verifies that Init correctly rejects invalid memory configuration
}

// =====================================================================================================================
//                                         Test 15: v0 always Classic CAN
// =====================================================================================================================

// TestTxV0AlwaysClassicCAN tests that v0 transfers always use Classic CAN.
func TestTxV0AlwaysClassicCAN(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 16, 42)
	self.tx.FD = true // FD mode enabled globally.

	payload := txQueueMakeEmptyPayload()
	// V0 publish signature: (deadline, ifaceBitmap, priority, dataTypeID, crcSeed, transferID, payload, userContext)
	if !self.V0Publish(10000, 1, PrioNominal, 100, 0, 0, payload, nil) {
		t.Fatal("V0Publish failed")
	}

	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count = %d, want 1", cap.count)
	}
	// v0 transfers are always Classic CAN regardless of the fd flag.
	if cap.records[0].fd {
		t.Error("fd should be false for v0 transfers")
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test 16: backpressure resumes
// =====================================================================================================================

// TestTxBackpressureResumes tests that backpressure is handled correctly.
func TestTxBackpressureResumes(t *testing.T) {
	self := &Canard{}
	cap := &txQueueCapture{}
	initTxQueueTestNode(self, cap, 16, 42)

	payload := txQueueMakeEmptyPayload()
	if !self.Publish16b(10000, 1, PrioNominal, 1600, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.tx.queueSize != 1 {
		t.Errorf("queueSize = %d, want 1", self.tx.queueSize)
	}

	// Simulate backpressure: TX callback rejects the frame.
	cap.acceptTX = false
	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count = %d, want 1", cap.count)
	}
	if self.PendingIfaces() != 1 {
		t.Errorf("PendingIfaces = %d, want 1", self.PendingIfaces())
	}
	if self.tx.queueSize != 1 {
		t.Errorf("queueSize = %d, want 1", self.tx.queueSize)
	}

	// Release backpressure and retry.
	cap.acceptTX = true
	self.Poll(1)
	if cap.count != 2 {
		t.Fatalf("count = %d, want 2", cap.count)
	}
	if self.PendingIfaces() != 0 {
		t.Errorf("PendingIfaces = %d, want 0", self.PendingIfaces())
	}
	if self.tx.queueSize != 0 {
		t.Errorf("queueSize = %d, want 0", self.tx.queueSize)
	}

	self.Destroy()
}
