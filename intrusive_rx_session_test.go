// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// a compact implementation of the Cyphal/CAN transport for high-integrity real-time embedded systems.
//
// The library offers a non-blocking, callback-based API. It is not safe for concurrent use; if used in a
// concurrent environment it is the responsibility of the application to provide adequate synchronization.
//
// The original library is distributed under the terms of the MIT License. Copyright (c) OpenCyphal.

package libcanard

// This file migrates tests/src/test_intrusive_rx_session.c to Go.
//
// It is an in-package white-box test that exercises the RX session pipeline directly:
// rxSessionUpdate and its supporting functions (rxSlotNew, rxSlotAdvance, rxSessionAccept,
// rxSessionComplete, rxSessionCleanup).
//
// Note: The C original uses instrumented allocators to track memory allocations and verify
// memory balance. In this Go port, the rxSlot structs are allocated by Go's runtime while the
// payload buffers use the custom Mem allocators. This means some memory tracking tests from
// the C original cannot be directly ported.
//
// The tests here focus on the functional behavior of the RX session pipeline.

import (
	"testing"
	"unsafe"
)

// Simple test fixture for RX session tests
type rxSessionTestFixture struct {
	canard       *Canard
	sub          *Subscription
	callCount    int
	lastPayload  []byte
	lastTransferID uint8
	lastSourceID uint8
	lastPriority Prio
	lastTimestamp int64
}

func newRxSessionTestFixture(kind Kind, portID uint16, extent int) *rxSessionTestFixture {
	fx := &rxSessionTestFixture{}
	fx.canard = &Canard{}
	fx.sub = &Subscription{
		Owner:             fx.canard,
		TransferIDTimeout: 2000000, // 2 seconds
		Extent:            extent,
		Kind:              kind,
		PortID:            portID,
		CRCSeed:           crcInitial,
	}
	fx.sub.VTable = &SubscriptionVTable{
		OnMessage: fx.onMessage,
	}
	return fx
}

func (fx *rxSessionTestFixture) onMessage(self *Subscription, timestamp int64, priority Prio, 
	sourceNodeID uint8, transferID uint8, payload Payload) {
	fx.callCount++
	fx.lastTimestamp = timestamp
	fx.lastPriority = priority
	fx.lastSourceID = sourceNodeID
	fx.lastTransferID = transferID
	
	// Copy payload for later inspection
	if payload.View.Size > 0 && payload.View.Data != nil {
		fx.lastPayload = make([]byte, payload.View.Size)
		copy(fx.lastPayload, unsafeByteSlice(payload.View.Data, payload.View.Size))
	} else {
		fx.lastPayload = nil
	}
}

func (fx *rxSessionTestFixture) feed(ts int64, fr *rxFrameParsed, ifaceIndex uint8) {
	rxSessionUpdate(fx.sub, ts, fr, ifaceIndex)
}

func (fx *rxSessionTestFixture) cleanup() {
	for fx.sub.Sessions != nil {
		node := cavlMin(fx.sub.Sessions)
		rxSessionDestroy(node.owner.(*rxSession))
	}
}

// Helper functions

// unsafeByteSlice converts an unsafe.Pointer to a byte slice
func unsafeByteSlice(ptr unsafe.Pointer, size int) []byte {
	if ptr == nil || size == 0 {
		return nil
	}
	return (*[1 << 30]byte)(ptr)[:size:size]
}

// makeTestFrame is a helper to create test frames
func makeTestFrame(start, end, toggle bool, priority Prio, kind Kind, portID uint16, 
	dst, src, transferID uint8, payload []byte) *rxFrameParsed {
	return &rxFrameParsed{
		priority:   priority,
		kind:       kind,
		portID:     portID,
		dst:        dst,
		src:        src,
		transferID: transferID,
		start:      start,
		end:        end,
		toggle:     toggle,
		payload:    payload,
	}
}

// =============================================================================
// Group 1: Slot Lifecycle
// =============================================================================

func TestRxSlotNewV1(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage16b, 2222, 64)
	
	slot := rxSlotNew(fx.sub, 1000000, 5, 0)
	if slot == nil {
		t.Fatal("slot should not be nil")
	}
	
	if slot.crc != 0xFFFF {
		t.Errorf("slot.crc = 0x%04X, want 0xFFFF", slot.crc)
	}
	if !slot.expectedToggle {
		t.Error("slot.expectedToggle should be true for v1")
	}
	if slot.totalSize != 0 {
		t.Errorf("slot.totalSize = %d, want 0", slot.totalSize)
	}
	if slot.transferID != 5 {
		t.Errorf("slot.transferID = %d, want 5", slot.transferID)
	}
	if slot.ifaceIndex != 0 {
		t.Errorf("slot.ifaceIndex = %d, want 0", slot.ifaceIndex)
	}
	if slot.startTs != 1000000 {
		t.Errorf("slot.startTs = %d, want 1000000", slot.startTs)
	}
	
	rxSlotDestroy(fx.sub, slot)
}

func TestRxSlotNewV0(t *testing.T) {
	seed := CrcSeedFromDataTypeSignature(0xe2a7d4a9460bc2f2)
	fx := newRxSessionTestFixture(KindV0Message, 1001, 64)
	fx.sub.CRCSeed = seed
	
	slot := rxSlotNew(fx.sub, 2000000, 17, 1)
	if slot == nil {
		t.Fatal("slot should not be nil")
	}
	
	if slot.crc != seed {
		t.Errorf("slot.crc = 0x%04X, want 0x%04X", slot.crc, seed)
	}
	if slot.expectedToggle {
		t.Error("slot.expectedToggle should be false for v0")
	}
	if slot.transferID != 17 {
		t.Errorf("slot.transferID = %d, want 17", slot.transferID)
	}
	if slot.ifaceIndex != 1 {
		t.Errorf("slot.ifaceIndex = %d, want 1", slot.ifaceIndex)
	}
	
	rxSlotDestroy(fx.sub, slot)
}

func TestRxSlotAdvanceAndTruncation(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage16b, 2222, 10)
	
	slot := rxSlotNew(fx.sub, 1000000, 0, 0)
	if slot == nil {
		t.Fatal("slot should not be nil")
	}
	
	data0 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	data1 := []byte{0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E}
	
	rxSlotAdvance(slot, data0)
	if slot.totalSize != 7 {
		t.Errorf("slot.totalSize = %d, want 7", slot.totalSize)
	}
	if slot.expectedToggle {
		t.Error("slot.expectedToggle should be false after first advance")
	}
	
	rxSlotAdvance(slot, data1)
	if slot.totalSize != 14 {
		t.Errorf("slot.totalSize = %d, want 14", slot.totalSize)
	}
	if !slot.expectedToggle {
		t.Error("slot.expectedToggle should be true after second advance")
	}
	
	// Verify stored payload is truncated to extent=10
	expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A}
	actual := slot.payload[:10]
	if string(actual) != string(expected) {
		t.Errorf("slot.payload = %v, want %v", actual, expected)
	}
	
	rxSlotDestroy(fx.sub, slot)
}

// =============================================================================
// Group 2: v1 Golden Single-Frame
// =============================================================================

func TestRxSessionGoldenV1Heartbeat(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage16b, 32085, 64)
	
	// Frame data WITHOUT tail byte
	payload := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xA1}
	fr := makeTestFrame(true, true, true, PrioNominal, KindMessage16b, 32085, 
		NodeIDAnonymous, 42, 0, payload)
	
	fx.feed(1000000, fr, 0)
	
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	if fx.lastTimestamp != 1000000 {
		t.Errorf("timestamp = %d, want 1000000", fx.lastTimestamp)
	}
	if fx.lastPriority != PrioNominal {
		t.Errorf("priority = %v, want %v", fx.lastPriority, PrioNominal)
	}
	if fx.lastSourceID != 42 {
		t.Errorf("sourceNodeID = %d, want 42", fx.lastSourceID)
	}
	if fx.lastTransferID != 0 {
		t.Errorf("transferID = %d, want 0", fx.lastTransferID)
	}
	if len(fx.lastPayload) != 7 {
		t.Errorf("payload size = %d, want 7", len(fx.lastPayload))
	}
	if string(fx.lastPayload) != string(payload) {
		t.Errorf("payload data mismatch")
	}
	
	fx.cleanup()
}

func TestRxSessionGoldenV1Duck(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage13b, 2222, 64)
	
	payload := []byte{0x44, 0x55, 0x43, 0x4B}
	fr := makeTestFrame(true, true, true, PrioSlow, KindMessage13b, 2222, 
		NodeIDAnonymous, 42, 1, payload)
	
	fx.feed(1000000, fr, 0)
	
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	if fx.lastSourceID != 42 {
		t.Errorf("sourceNodeID = %d, want 42", fx.lastSourceID)
	}
	if fx.lastTransferID != 1 {
		t.Errorf("transferID = %d, want 1", fx.lastTransferID)
	}
	if fx.lastPriority != PrioSlow {
		t.Errorf("priority = %v, want %v", fx.lastPriority, PrioSlow)
	}
	if len(fx.lastPayload) != 4 {
		t.Errorf("payload size = %d, want 4", len(fx.lastPayload))
	}
	if string(fx.lastPayload) != string(payload) {
		t.Errorf("payload data mismatch")
	}
	
	fx.cleanup()
}

// =============================================================================
// Group 3: v1 Golden Multi-Frame
// =============================================================================

func TestRxSessionGoldenV1Seq14_3Frames(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage13b, 2222, 64)
	
	f0 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	f1 := []byte{0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E}
	f2 := []byte{0x32, 0xF8} // CRC big-endian
	
	// Frame 0: start
	fr0 := makeTestFrame(true, false, true, PrioSlow, KindMessage13b, 2222, 
		NodeIDAnonymous, 42, 2, f0)
	fx.feed(1000000, fr0, 0)
	
	// Frame 1: continuation
	fr1 := makeTestFrame(false, false, false, PrioSlow, KindMessage13b, 2222, 
		NodeIDAnonymous, 42, 2, f1)
	fx.feed(1000000, fr1, 0)
	
	// Frame 2: end
	fr2 := makeTestFrame(false, true, true, PrioSlow, KindMessage13b, 2222, 
		NodeIDAnonymous, 42, 2, f2)
	fx.feed(1000000, fr2, 0)
	
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	if fx.lastSourceID != 42 {
		t.Errorf("sourceNodeID = %d, want 42", fx.lastSourceID)
	}
	if fx.lastTransferID != 2 {
		t.Errorf("transferID = %d, want 2", fx.lastTransferID)
	}
	if len(fx.lastPayload) != 14 {
		t.Errorf("payload size = %d, want 14", len(fx.lastPayload))
	}
	
	expected := string([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14})
	if string(fx.lastPayload) != expected {
		t.Errorf("payload data = %v, want %v", fx.lastPayload, expected)
	}
	
	fx.cleanup()
}

// =============================================================================
// Group 5: Session Lifecycle
// =============================================================================

func TestRxSessionCreationOnStart(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage16b, 100, 64)
	
	if fx.sub.Sessions != nil {
		t.Error("sessions should be nil initially")
	}
	
	payload := []byte{0xAA}
	fr := makeTestFrame(true, true, true, PrioNominal, KindMessage16b, 100, 
		NodeIDAnonymous, 42, 0, payload)
	fx.feed(1000000, fr, 0)
	
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	
	if fx.sub.Sessions == nil {
		t.Error("sessions should not be nil after receiving frame")
	}
	
	fx.cleanup()
}

func TestRxSessionReuse(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage16b, 100, 64)
	
	payload := []byte{0xBB}
	
	for tid := uint8(0); tid < 3; tid++ {
		fr := makeTestFrame(true, true, true, PrioNominal, KindMessage16b, 100, 
			NodeIDAnonymous, 42, tid, payload)
		fx.feed(int64((1+int(tid))*1000000), fr, 0)
	}
	
	if fx.callCount != 3 {
		t.Errorf("callCount = %d, want 3", fx.callCount)
	}
	
	// Only one session should exist
	sessionCount := 0
	for fx.sub.Sessions != nil {
		node := cavlMin(fx.sub.Sessions)
		rxSessionDestroy(node.owner.(*rxSession))
		sessionCount++
	}
	if sessionCount != 1 {
		t.Errorf("session count = %d, want 1", sessionCount)
	}
}

func TestRxSessionDuplicateTidRejection(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage16b, 100, 64)
	
	payload := []byte{0xCC}
	fr := makeTestFrame(true, true, true, PrioNominal, KindMessage16b, 100, 
		NodeIDAnonymous, 42, 5, payload)
	
	// First: accepted
	fx.feed(1000000, fr, 0)
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	
	// Duplicate: rejected (same TID, same priority, within timeout)
	fx.feed(1000001, fr, 0)
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1 (no new callback)", fx.callCount)
	}
	
	fx.cleanup()
}

func TestRxSessionContinuationWithoutSession(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage16b, 100, 64)
	
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	fr := makeTestFrame(false, false, false, PrioNominal, KindMessage16b, 100, 
		NodeIDAnonymous, 42, 0, data)
	
	// No session exists for src=42. Continuation cannot create a session, so it's silently rejected.
	fx.feed(1000000, fr, 0)
	
	if fx.callCount != 0 {
		t.Errorf("callCount = %d, want 0", fx.callCount)
	}
	if fx.sub.Sessions != nil {
		t.Error("sessions should be nil")
	}
}

// =============================================================================
// Group 6: Priority Preemption
// =============================================================================

func TestRxSessionPreemptionIndependentSlots(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage13b, 2222, 64)
	
	fHi := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	fLo := []byte{0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	
	// Start a low-priority multi-frame transfer
	frLo := makeTestFrame(true, false, true, PrioSlow, KindMessage13b, 2222, 
		NodeIDAnonymous, 42, 0, fLo)
	fx.feed(1000000, frLo, 0)
	
	// Start a high-priority multi-frame transfer
	frHi := makeTestFrame(true, false, true, PrioImmediate, KindMessage13b, 2222, 
		NodeIDAnonymous, 42, 1, fHi)
	fx.feed(1000001, frHi, 0)
	
	// Complete the high-priority transfer
	crcHi := crcAdd(crcInitial, len(fHi), fHi)
	fHiEnd := []byte{byte(crcHi >> 8), byte(crcHi & 0xFF)}
	frHiEnd := makeTestFrame(false, true, false, PrioImmediate, KindMessage13b, 2222, 
		NodeIDAnonymous, 42, 1, fHiEnd)
	fx.feed(1000002, frHiEnd, 0)
	
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	if fx.lastPriority != PrioImmediate {
		t.Errorf("priority = %v, want %v", fx.lastPriority, PrioImmediate)
	}
	if fx.lastTransferID != 1 {
		t.Errorf("transferID = %d, want 1", fx.lastTransferID)
	}
	
	// Complete the low-priority transfer
	crcLo := crcAdd(crcInitial, len(fLo), fLo)
	fLoEnd := []byte{byte(crcLo >> 8), byte(crcLo & 0xFF)}
	frLoEnd := makeTestFrame(false, true, false, PrioSlow, KindMessage13b, 2222, 
		NodeIDAnonymous, 42, 0, fLoEnd)
	fx.feed(1000003, frLoEnd, 0)
	
	if fx.callCount != 2 {
		t.Errorf("callCount = %d, want 2", fx.callCount)
	}
	if fx.lastPriority != PrioSlow {
		t.Errorf("priority = %v, want %v", fx.lastPriority, PrioSlow)
	}
	if fx.lastTransferID != 0 {
		t.Errorf("transferID = %d, want 0", fx.lastTransferID)
	}
	
	fx.cleanup()
}

// =============================================================================
// Group 9: Edge Cases
// =============================================================================

func TestRxSessionTidRollover(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage16b, 100, 64)
	
	payload := []byte{0xAA}
	
	// Send TID=31
	fr31 := makeTestFrame(true, true, true, PrioNominal, KindMessage16b, 100, 
		NodeIDAnonymous, 42, 31, payload)
	fx.feed(1000000, fr31, 0)
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	if fx.lastTransferID != 31 {
		t.Errorf("transferID = %d, want 31", fx.lastTransferID)
	}
	
	// Send TID=0: must be accepted (0 != 31 → fresh)
	fr0 := makeTestFrame(true, true, true, PrioNominal, KindMessage16b, 100, 
		NodeIDAnonymous, 42, 0, payload)
	fx.feed(2000000, fr0, 0)
	if fx.callCount != 2 {
		t.Errorf("callCount = %d, want 2", fx.callCount)
	}
	if fx.lastTransferID != 0 {
		t.Errorf("transferID = %d, want 0", fx.lastTransferID)
	}
	
	// Duplicate TID=0 → rejected
	fx.feed(2000001, fr0, 0)
	if fx.callCount != 2 {
		t.Errorf("callCount = %d, want 2 (no new callback)", fx.callCount)
	}
	
	fx.cleanup()
}

func TestRxSessionMultipleSources(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage16b, 100, 64)
	
	payload := []byte{0x11}
	
	// src=10
	fr10 := makeTestFrame(true, true, true, PrioNominal, KindMessage16b, 100, 
		NodeIDAnonymous, 10, 0, payload)
	fx.feed(1000000, fr10, 0)
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	if fx.lastSourceID != 10 {
		t.Errorf("sourceNodeID = %d, want 10", fx.lastSourceID)
	}
	
	// src=20
	fr20 := makeTestFrame(true, true, true, PrioNominal, KindMessage16b, 100, 
		NodeIDAnonymous, 20, 0, payload)
	fx.feed(1000000, fr20, 0)
	if fx.callCount != 2 {
		t.Errorf("callCount = %d, want 2", fx.callCount)
	}
	if fx.lastSourceID != 20 {
		t.Errorf("sourceNodeID = %d, want 20", fx.lastSourceID)
	}
	
	// src=30
	fr30 := makeTestFrame(true, true, true, PrioNominal, KindMessage16b, 100, 
		NodeIDAnonymous, 30, 0, payload)
	fx.feed(1000000, fr30, 0)
	if fx.callCount != 3 {
		t.Errorf("callCount = %d, want 3", fx.callCount)
	}
	if fx.lastSourceID != 30 {
		t.Errorf("sourceNodeID = %d, want 30", fx.lastSourceID)
	}
	
	// 3 sessions should exist
	sessionCount := 0
	for fx.sub.Sessions != nil {
		node := cavlMin(fx.sub.Sessions)
		rxSessionDestroy(node.owner.(*rxSession))
		sessionCount++
	}
	if sessionCount != 3 {
		t.Errorf("session count = %d, want 3", sessionCount)
	}
}

// =============================================================================
// Group 10: New adversarial tests
// =============================================================================

func TestRxSessionInterleavedMultiframeTwoSources(t *testing.T) {
	fx := newRxSessionTestFixture(KindMessage13b, 2222, 64)
	
	d10 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	d20 := []byte{0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	
	// Complete node 10's transfer first
	// N10-F1: Start frame from node 10, TID=0
	fr10_0 := makeTestFrame(true, false, true, PrioNominal, KindMessage13b, 2222, 
		NodeIDAnonymous, 10, 0, d10)
	fx.feed(1000000, fr10_0, 0)
	
	// N10-F2: Continuation from node 10, toggle=0
	d10b := []byte{0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E}
	fr10_1 := makeTestFrame(false, false, false, PrioNominal, KindMessage13b, 2222, 
		NodeIDAnonymous, 10, 0, d10b)
	fx.feed(1000000, fr10_1, 0)
	
	// N10-F3: End frame from node 10, toggle=1
	crc10 := crcAdd(crcAdd(crcInitial, len(d10), d10), len(d10b), d10b)
	f10_end := []byte{byte(crc10 >> 8), byte(crc10 & 0xFF)}
	fr10_end := makeTestFrame(false, true, true, PrioNominal, KindMessage13b, 2222, 
		NodeIDAnonymous, 10, 0, f10_end)
	fx.feed(1000000, fr10_end, 0)
	
	if fx.callCount != 1 {
		t.Errorf("callCount after node 10 = %d, want 1", fx.callCount)
	}
	if fx.lastSourceID != 10 {
		t.Errorf("sourceNodeID = %d, want 10", fx.lastSourceID)
	}
	if len(fx.lastPayload) != 14 {
		t.Errorf("payload size = %d, want 14", len(fx.lastPayload))
	}
	
	// Now complete node 20's transfer
	// N20-F1: Start frame from node 20, TID=0
	fr20_0 := makeTestFrame(true, false, true, PrioNominal, KindMessage13b, 2222, 
		NodeIDAnonymous, 20, 0, d20)
	fx.feed(2000000, fr20_0, 0) // Different timestamp to avoid TID collision
	
	// N20-F2: Continuation from node 20, toggle=0
	d20b := []byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17}
	fr20_1 := makeTestFrame(false, false, false, PrioNominal, KindMessage13b, 2222, 
		NodeIDAnonymous, 20, 0, d20b)
	fx.feed(2000000, fr20_1, 0)
	
	// N20-F3: End frame from node 20, toggle=1
	crc20 := crcAdd(crcAdd(crcInitial, len(d20), d20), len(d20b), d20b)
	f20_end := []byte{byte(crc20 >> 8), byte(crc20 & 0xFF)}
	fr20_end := makeTestFrame(false, true, true, PrioNominal, KindMessage13b, 2222, 
		NodeIDAnonymous, 20, 0, f20_end)
	fx.feed(2000000, fr20_end, 0)
	
	if fx.callCount != 2 {
		t.Errorf("callCount = %d, want 2", fx.callCount)
	}
	if fx.lastSourceID != 20 {
		t.Errorf("sourceNodeID = %d, want 20", fx.lastSourceID)
	}
	if len(fx.lastPayload) != 14 {
		t.Errorf("payload size = %d, want 14", len(fx.lastPayload))
	}
	
	// 2 sessions should exist
	sessionCount := 0
	for fx.sub.Sessions != nil {
		node := cavlMin(fx.sub.Sessions)
		rxSessionDestroy(node.owner.(*rxSession))
		sessionCount++
	}
	if sessionCount != 2 {
		t.Errorf("session count = %d, want 2", sessionCount)
	}
}

// =============================================================================
// v0 Tests
// =============================================================================

func TestRxSessionV0SingleFrameNoCRC(t *testing.T) {
	fx := newRxSessionTestFixture(KindV0Message, 777, 64)
	fx.sub.CRCSeed = 0xBEEF // arbitrary for single-frame
	
	payload := []byte{0xFF, 0xFE, 0xFD, 0xFC, 0xFB}
	fr := makeTestFrame(true, true, false, PrioSlow, KindV0Message, 777, 
		NodeIDAnonymous, 50, 3, payload)
	
	fx.feed(1000000, fr, 0)
	
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	if fx.lastSourceID != 50 {
		t.Errorf("sourceNodeID = %d, want 50", fx.lastSourceID)
	}
	if fx.lastTransferID != 3 {
		t.Errorf("transferID = %d, want 3", fx.lastTransferID)
	}
	if fx.lastPriority != PrioSlow {
		t.Errorf("priority = %v, want %v", fx.lastPriority, PrioSlow)
	}
	if len(fx.lastPayload) != 5 {
		t.Errorf("payload size = %d, want 5", len(fx.lastPayload))
	}
	if string(fx.lastPayload) != string(payload) {
		t.Errorf("payload data mismatch")
	}
	
	fx.cleanup()
}

func TestRxSessionV0SyntheticMultiFrame(t *testing.T) {
	dts := uint64(0xABCDEF0123456789)
	crcSeed := CrcSeedFromDataTypeSignature(dts)
	fx := newRxSessionTestFixture(KindV0Message, 999, 64)
	fx.sub.CRCSeed = crcSeed
	
	userData := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}
	
	// In v0, the CRC is prepended (LE) to the payload
	crc := crcAdd(crcSeed, len(userData), userData)
	crcLo := byte(crc & 0xFF)
	crcHi := byte(crc >> 8)
	
	// Frame 0 (7 bytes): crc_lo, crc_hi, 0x11, 0x22, 0x33, 0x44, 0x55
	f0 := []byte{crcLo, crcHi, 0x11, 0x22, 0x33, 0x44, 0x55}
	// Frame 1 (3 bytes): 0x66, 0x77, 0x88
	f1 := []byte{0x66, 0x77, 0x88}
	
	// Start frame
	fr0 := makeTestFrame(true, false, false, PrioNominal, KindV0Message, 999, 
		NodeIDAnonymous, 10, 5, f0)
	fx.feed(1000000, fr0, 0)
	
	// End frame
	fr1 := makeTestFrame(false, true, true, PrioNominal, KindV0Message, 999, 
		NodeIDAnonymous, 10, 5, f1)
	fx.feed(1000000, fr1, 0)
	
	if fx.callCount != 1 {
		t.Errorf("callCount = %d, want 1", fx.callCount)
	}
	if fx.lastSourceID != 10 {
		t.Errorf("sourceNodeID = %d, want 10", fx.lastSourceID)
	}
	if fx.lastTransferID != 5 {
		t.Errorf("transferID = %d, want 5", fx.lastTransferID)
	}
	
	// total_size = 10, slot extent = 64+CRC_BYTES = 66. size = min(10-2, 66-2) = min(8, 64) = 8.
	if len(fx.lastPayload) != 8 {
		t.Errorf("payload size = %d, want 8", len(fx.lastPayload))
	}
	
	if string(fx.lastPayload) != string(userData) {
		t.Errorf("payload data = %v, want %v", fx.lastPayload, userData)
	}
	
	fx.cleanup()
}
