// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// a compact implementation of the Cyphal/CAN transport for high-integrity real-time embedded systems.
//
// The library offers a non-blocking, callback-based API. It is not safe for concurrent use; if used in a
// concurrent environment it is the responsibility of the application to provide adequate synchronization.
//
// The original library is distributed under the terms of the MIT License. Copyright (c) OpenCyphal.

package libcanard

// This file migrates tests/src/test_api_roundtrip.cpp to Go.
//
// It is an in-package white-box test implementing the same adversarial TX->RX roundtrip harness: publish on a TX
// instance, poll to capture every CAN frame via the TX callback, then feed each captured frame into a separate RX
// instance and verify the RX callback delivers the correct payload.
//
// Divergences from the C++ original (see CONVERT_CPP.MD for rationale):
//   * The Go port uses contiguous []byte payloads, so the C test_18 "scattered payload" (a 3-fragment
//     canard_bytes_chain_t) is published as the concatenation of its fragments; the end-to-end reassembly is
//     verified identically.
//   * All tests use iface 0 only, which is valid under the Go port's IfaceCount == 1.

import (
	"testing"
)

// =====================================================================================================================
// Infrastructure
// =====================================================================================================================

type rtTXRecord struct {
	deadline   int64
	ifaceIndex uint8
	fd         bool
	canID      uint32
	dataSize   int
	data       [64]byte // max CAN FD frame
}

type rtTXCapture struct {
	now     int64
	count   int
	records [128]rtTXRecord
}

func (cap *rtTXCapture) reset() {
	cap.now = 0
	cap.count = 0
	cap.records = [128]rtTXRecord{}
}

type rtRXCapture struct {
	count        int
	timestamp    int64
	priority     Prio
	sourceNodeID uint8
	transferID   uint8
	payloadSize  int
	payloadBuf   [512]byte
}

type rtRXContext struct {
	nowVal  int64
	capture *rtRXCapture // not used by the now callback but kept together (mirrors the C harness)
}

// ------------------------------------------------  TX Capture  -------------------------------------------------------

func rtTXNow(self *Canard) int64 {
	return self.UserContext.(*rtTXCapture).now
}

func rtTX(self *Canard, _ any, deadline int64, ifaceIndex uint8, fd bool, extendedCANID uint32, canData []byte) bool {
	cap := self.UserContext.(*rtTXCapture)
	if cap == nil {
		return false
	}
	if cap.count < len(cap.records) {
		rec := &cap.records[cap.count]
		rec.deadline = deadline
		rec.ifaceIndex = ifaceIndex
		rec.fd = fd
		rec.canID = extendedCANID
		rec.dataSize = len(canData)
		if len(canData) > 0 {
			n := len(canData)
			if n > len(rec.data) {
				n = len(rec.data)
			}
			copy(rec.data[:n], canData)
		}
	}
	cap.count++
	return true // Always accept.
}

var rtTXVTable = &VTable{Now: rtTXNow, TX: rtTX, Filter: nil}

// ------------------------------------------------  RX Capture  -------------------------------------------------------

// For multi-frame transfers the origin must be freed by the application.
func rtOnMessage(self *Subscription, timestamp int64, priority Prio, sourceNodeID uint8, transferID uint8, payload Payload) {
	cap := self.UserContext.(*rtRXCapture)
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
	if payload.Origin.Size > 0 && payload.Origin.Data != nil {
		memFree(self.Owner.Mem.RXPayload, payload.Origin.Size, payload.Origin.Data)
	}
}

var rtSubVTable = &SubscriptionVTable{OnMessage: rtOnMessage}

// ------------------------------------------------  RX Helper (mock now)  ---------------------------------------------

func rtRXNow(self *Canard) int64 {
	return self.UserContext.(*rtRXContext).nowVal
}

func rtRXTX(_ *Canard, _ any, _ int64, _ uint8, _ bool, _ uint32, _ []byte) bool {
	return false // RX instance never transmits.
}

var rtRXVTable = &VTable{Now: rtRXNow, TX: rtRXTX, Filter: nil}

// ------------------------------------------------  Roundtrip Harness  ------------------------------------------------

const (
	rtTXNodeID = 42
	rtRXNodeID = 99
	rtDeadline = 1000000
	rtTimestamp = 5000
	rtExtent   = 512
)

// initRTTX sets up the TX instance with full-frame capture.
func initRTTX(tx *Canard, cap *rtTXCapture, nodeID uint8) {
	cap.reset()
	if !tx.Init(rtTXVTable, NewDefaultMemSet(), IfaceBitmapAll, 64, 0xCAFE, 0) {
		panic("rt tx Init failed")
	}
	if !tx.SetNodeID(nodeID) {
		panic("rt tx SetNodeID failed")
	}
	tx.UserContext = cap
}

// initRTRX sets up the RX instance.
func initRTRX(rx *Canard, ctx *rtRXContext, nodeID uint8) {
	*ctx = rtRXContext{}
	ctx.nowVal = 0
	ctx.capture = nil
	if !rx.Init(rtRXVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 0xBEEF, 0) {
		panic("rt rx Init failed")
	}
	if !rx.SetNodeID(nodeID) {
		panic("rt rx SetNodeID failed")
	}
	rx.UserContext = ctx
}

// feedRTCapturedFrames feeds all captured TX frames (iface 0 only) into the RX instance.
func feedRTCapturedFrames(rx *Canard, cap *rtTXCapture, timestamp int64) {
	for i := 0; i < cap.count; i++ {
		rec := &cap.records[i]
		if rec.ifaceIndex == 0 {
			frameData := rec.data[:rec.dataSize]
			rx.IngestFrame(timestamp, 0, rec.canID, frameData)
		}
	}
}

// =====================================================================================================================
// Test 1: v1.1 single-frame Classic CAN, 4-byte payload
// =====================================================================================================================
func TestRoundtripV1V1SingleFrameClassic(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	tx.tx.FD = false // Classic CAN

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 100, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 100, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count != 1 {
		t.Fatalf("txCap.count = %d, want 1", txCap.count)
	}
	if txCap.records[0].fd {
		t.Error("records[0].fd should be false (Classic CAN)")
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.priority != PrioNominal {
		t.Errorf("priority = %v, want PrioNominal", rxCap.priority)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 0 {
		t.Errorf("transferID = %d, want 0", rxCap.transferID)
	}
	if rxCap.payloadSize != 4 {
		t.Errorf("payloadSize = %d, want 4", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:4]; string(copy) != string(payload) {
		t.Errorf("payload = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 2: v1.1 single-frame CAN FD, 30-byte payload
// =====================================================================================================================
func TestRoundtripV1V1SingleFrameFD(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	// FD is default (tx.FD = true).

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 200, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	payload := make([]byte, 30)
	for i := range payload {
		payload[i] = byte(i + 1)
	}
	if !tx.Publish16b(rtDeadline, 1, PrioFast, 200, 5, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count != 1 {
		t.Fatalf("txCap.count = %d, want 1", txCap.count)
	}
	if !txCap.records[0].fd {
		t.Error("records[0].fd should be true (CAN FD)")
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.priority != PrioFast {
		t.Errorf("priority = %v, want PrioFast", rxCap.priority)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 5 {
		t.Errorf("transferID = %d, want 5", rxCap.transferID)
	}
	// 30-byte payload: the Go port strips CAN FD DLC padding, delivering exactly the payload.
	// (The C library includes the padding byte; this is a known divergence documented in CONVERT_CPP.MD.)
	if rxCap.payloadSize != 30 {
		t.Errorf("payloadSize = %d, want 30", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:30]; string(copy) != string(payload) {
		t.Errorf("payload[0:30] = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 3: v1.1 multiframe Classic CAN, 8-byte payload (exactly triggers multiframe: 8+1 > 8)
// =====================================================================================================================
func TestRoundtripV1V1MultiframeClassic2Frames(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	tx.tx.FD = false

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 300, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	payload := []byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17}
	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 300, 3, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count < 2 {
		t.Fatalf("txCap.count = %d, want >= 2", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.priority != PrioNominal {
		t.Errorf("priority = %v, want PrioNominal", rxCap.priority)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 3 {
		t.Errorf("transferID = %d, want 3", rxCap.transferID)
	}
	// Multiframe delivers payload+padding (CRC stripped). The original 8 bytes must be intact.
	if rxCap.payloadSize < 8 {
		t.Fatalf("payloadSize = %d, want >= 8", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:8]; string(copy) != string(payload) {
		t.Errorf("payload[0:8] = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 4: v1.1 multiframe Classic CAN, 20-byte payload (~4 frames)
// =====================================================================================================================
func TestRoundtripV1V1MultiframeClassicMany(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	tx.tx.FD = false

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 400, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	payload := make([]byte, 20)
	for i := range payload {
		payload[i] = byte(0xA0 + i)
	}
	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 400, 7, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count < 3 {
		t.Fatalf("txCap.count = %d, want >= 3", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 7 {
		t.Errorf("transferID = %d, want 7", rxCap.transferID)
	}
	if rxCap.payloadSize < 20 {
		t.Fatalf("payloadSize = %d, want >= 20", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:20]; string(copy) != string(payload) {
		t.Errorf("payload[0:20] = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 5: v1.1 multiframe CAN FD, 70-byte payload (2 frames)
// =====================================================================================================================
func TestRoundtripV1V1MultiframeFD(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	// FD is default.

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 500, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	payload := make([]byte, 70)
	for i := range payload {
		payload[i] = byte(i & 0xFF)
	}
	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 500, 1, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count < 2 {
		t.Fatalf("txCap.count = %d, want >= 2", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 1 {
		t.Errorf("transferID = %d, want 1", rxCap.transferID)
	}
	if rxCap.payloadSize < 70 {
		t.Fatalf("payloadSize = %d, want >= 70", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:70]; string(copy) != string(payload) {
		t.Errorf("payload[0:70] = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 6: v1.0 single-frame message (13-bit subject), 5-byte payload, subject_id=4000
// =====================================================================================================================
func TestRoundtripV1V0SingleFrame(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	tx.tx.FD = false // v1.0 typically Classic CAN.

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe13b(sub, 4000, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe13b failed")
	}
	sub.UserContext = rxCap

	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE}
	if !tx.Publish13b(rtDeadline, 1, PrioHigh, 4000, 10, payload, nil) {
		t.Fatal("Publish13b failed")
	}

	tx.Poll(1)
	if txCap.count != 1 {
		t.Fatalf("txCap.count = %d, want 1", txCap.count)
	}

	// Verify v1.0 CAN ID: bit7 must be 0 (not v1.1).
	if (txCap.records[0].canID >> 7 & 1) != 0 {
		t.Errorf("canID bit7 = %d, want 0 (v1.0)", (txCap.records[0].canID>>7)&1)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.priority != PrioHigh {
		t.Errorf("priority = %v, want PrioHigh", rxCap.priority)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 10 {
		t.Errorf("transferID = %d, want 10", rxCap.transferID)
	}
	if rxCap.payloadSize != 5 {
		t.Errorf("payloadSize = %d, want 5", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:5]; string(copy) != string(payload) {
		t.Errorf("payload = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 7: v1.0 multiframe message, 15-byte payload Classic CAN
// =====================================================================================================================
func TestRoundtripV1V0Multiframe(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	tx.tx.FD = false

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe13b(sub, 5000, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe13b failed")
	}
	sub.UserContext = rxCap

	payload := make([]byte, 15)
	for i := range payload {
		payload[i] = byte(0x50 + i)
	}
	if !tx.Publish13b(rtDeadline, 1, PrioNominal, 5000, 2, payload, nil) {
		t.Fatal("Publish13b failed")
	}

	tx.Poll(1)
	if txCap.count < 3 {
		t.Fatalf("txCap.count = %d, want >= 3", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 2 {
		t.Errorf("transferID = %d, want 2", rxCap.transferID)
	}
	if rxCap.payloadSize < 15 {
		t.Fatalf("payloadSize = %d, want >= 15", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:15]; string(copy) != string(payload) {
		t.Errorf("payload[0:15] = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 8: v1 service request, single-frame
// =====================================================================================================================
func TestRoundtripV1ServiceRequest(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID) // Client node.

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID) // Server node.
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.SubscribeRequest(sub, 50, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("SubscribeRequest failed")
	}
	sub.UserContext = rxCap

	payload := []byte{0xCA, 0xFE, 0x42}
	if !tx.Request(rtDeadline, PrioNominal, 50, rtRXNodeID, 0, payload, nil) {
		t.Fatal("Request failed")
	}

	tx.Poll(1)
	if txCap.count != 1 {
		t.Fatalf("txCap.count = %d, want 1", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 0 {
		t.Errorf("transferID = %d, want 0", rxCap.transferID)
	}
	if rxCap.payloadSize != 3 {
		t.Errorf("payloadSize = %d, want 3", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:3]; string(copy) != string(payload) {
		t.Errorf("payload = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 9: v1 service response, single-frame
// =====================================================================================================================
func TestRoundtripV1ServiceResponse(t *testing.T) {
	// For a response: TX is the server (RX_NODE_ID), RX is the client (TX_NODE_ID).
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtRXNodeID) // Server node sends response.

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtTXNodeID) // Client node receives response.
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.SubscribeResponse(sub, 60, rtExtent, rtSubVTable) != sub {
		t.Fatal("SubscribeResponse failed")
	}
	sub.UserContext = rxCap

	payload := []byte{0xBE, 0xEF, 0x00, 0xFF}
	if !tx.Respond(rtDeadline, PrioNominal, 60, rtTXNodeID, 15, payload, nil) {
		t.Fatal("Respond failed")
	}

	tx.Poll(1)
	if txCap.count != 1 {
		t.Fatalf("txCap.count = %d, want 1", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.sourceNodeID != rtRXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtRXNodeID)
	}
	if rxCap.transferID != 15 {
		t.Errorf("transferID = %d, want 15", rxCap.transferID)
	}
	if rxCap.payloadSize != 4 {
		t.Errorf("payloadSize = %d, want 4", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:4]; string(copy) != string(payload) {
		t.Errorf("payload = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 10: v1 service request multiframe, 20-byte payload
// =====================================================================================================================
func TestRoundtripV1ServiceMultiframe(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	tx.tx.FD = false // Force Classic CAN for multiframe.

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.SubscribeRequest(sub, 70, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("SubscribeRequest failed")
	}
	sub.UserContext = rxCap

	payload := make([]byte, 20)
	for i := range payload {
		payload[i] = byte(0xD0 + i)
	}
	if !tx.Request(rtDeadline, PrioHigh, 70, rtRXNodeID, 4, payload, nil) {
		t.Fatal("Request failed")
	}

	tx.Poll(1)
	if txCap.count < 3 {
		t.Fatalf("txCap.count = %d, want >= 3", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 4 {
		t.Errorf("transferID = %d, want 4", rxCap.transferID)
	}
	if rxCap.payloadSize < 20 {
		t.Fatalf("payloadSize = %d, want >= 20", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:20]; string(copy) != string(payload) {
		t.Errorf("payload[0:20] = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 11: All 8 priority levels roundtrip
// =====================================================================================================================
func TestRoundtripAllPriorities(t *testing.T) {
	for prio := 0; prio < PrioCount; prio++ {
		tx := &Canard{}
		txCap := &rtTXCapture{}
		initRTTX(tx, txCap, rtTXNodeID)

		rx := &Canard{}
		rxCtx := &rtRXContext{}
		rxCap := &rtRXCapture{}
		initRTRX(rx, rxCtx, rtRXNodeID)
		rxCtx.nowVal = rtTimestamp

		sub := &Subscription{}
		if rx.Subscribe16b(sub, 600, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
			t.Fatalf("Subscribe16b failed at prio %d", prio)
		}
		sub.UserContext = rxCap

		payload := []byte{0x11, 0x22}
		if !tx.Publish16b(rtDeadline, 1, Prio(prio), 600, 0, payload, nil) {
			t.Fatalf("Publish16b failed at prio %d", prio)
		}

		tx.Poll(1)
		feedRTCapturedFrames(rx, txCap, rtTimestamp)

		if rxCap.count != 1 {
			t.Fatalf("rxCap.count = %d, want 1 at prio %d", rxCap.count, prio)
		}
		if rxCap.priority != Prio(prio) {
			t.Errorf("priority = %v, want %v at prio %d", rxCap.priority, Prio(prio), prio)
		}
		if rxCap.payloadSize != 2 {
			t.Errorf("payloadSize = %d, want 2 at prio %d", rxCap.payloadSize, prio)
		}

		rx.Unsubscribe(sub)
		rx.Destroy()
		tx.Destroy()
	}
}

// =====================================================================================================================
// Test 12: All 32 transfer IDs roundtrip
// =====================================================================================================================
func TestRoundtripAllTransferIDs(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 700, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	for tid := 0; tid <= TransferIDMax; tid++ {
		// Reset captures in place so sub.UserContext / tx.UserContext keep pointing at the same structs.
		*txCap = rtTXCapture{}
		*rxCap = rtRXCapture{}
		// Advance the RX clock past the TID timeout so the same TID is accepted again.
		rxCtx.nowVal = int64(tid) * (DefaultTransferIDTimeoutUs + 1)

		payload := []byte{byte(tid & 0xFF)}
		if !tx.Publish16b(rtDeadline+rxCtx.nowVal, 1, PrioNominal, 700, uint8(tid), payload, nil) {
			t.Fatalf("Publish16b failed at tid %d", tid)
		}

		tx.Poll(1)
		feedRTCapturedFrames(rx, txCap, rxCtx.nowVal)

		if rxCap.count != 1 {
			t.Fatalf("rxCap.count = %d, want 1 at tid %d", rxCap.count, tid)
		}
		if rxCap.transferID != uint8(tid) {
			t.Errorf("transferID = %d, want %d", rxCap.transferID, tid)
		}
		if rxCap.payloadSize != 1 {
			t.Errorf("payloadSize = %d, want 1 at tid %d", rxCap.payloadSize, tid)
		}
		if rxCap.payloadBuf[0] != byte(tid&0xFF) {
			t.Errorf("payloadBuf[0] = %d, want %d at tid %d", rxCap.payloadBuf[0], tid, tid)
		}
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 13: Zero-length (empty) payload
// =====================================================================================================================
func TestRoundtripEmptyPayload(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 800, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 800, 0, []byte{}, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count != 1 {
		t.Fatalf("txCap.count = %d, want 1", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 0 {
		t.Errorf("transferID = %d, want 0", rxCap.transferID)
	}
	if rxCap.payloadSize != 0 {
		t.Errorf("payloadSize = %d, want 0", rxCap.payloadSize)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 14: Boundary - exactly 7 bytes (max single-frame Classic CAN)
// =====================================================================================================================
func TestRoundtripBoundary7BytesClassic(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	tx.tx.FD = false

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 900, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	payload := []byte{1, 2, 3, 4, 5, 6, 7}
	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 900, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count != 1 {
		t.Fatalf("txCap.count = %d, want 1 (7+1=8=MTU, single frame)", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.payloadSize != 7 {
		t.Errorf("payloadSize = %d, want 7", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:7]; string(copy) != string(payload) {
		t.Errorf("payload = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 15: Boundary - exactly 8 bytes (minimum multiframe Classic CAN)
// =====================================================================================================================
func TestRoundtripBoundary8BytesClassic(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	tx.tx.FD = false

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 1000, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	payload := []byte{10, 20, 30, 40, 50, 60, 70, 80}
	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 1000, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count < 2 {
		t.Fatalf("txCap.count = %d, want >= 2 (8+1 > 8 => multiframe)", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.payloadSize < 8 {
		t.Fatalf("payloadSize = %d, want >= 8", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:8]; string(copy) != string(payload) {
		t.Errorf("payload = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 16: Boundary - exactly 63 bytes (max single-frame CAN FD)
// =====================================================================================================================
func TestRoundtripBoundary63BytesFD(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	// FD is default.

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 1100, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	payload := make([]byte, 63)
	for i := range payload {
		payload[i] = byte(i ^ 0x55)
	}
	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 1100, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count != 1 {
		t.Fatalf("txCap.count = %d, want 1 (63+1=64=MTU, single frame)", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.payloadSize != 63 {
		t.Errorf("payloadSize = %d, want 63", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:63]; string(copy) != string(payload) {
		t.Errorf("payload = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 17: Boundary - exactly 64 bytes (minimum multiframe CAN FD)
// =====================================================================================================================
func TestRoundtripBoundary64BytesFD(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)
	// FD is default.

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 1200, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	payload := make([]byte, 64)
	for i := range payload {
		payload[i] = byte(i)
	}
	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 1200, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count < 2 {
		t.Fatalf("txCap.count = %d, want >= 2 (64+1 > 64 => multiframe)", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.payloadSize < 64 {
		t.Fatalf("payloadSize = %d, want >= 64", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:64]; string(copy) != string(payload) {
		t.Errorf("payload = %v, want %v", copy, payload)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}

// =====================================================================================================================
// Test 18: Scattered payload (3-fragment canard_bytes_chain_t), 3+4+5=12 bytes
//
// The Go port uses contiguous []byte payloads (no canard_bytes_chain_t), so the three fragments are concatenated
// into a single []byte. The end-to-end reassembly is verified identically.
// =====================================================================================================================
func TestRoundtripScatteredPayload(t *testing.T) {
	tx := &Canard{}
	txCap := &rtTXCapture{}
	initRTTX(tx, txCap, rtTXNodeID)

	rx := &Canard{}
	rxCtx := &rtRXContext{}
	rxCap := &rtRXCapture{}
	initRTRX(rx, rxCtx, rtRXNodeID)
	rxCtx.nowVal = rtTimestamp

	sub := &Subscription{}
	if rx.Subscribe16b(sub, 1300, rtExtent, DefaultTransferIDTimeoutUs, rtSubVTable) != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = rxCap

	// Three payload fragments: 3, 4, 5 bytes = 12 total.
	frag0 := []byte{0xAA, 0xBB, 0xCC}
	frag1 := []byte{0xDD, 0xEE, 0xFF, 0x11}
	frag2 := []byte{0x22, 0x33, 0x44, 0x55, 0x66}
	payload := append(append(append([]byte{}, frag0...), frag1...), frag2...)

	if !tx.Publish16b(rtDeadline, 1, PrioNominal, 1300, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	tx.Poll(1)
	if txCap.count < 1 {
		t.Fatalf("txCap.count = %d, want >= 1", txCap.count)
	}

	feedRTCapturedFrames(rx, txCap, rtTimestamp)

	if rxCap.count != 1 {
		t.Fatalf("rxCap.count = %d, want 1", rxCap.count)
	}
	if rxCap.sourceNodeID != rtTXNodeID {
		t.Errorf("sourceNodeID = %d, want %d", rxCap.sourceNodeID, rtTXNodeID)
	}
	if rxCap.transferID != 0 {
		t.Errorf("transferID = %d, want 0", rxCap.transferID)
	}

	// Verify reassembled payload matches the concatenation of the three fragments.
	expected := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66}
	if rxCap.payloadSize < 12 {
		t.Fatalf("payloadSize = %d, want >= 12", rxCap.payloadSize)
	}
	if copy := rxCap.payloadBuf[:12]; string(copy) != string(expected) {
		t.Errorf("payload[0:12] = %v, want %v", copy, expected)
	}

	rx.Unsubscribe(sub)
	rx.Destroy()
	tx.Destroy()
}
