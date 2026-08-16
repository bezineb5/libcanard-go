// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// a compact implementation of the Cyphal/CAN transport for high-integrity real-time embedded systems.
//
// The library offers a non-blocking, callback-based API. It is not safe for concurrent use; if used in a
// concurrent environment it is the responsibility of the application to provide adequate synchronization.
//
// The original library is distributed under the terms of the MIT License. Copyright (c) OpenCyphal.

package libcanard

// This file migrates tests/src/test_api_rx_edge.cpp to Go.
//
// It is an in-package white-box test that exercises adversarial edge cases for the RX pipeline,
// including multiframe reassembly, CRC validation, extent truncation, transfer-ID deduplication,
// priority preemption, anonymous transfers, malformed frames, interface affinity, and session lifecycle.
//
// See CONVERT_CPP.MD for the conversion guidelines.

import (
	"testing"
)

// =====================================================================================================================
//                                         RX Capture Callback
// =====================================================================================================================

// rxEdgeCapture mirrors the C++ rx_capture_t.
type rxEdgeCapture struct {
	count        int
	timestamp    int64
	priority     Prio
	sourceNodeID uint8
	transferID   uint8
	payloadSize  int
	payloadBuf   [512]byte
}

func (cap *rxEdgeCapture) reset() {
	cap.count = 0
	cap.timestamp = 0
	cap.priority = 0
	cap.sourceNodeID = 0
	cap.transferID = 0
	cap.payloadSize = 0
	cap.payloadBuf = [512]byte{}
}

// rxEdgeCaptureOnMessage mirrors capture_on_message.
func rxEdgeCaptureOnMessage(self *Subscription, timestamp int64, priority Prio, sourceNodeID uint8, transferID uint8, payload Payload) {
	cap := self.UserContext.(*rxEdgeCapture)
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
	// For multi-frame transfers the origin must be freed by the application.
	if payload.Origin.Size > 0 && payload.Origin.Data != nil {
		memFree(self.Owner.Mem.RXPayload, payload.Origin.Size, payload.Origin.Data)
	}
}

var rxEdgeCaptureSubVTable = &SubscriptionVTable{OnMessage: rxEdgeCaptureOnMessage}

// =====================================================================================================================
//                                         Mock VTable
// =====================================================================================================================

// rxEdgeMockNow returns the value stored in self.UserContext (*int64).
func rxEdgeMockNow(self *Canard) int64 {
	if self.UserContext != nil {
		return *(self.UserContext.(*int64))
	}
	return 0
}

func rxEdgeMockTX(_ *Canard, _ any, _ int64, _ uint8, _ bool, _ uint32, _ []byte) bool {
	return false
}

var rxEdgeTestVTable = NewPlatform(rxEdgeMockNow, rxEdgeMockTX, nil)

// initRxEdgeCanard initializes a Canard instance for RX edge testing with a controllable clock.
func initRxEdgeCanard(self *Canard, nowVal *int64, nodeID uint8) {
	*nowVal = 0
	if !self.Init(rxEdgeTestVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 1234, 0) {
		panic("Init failed")
	}
	if !self.SetNodeID(nodeID) {
		panic("SetNodeID failed")
	}
	self.UserContext = nowVal
}

// =====================================================================================================================
//                                    CAN Frame Construction Helpers
// =====================================================================================================================

// makeV1V1MsgCANID creates a v1.1 message CAN ID: priority[28:26] | subject_id[25:8] | bit7=1(v1.1) | src[6:0]
func rxEdgeMakeV1V1MsgCANID(prio Prio, subjectID uint16, src uint8) uint32 {
	return (uint32(prio) << prioShift) | (uint32(subjectID) << 8) | (1 << 7) | (uint32(src) & 0x7F)
}

// makeV1SingleTail creates a single-frame tail byte for v1: SOT=1, EOT=1, toggle=1.
func rxEdgeMakeV1SingleTail(tid uint8) uint8 {
	return uint8(tailSOT | tailEOT | tailToggle | (tid & transferIDMax))
}

// makeV1Tail creates a v1 tail byte with the specified flags.
func rxEdgeMakeV1Tail(sot, eot, toggle bool, tid uint8) uint8 {
	b := uint8(0)
	if sot {
		b |= tailSOT
	}
	if eot {
		b |= tailEOT
	}
	if toggle {
		b |= tailToggle
	}
	return b | (tid & transferIDMax)
}

// makeV0MsgCANID creates a v0 message CAN ID: priority[28:26] | data_type_id[23:8] | bit7=0 | src[6:0]
func rxEdgeMakeV0MsgCANID(prio Prio, dataTypeID uint16, src uint8) uint32 {
	return (uint32(prio) << prioShift) | (uint32(dataTypeID) << 8) | (uint32(src) & 0x7F)
}

// makeV0Tail creates a v0 tail byte with the specified flags.
func rxEdgeMakeV0Tail(sot, eot, toggle bool, tid uint8) uint8 {
	b := uint8(0)
	if sot {
		b |= tailSOT
	}
	if eot {
		b |= tailEOT
	}
	if toggle {
		b |= tailToggle
	}
	return b | (tid & transferIDMax)
}

// =====================================================================================================================
//                                    CRC-16/CCITT-FALSE Helper
// =====================================================================================================================

// crc16CCITT computes CRC-16/CCITT-FALSE (polynomial 0x1021, init 0xFFFF, no reflection).
// This mirrors the C++ crc16_ccitt function using the library's crcAdd.
func rxEdgeCRC16CCITT(crc uint16, data []byte) uint16 {
	return crcAdd(crc, len(data), data)
}

// =====================================================================================================================
//                                         Test: v1 2-frame Classic CAN multiframe
// =====================================================================================================================

// TestRxV1Multiframe2FrameClassic tests v1 2-frame Classic CAN multiframe reassembly.
func TestRxV1Multiframe2FrameClassic(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 1000, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	// Construct 2-frame v1.1 multiframe. Payload: 8 bytes {0x10..0x17}.
	// Classic CAN: MTU=8, payload_per_frame=7 (one byte for tail).
	// Frame 1 (SOT): 7 payload bytes + tail(SOT=1,EOT=0,toggle=1,tid=3)
	// Frame 2 (EOT): 1 payload byte + CRC(2 bytes, big-endian) + tail(SOT=0,EOT=1,toggle=0,tid=3)
	// Padding: frame 2 has 4 unused bytes that need to be padded with zeros before the CRC.
	payload := []byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1000, 10)

	// CRC over payload(8) + padding(4).
	padding := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, padding)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	// Frame 1 (8 bytes).
	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 3)

	// Frame 2 (8 bytes): [payload[7], pad, pad, pad, pad, crc_hi, crc_lo] + tail
	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[1] = 0x00
	frame2[2] = 0x00
	frame2[3] = 0x00
	frame2[4] = 0x00
	frame2[5] = crcHi
	frame2[6] = crcLo
	frame2[7] = rxEdgeMakeV1Tail(false, true, false, 3)

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1 = %d, want 0", cap.count)
	}

	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	if cap.timestamp != 100 {
		t.Errorf("timestamp = %d, want 100", cap.timestamp)
	}
	if cap.priority != PrioNominal {
		t.Errorf("priority = %v, want %v", cap.priority, PrioNominal)
	}
	if cap.sourceNodeID != 10 {
		t.Errorf("sourceNodeID = %d, want 10", cap.sourceNodeID)
	}
	if cap.transferID != 3 {
		t.Errorf("transferID = %d, want 3", cap.transferID)
	}
	// Delivered payload size is total_size - 2 (CRC stripped), where total_size includes the padding.
	// Frame 1 contributes 7 bytes, frame 2 contributes 7 bytes (1 payload + 4 padding + 2 CRC) => total_size=14.
	// Delivered size = min(14 - 2, extent) = 12.
	if cap.payloadSize != 12 {
		t.Errorf("payloadSize = %d, want 12", cap.payloadSize)
	}
	for i := 0; i < 8; i++ {
		if cap.payloadBuf[i] != payload[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], payload[i])
		}
	}
	// Padding bytes should be zero.
	for i := 8; i < 12; i++ {
		if cap.payloadBuf[i] != 0x00 {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x00", i, cap.payloadBuf[i])
		}
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: v1 3-frame multiframe
// =====================================================================================================================

// TestRxV1Multiframe3Frame tests v1 3-frame multiframe reassembly.
func TestRxV1Multiframe3Frame(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 2000, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	// 15 bytes of payload. Classic CAN, MTU=8, 7 data bytes per frame.
	// Frame 1 (SOT): 7 payload bytes, tail(SOT=1,EOT=0,toggle=1,tid=5)
	// Frame 2 (mid): 7 payload bytes, tail(SOT=0,EOT=0,toggle=0,tid=5)
	// Frame 3 (EOT): 1 payload byte + 4 pad + CRC(2) = 7, tail(SOT=0,EOT=1,toggle=1,tid=5)
	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F}
	canID := rxEdgeMakeV1V1MsgCANID(PrioHigh, 2000, 20)

	// CRC over payload(15) + padding(4).
	pad := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, pad)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 5)

	frame2 := make([]byte, 8)
	copy(frame2, payload[7:14])
	frame2[7] = rxEdgeMakeV1Tail(false, false, false, 5)

	frame3 := make([]byte, 8)
	frame3[0] = payload[14]
	frame3[1] = 0x00
	frame3[2] = 0x00
	frame3[3] = 0x00
	frame3[4] = 0x00
	frame3[5] = crcHi
	frame3[6] = crcLo
	frame3[7] = rxEdgeMakeV1Tail(false, true, true, 5)

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1 = %d, want 0", cap.count)
	}

	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame2 = %d, want 0", cap.count)
	}

	if !self.IngestFrame(300, 0, canID, frame3) {
		t.Fatal("IngestFrame frame3 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	if cap.timestamp != 100 {
		t.Errorf("timestamp = %d, want 100", cap.timestamp)
	}
	if cap.priority != PrioHigh {
		t.Errorf("priority = %v, want %v", cap.priority, PrioHigh)
	}
	if cap.sourceNodeID != 20 {
		t.Errorf("sourceNodeID = %d, want 20", cap.sourceNodeID)
	}
	if cap.transferID != 5 {
		t.Errorf("transferID = %d, want 5", cap.transferID)
	}
	// Delivered size = total_size - 2 = (7+7+7) - 2 = 19.
	if cap.payloadSize != 19 {
		t.Errorf("payloadSize = %d, want 19", cap.payloadSize)
	}
	for i := 0; i < 15; i++ {
		if cap.payloadBuf[i] != payload[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], payload[i])
		}
	}
	for i := 15; i < 19; i++ {
		if cap.payloadBuf[i] != 0x00 {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x00", i, cap.payloadBuf[i])
		}
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: v1 2-frame CAN FD multiframe
// =====================================================================================================================

// TestRxV1MultiframeFD tests v1 2-frame CAN FD multiframe reassembly.
func TestRxV1MultiframeFD(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 3000, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	// CAN FD: MTU=64, payload per frame=63. A 70-byte payload needs 2 frames.
	// Frame 1 (SOT): 63 payload bytes + tail(SOT=1,EOT=0,toggle=1,tid=1)
	// Frame 2 (EOT): 7 remaining payload bytes + padding(52) + CRC(2) + tail(SOT=0,EOT=1,toggle=0,tid=1)
	payload := make([]byte, 70)
	for i := 0; i < len(payload); i++ {
		payload[i] = byte(i & 0xFF)
	}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 3000, 30)

	// CRC covers payload(70) only (no padding for compact last frame).
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	// Frame 1 (64 bytes).
	frame1 := make([]byte, 64)
	copy(frame1, payload[:63])
	frame1[63] = rxEdgeMakeV1Tail(true, false, true, 1)

	// Frame 2 (10 bytes): 7 payload + CRC(2) + tail (no padding needed in compact last frame).

	frame2 := make([]byte, 10)
	copy(frame2, payload[63:70])
	frame2[7] = crcHi
	frame2[8] = crcLo
	frame2[9] = rxEdgeMakeV1Tail(false, true, false, 1)

	nowVal = 500
	if !self.IngestFrame(500, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1 = %d, want 0", cap.count)
	}

	if !self.IngestFrame(600, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	if cap.sourceNodeID != 30 {
		t.Errorf("sourceNodeID = %d, want 30", cap.sourceNodeID)
	}
	if cap.transferID != 1 {
		t.Errorf("transferID = %d, want 1", cap.transferID)
	}
	if cap.payloadSize != 70 {
		t.Errorf("payloadSize = %d, want 70", cap.payloadSize)
	}
	for i := 0; i < 70; i++ {
		if cap.payloadBuf[i] != payload[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], payload[i])
		}
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: multiframe CRC error
// =====================================================================================================================

// TestRxMultiframeCRCError tests that multiframe with CRC error is rejected.
func TestRxMultiframeCRCError(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 1100, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1100, 10)

	// Correct CRC (for sanity reference).
	pad := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, pad)

	// Corrupt the CRC by flipping the low byte.
	badCRCHi := uint8((uint(crc) >> 8) & 0xFF)
	badCRCLo := uint8((crc & 0xFF) ^ 0x01) // flipped bit

	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 0)

	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[1] = 0x00
	frame2[2] = 0x00
	frame2[3] = 0x00
	frame2[4] = 0x00
	frame2[5] = badCRCHi
	frame2[6] = badCRCLo
	frame2[7] = rxEdgeMakeV1Tail(false, true, false, 0)

	errBefore := self.Err.RXTransfer
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}

	if cap.count != 0 {
		t.Errorf("count = %d, want 0 (no delivery)", cap.count)
	}
	if self.Err.RXTransfer != errBefore+1 {
		t.Errorf("RXTransfer errors = %d, want %d", self.Err.RXTransfer, errBefore+1)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: single bit flip in payload
// =====================================================================================================================

// TestRxMultiframeSingleBitFlip tests that multiframe with single bit flip is rejected.
func TestRxMultiframeSingleBitFlip(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 1200, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payload := []byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1200, 10)

	// Compute correct CRC.
	pad := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, pad)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	// Frame 1 with a single bit flip in byte 3.
	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[3] ^= 0x04 // flip one bit in the 4th payload byte
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 9)

	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[1] = 0x00
	frame2[2] = 0x00
	frame2[3] = 0x00
	frame2[4] = 0x00
	frame2[5] = crcHi
	frame2[6] = crcLo
	frame2[7] = rxEdgeMakeV1Tail(false, true, false, 9)

	errBefore := self.Err.RXTransfer
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}

	if cap.count != 0 {
		t.Errorf("count = %d, want 0", cap.count)
	}
	if self.Err.RXTransfer != errBefore+1 {
		t.Errorf("RXTransfer errors = %d, want %d", self.Err.RXTransfer, errBefore+1)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: extent truncation single-frame
// =====================================================================================================================

// TestRxExtentTruncationSingleFrame tests extent truncation for single-frame transfers.
func TestRxExtentTruncationSingleFrame(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	// Subscribe with extent=4; payload will be 6 bytes.
	if got := self.Subscribe16b(sub, 1300, 4, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1300, 10)
	frame := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, rxEdgeMakeV1SingleTail(0)}

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	// For single-frame, the view points into the raw CAN buffer.
	// The library does not truncate single frames; that is an application-level concern.
	// So we expect the full 6 bytes.
	if cap.payloadSize != 6 {
		t.Errorf("payloadSize = %d, want 6", cap.payloadSize)
	}
	if cap.payloadBuf[0] != 0x01 {
		t.Errorf("payloadBuf[0] = 0x%02X, want 0x01", cap.payloadBuf[0])
	}
	if cap.payloadBuf[5] != 0x06 {
		t.Errorf("payloadBuf[5] = 0x%02X, want 0x06", cap.payloadBuf[5])
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: extent truncation multiframe
// =====================================================================================================================

// TestRxExtentTruncationMultiframe tests extent truncation for multiframe transfers.
func TestRxExtentTruncationMultiframe(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	// Subscribe with extent=5. Payload will be 8 bytes. Expect truncation to 5.
	if got := self.Subscribe16b(sub, 1400, 5, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payload := []byte{0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1400, 10)

	// CRC is computed over the full 8 payload bytes + 4 padding.
	pad := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, pad)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 2)

	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[1] = 0x00
	frame2[2] = 0x00
	frame2[3] = 0x00
	frame2[4] = 0x00
	frame2[5] = crcHi
	frame2[6] = crcLo
	frame2[7] = rxEdgeMakeV1Tail(false, true, false, 2)

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}

	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	// Delivered payload should be truncated to 5 bytes.
	// The total_size is 12 (8 payload + 4 pad), so size = min(total_size - 2, extent) = min(10, 5) = 5.
	if cap.payloadSize != 5 {
		t.Errorf("payloadSize = %d, want 5", cap.payloadSize)
	}
	if cap.payloadBuf[0] != 0xA0 {
		t.Errorf("payloadBuf[0] = 0x%02X, want 0xA0", cap.payloadBuf[0])
	}
	if cap.payloadBuf[4] != 0xA4 {
		t.Errorf("payloadBuf[4] = 0x%02X, want 0xA4", cap.payloadBuf[4])
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: transfer-ID duplicate rejected
// =====================================================================================================================

// TestRxTransferIDDuplicateRejected tests that duplicate transfer-ID is rejected.
func TestRxTransferIDDuplicateRejected(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 1500, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1500, 10)
	frame := []byte{0xDE, 0xAD, rxEdgeMakeV1SingleTail(7)}

	// First ingestion at t=0.
	nowVal = 0
	if !self.IngestFrame(0, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}

	// Second ingestion with same TID at t=1000000 (within 2s timeout). Should be rejected as duplicate.
	nowVal = 1000000
	if !self.IngestFrame(1000000, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1 (still)", cap.count)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: transfer-ID timeout boundary (accepted)
// =====================================================================================================================

// TestRxTransferIDTimeoutBoundary tests that transfer-ID at timeout boundary is accepted.
func TestRxTransferIDTimeoutBoundary(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	tidTimeout := int64(2000000) // 2 seconds
	if got := self.Subscribe16b(sub, 1600, 256, tidTimeout, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1600, 10)
	frame := []byte{0xAA, rxEdgeMakeV1SingleTail(5)}

	// First at t=0.
	nowVal = 0
	if !self.IngestFrame(0, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}

	// Second at t=tid_timeout+1 (just past the timeout). Should be accepted.
	nowVal = tidTimeout + 1
	if !self.IngestFrame(tidTimeout+1, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 2 {
		t.Errorf("count = %d, want 2", cap.count)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: transfer-ID timeout within (rejected)
// =====================================================================================================================

// TestRxTransferIDTimeoutWithin tests that transfer-ID within timeout is rejected.
func TestRxTransferIDTimeoutWithin(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	tidTimeout := int64(2000000)
	if got := self.Subscribe16b(sub, 1700, 256, tidTimeout, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1700, 10)
	frame := []byte{0xBB, rxEdgeMakeV1SingleTail(5)}

	// First at t=0.
	nowVal = 0
	if !self.IngestFrame(0, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}

	// Second at t=tid_timeout-1 (just before timeout). Should be rejected.
	nowVal = tidTimeout - 1
	if !self.IngestFrame(tidTimeout-1, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1 (still)", cap.count)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: cross-interface duplicate
// =====================================================================================================================

// TestRxDuplicateCrossInterface tests that duplicate across interfaces is rejected.
// Note: This test is guarded with t.Skip if IfaceCount < 2, as the Go port has IfaceCount=1.
func TestRxDuplicateCrossInterface(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 1800, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1800, 10)
	frame := []byte{0xCC, rxEdgeMakeV1SingleTail(0)}

	// First on interface 0.
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}

	// Same transfer on interface 1. Should be rejected (different interface, same TID, within timeout).
	nowVal = 101
	if !self.IngestFrame(101, 1, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1 (still)", cap.count)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: priority preemption
// =====================================================================================================================

// TestRxPriorityPreemption tests priority preemption.
func TestRxPriorityPreemption(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 1900, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payloadLo := []byte{0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7}
	payloadHi := []byte{0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7}

	// Low-priority (prio=4) transfer with TID=0, from node 10.
	canIDLo := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1900, 10)
	pad := []byte{0, 0, 0, 0}
	crcLoFull := rxEdgeCRC16CCITT(crcInitial, payloadLo)
	crcLoFull = rxEdgeCRC16CCITT(crcLoFull, pad)

	frame1Lo := make([]byte, 8)
	copy(frame1Lo, payloadLo[:7])
	frame1Lo[7] = rxEdgeMakeV1Tail(true, false, true, 0)

	frame2Lo := make([]byte, 8)
	frame2Lo[0] = payloadLo[7]
	frame2Lo[5] = uint8((uint(crcLoFull) >> 8) & 0xFF)
	frame2Lo[6] = uint8(crcLoFull & 0xFF)
	frame2Lo[7] = rxEdgeMakeV1Tail(false, true, false, 0)

	// High-priority (prio=2) transfer with TID=1, from same node 10.
	canIDHi := rxEdgeMakeV1V1MsgCANID(PrioFast, 1900, 10)
	crcHiFull := rxEdgeCRC16CCITT(crcInitial, payloadHi)
	crcHiFull = rxEdgeCRC16CCITT(crcHiFull, pad)

	frame1Hi := make([]byte, 8)
	copy(frame1Hi, payloadHi[:7])
	frame1Hi[7] = rxEdgeMakeV1Tail(true, false, true, 1)

	frame2Hi := make([]byte, 8)
	frame2Hi[0] = payloadHi[7]
	frame2Hi[5] = uint8((uint(crcHiFull) >> 8) & 0xFF)
	frame2Hi[6] = uint8(crcHiFull & 0xFF)
	frame2Hi[7] = rxEdgeMakeV1Tail(false, true, false, 1)

	nowVal = 100
	// Start low-priority.
	if !self.IngestFrame(100, 0, canIDLo, frame1Lo) {
		t.Fatal("IngestFrame frame1Lo failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1Lo = %d, want 0", cap.count)
	}
	// Start high-priority (preemption -- different priority slot).
	if !self.IngestFrame(110, 0, canIDHi, frame1Hi) {
		t.Fatal("IngestFrame frame1Hi failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1Hi = %d, want 0", cap.count)
	}
	// Complete low-priority.
	if !self.IngestFrame(120, 0, canIDLo, frame2Lo) {
		t.Fatal("IngestFrame frame2Lo failed")
	}
	if cap.count != 1 {
		t.Errorf("count after frame2Lo = %d, want 1", cap.count)
	}
	if cap.priority != PrioNominal {
		t.Errorf("priority = %v, want %v", cap.priority, PrioNominal)
	}
	for i := 0; i < 8; i++ {
		if cap.payloadBuf[i] != payloadLo[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], payloadLo[i])
		}
	}
	// Complete high-priority.
	if !self.IngestFrame(130, 0, canIDHi, frame2Hi) {
		t.Fatal("IngestFrame frame2Hi failed")
	}
	if cap.count != 2 {
		t.Errorf("count = %d, want 2", cap.count)
	}
	if cap.priority != PrioFast {
		t.Errorf("priority = %v, want %v", cap.priority, PrioFast)
	}
	for i := 0; i < 8; i++ {
		if cap.payloadBuf[i] != payloadHi[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], payloadHi[i])
		}
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: priority preemption interleaved
// =====================================================================================================================

// TestRxPriorityPreemptionInterleaved tests priority preemption with interleaved frames.
func TestRxPriorityPreemptionInterleaved(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 2100, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payloadLo := []byte{0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7}
	payloadHi := []byte{0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7}
	pad := []byte{0, 0, 0, 0}

	canIDLo := rxEdgeMakeV1V1MsgCANID(PrioNominal, 2100, 10)
	canIDHi := rxEdgeMakeV1V1MsgCANID(PrioFast, 2100, 10)

	// Build frames for both transfers.
	crcLo := rxEdgeCRC16CCITT(rxEdgeCRC16CCITT(crcInitial, payloadLo), pad)
	crcHi := rxEdgeCRC16CCITT(rxEdgeCRC16CCITT(crcInitial, payloadHi), pad)

	f1Lo := make([]byte, 8)
	f2Lo := make([]byte, 8)
	f1Hi := make([]byte, 8)
	f2Hi := make([]byte, 8)

	copy(f1Lo, payloadLo[:7])
	f1Lo[7] = rxEdgeMakeV1Tail(true, false, true, 0)
	f2Lo[0] = payloadLo[7]
	f2Lo[5] = uint8((uint(crcLo) >> 8) & 0xFF)
	f2Lo[6] = uint8(crcLo & 0xFF)
	f2Lo[7] = rxEdgeMakeV1Tail(false, true, false, 0)

	copy(f1Hi, payloadHi[:7])
	f1Hi[7] = rxEdgeMakeV1Tail(true, false, true, 1)
	f2Hi[0] = payloadHi[7]
	f2Hi[5] = uint8((uint(crcHi) >> 8) & 0xFF)
	f2Hi[6] = uint8(crcHi & 0xFF)
	f2Hi[7] = rxEdgeMakeV1Tail(false, true, false, 1)

	nowVal = 100
	// Interleave: lo_frame1, hi_frame1, lo_frame2, hi_frame2.
	if !self.IngestFrame(100, 0, canIDLo, f1Lo) {
		t.Fatal("IngestFrame f1Lo failed")
	}
	if cap.count != 0 {
		t.Errorf("count after f1Lo = %d, want 0", cap.count)
	}

	if !self.IngestFrame(110, 0, canIDHi, f1Hi) {
		t.Fatal("IngestFrame f1Hi failed")
	}
	if cap.count != 0 {
		t.Errorf("count after f1Hi = %d, want 0", cap.count)
	}

	if !self.IngestFrame(120, 0, canIDLo, f2Lo) {
		t.Fatal("IngestFrame f2Lo failed")
	}
	if cap.count != 1 {
		t.Errorf("count after f2Lo = %d, want 1", cap.count)
	}
	if cap.priority != PrioNominal {
		t.Errorf("priority = %v, want %v", cap.priority, PrioNominal)
	}

	if !self.IngestFrame(130, 0, canIDHi, f2Hi) {
		t.Fatal("IngestFrame f2Hi failed")
	}
	if cap.count != 2 {
		t.Errorf("count = %d, want 2", cap.count)
	}
	if cap.priority != PrioFast {
		t.Errorf("priority = %v, want %v", cap.priority, PrioFast)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: anonymous single-frame accepted
// =====================================================================================================================

// TestRxAnonymousSingleFrameAccepted tests that anonymous single-frame is accepted.
func TestRxAnonymousSingleFrameAccepted(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	// Subscribe to v1.0 subject 100 (anonymous nodes use v1.0 format with bit24=1 and src field as discriminator).
	if got := self.Subscribe13b(sub, 100, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe13b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	// v1.0 anonymous message: prio[28:26] | 0 | 1(bit24=anon) | subject_id[20:8] | bit7=0 | discriminator[6:0]
	// The source field in the CAN ID is a pseudo-CRC of the payload (discriminator), not a real node-ID.
	canID := (uint32(PrioNominal) << prioShift) | (1 << 24) | (uint32(100) << 8) | 0x55 // discriminator = 0x55

	// v1 anonymous is single-frame only, toggle must start at 1 (v1 start toggle).
	frame := []byte{0xCA, 0xFE, rxEdgeMakeV1SingleTail(0)}

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	if cap.priority != PrioNominal {
		t.Errorf("priority = %v, want %v", cap.priority, PrioNominal)
	}
	if cap.payloadSize != 2 {
		t.Errorf("payloadSize = %d, want 2", cap.payloadSize)
	}
	if cap.payloadBuf[0] != 0xCA {
		t.Errorf("payloadBuf[0] = 0x%02X, want 0xCA", cap.payloadBuf[0])
	}
	if cap.payloadBuf[1] != 0xFE {
		t.Errorf("payloadBuf[1] = 0x%02X, want 0xFE", cap.payloadBuf[1])
	}
	// Source should be CANARD_NODE_ID_ANONYMOUS (0xFF).
	if cap.sourceNodeID != NodeIDAnonymous {
		t.Errorf("sourceNodeID = %d, want %d (NodeIDAnonymous)", cap.sourceNodeID, NodeIDAnonymous)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: anonymous multiframe rejected
// =====================================================================================================================

// TestRxAnonymousMultiframeRejected tests that anonymous multiframe is rejected.
func TestRxAnonymousMultiframeRejected(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe13b(sub, 200, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe13b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	// Construct a v1.0 anonymous frame that looks like SOT but not EOT (attempting multiframe).
	// For v1.0: prio[28:26] | 0 | bit24=1(anon) | subject_id[20:8] | bit7=0 | discriminator[6:0]
	canID := (uint32(PrioNominal) << prioShift) | (1 << 24) | (uint32(200) << 8) | 0x33

	// SOT=1, EOT=0, toggle=1 (v1 start). This should be rejected because anonymous cannot be multiframe.
	// The rx_parse function enforces: is_v1 = is_v1 && start && end for anonymous frames.
	frame := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, rxEdgeMakeV1Tail(true, false, true, 0)}

	errBefore := self.Err.RXFrame
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 0 {
		t.Errorf("count = %d, want 0", cap.count)
	}
	// The frame should be unparseable as v1, and potentially parseable as v0 but with no matching subscription.
	// If neither parses, rx_frame error is incremented. Let's just verify no delivery.
	// The v0 parse may succeed, so we don't assert on error count.
	_ = errBefore

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: empty frame (malformed)
// =====================================================================================================================

// TestRxMalformedEmptyFrame tests that empty frame is rejected.
func TestRxMalformedEmptyFrame(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 2200, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 2200, 10)
	// Empty frame
	canData := []byte{}

	errBefore := self.Err.RXFrame
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, canData) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 0 {
		t.Errorf("count = %d, want 0", cap.count)
	}
	if self.Err.RXFrame != errBefore+1 {
		t.Errorf("RXFrame errors = %d, want %d", self.Err.RXFrame, errBefore+1)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: wrong toggle rejected
// =====================================================================================================================

// TestRxWrongToggleRejected tests that frame with wrong toggle is rejected.
func TestRxWrongToggleRejected(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 2300, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 2300, 10)

	// Correct CRC.
	pad := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, pad)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	// Frame 1 (SOT): toggle=1 (correct for v1).
	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 4)

	// Frame 2 (EOT): toggle=1 (WRONG! should be 0).
	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[5] = crcHi
	frame2[6] = crcLo
	frame2[7] = rxEdgeMakeV1Tail(false, true, true, 4) // toggle=1, should be 0

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1 = %d, want 0", cap.count)
	}

	// The second frame has the wrong toggle. The admission solver checks:
	// slot->expected_toggle == fr->toggle. Expected is 0 (toggled from initial 1), but we sent 1. Rejected.
	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 0 {
		t.Errorf("count = %d, want 0 (never completes)", cap.count)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: interface affinity
// =====================================================================================================================

// TestRxInterfaceAffinity tests interface affinity for multiframe transfers.
func TestRxInterfaceAffinity(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 2400, 256, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 2400, 10)

	pad := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, pad)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 6)

	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[5] = crcHi
	frame2[6] = crcLo
	frame2[7] = rxEdgeMakeV1Tail(false, true, false, 6)

	nowVal = 100
	// Start on interface 0.
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1 = %d, want 0", cap.count)
	}

	// Continuation on interface 1. Should be dropped (interface affinity mismatch).
	if !self.IngestFrame(200, 1, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 0 {
		t.Errorf("count = %d, want 0 (transfer never completes)", cap.count)
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: stale session cleanup
// =====================================================================================================================

// TestRxStaleSessionCleanup tests that stale sessions are cleaned up.
func TestRxStaleSessionCleanup(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	tidTimeout := int64(2000000)
	if got := self.Subscribe16b(sub, 2500, 256, tidTimeout, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 2500, 10)
	frame1 := []byte{0xDD, rxEdgeMakeV1SingleTail(5)}

	// Receive first transfer at t=0.
	nowVal = 0
	if !self.IngestFrame(0, 0, canID, frame1) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}

	// Advance time well past session timeout (30s).
	// Call poll() enough times to trigger session cleanup.
	// RX_SESSION_TIMEOUT is 30 * MEGA = 30_000_000 us.
	// The session is destroyed when: last_admission_ts < (now - transfer_id_timeout)
	// and there are no in-progress slots.
	// We need to poll enough times because poll only cleans the oldest session per call.
	nowVal = 35000000 // 35 seconds.
	self.Poll(0)

	// Now send the same transfer-ID again. It should be accepted because the session was destroyed.
	frame2 := []byte{0xEE, rxEdgeMakeV1SingleTail(5)}
	if !self.IngestFrame(35000000, 0, canID, frame2) {
		t.Fatal("IngestFrame failed")
	}
	if cap.count != 2 {
		t.Errorf("count = %d, want 2", cap.count)
	}
	if cap.payloadBuf[0] != 0xEE {
		t.Errorf("payloadBuf[0] = 0x%02X, want 0xEE", cap.payloadBuf[0])
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: v0 multiframe roundtrip
// =====================================================================================================================

// TestRxV0MultiframeRoundtrip tests v0 multiframe roundtrip.
func TestRxV0MultiframeRoundtrip(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}

	// v0 data type signature for UAVCAN.protocol.GetNodeInfo (a well-known type).
	dataTypeSignature := uint64(0xEE468A8121C46A9E)
	crcSeed := CrcSeedFromDataTypeSignature(dataTypeSignature)

	// The extent represents user payload size; the library adds CRC overhead internally.
	if got := self.V0Subscribe(sub, 1000, crcSeed, 8, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("V0Subscribe returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payload := []byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80}
	canID := rxEdgeMakeV0MsgCANID(PrioNominal, 1000, 10)

	// v0 multiframe CRC: computed over the user payload only, then prepended little-endian.
	v0crc := rxEdgeCRC16CCITT(crcSeed, payload)

	// Total stream data: [crc_lo, crc_hi, payload[0..7]] = 10 bytes.
	// Classic CAN MTU=8, 7 data bytes per frame + tail byte.
	// Frame 1 (SOT): [crc_lo, crc_hi, payload[0..4]] + tail(SOT=1,EOT=0,toggle=0,tid=2) -- 7 data + 1 tail
	// Frame 2 (EOT): [payload[5..7]] + tail(SOT=0,EOT=1,toggle=1,tid=2) -- 3 data + 1 tail (last frame, short ok)

	frame1 := make([]byte, 8)
	frame1[0] = uint8(v0crc & 0xFF)                     // crc_lo
	frame1[1] = uint8((uint(v0crc) >> 8) & 0xFF)        // crc_hi
	copy(frame1[2:], payload[:5])                       // first 5 payload bytes
	frame1[7] = rxEdgeMakeV0Tail(true, false, false, 2) // SOT=1, EOT=0, toggle=0 (v0 starts toggle=0)

	frame2 := make([]byte, 4)
	copy(frame2, payload[5:8])                         // remaining 3 payload bytes
	frame2[3] = rxEdgeMakeV0Tail(false, true, true, 2) // SOT=0, EOT=1, toggle=1

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1 = %d, want 0", cap.count)
	}

	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	if cap.timestamp != 100 {
		t.Errorf("timestamp = %d, want 100", cap.timestamp)
	}
	if cap.priority != PrioNominal {
		t.Errorf("priority = %v, want %v", cap.priority, PrioNominal)
	}
	if cap.sourceNodeID != 10 {
		t.Errorf("sourceNodeID = %d, want 10", cap.sourceNodeID)
	}
	if cap.transferID != 2 {
		t.Errorf("transferID = %d, want 2", cap.transferID)
	}
	// The delivered payload excludes the 2-byte CRC prefix: 8 bytes of user data.
	if cap.payloadSize != 8 {
		t.Errorf("payloadSize = %d, want 8", cap.payloadSize)
	}
	for i := 0; i < 8; i++ {
		if cap.payloadBuf[i] != payload[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], payload[i])
		}
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: v0 extent excludes CRC
// =====================================================================================================================

// TestRxV0ExtentExcludesCRC tests that v0 extent excludes CRC overhead.
func TestRxV0ExtentExcludesCRC(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}

	dataTypeSignature := uint64(0xEE468A8121C46A9E)
	crcSeed := CrcSeedFromDataTypeSignature(dataTypeSignature)

	// extent=10 means we want up to 10 bytes of user payload (CRC handled internally).
	if got := self.V0Subscribe(sub, 2000, crcSeed, 10, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("V0Subscribe returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	// 10-byte user payload, which requires 2 classic CAN frames with v0 framing:
	// Total stream: [crc_lo, crc_hi, payload[0..9]] = 12 bytes, 7 data bytes per frame + tail.
	// Frame 1 (SOT): [crc_lo, crc_hi, payload[0..4]] + tail = 8 bytes
	// Frame 2 (EOT): [payload[5..9]] + tail = 6 bytes (last frame can be short).
	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A}
	canID := rxEdgeMakeV0MsgCANID(PrioNominal, 2000, 10)
	v0crc := rxEdgeCRC16CCITT(crcSeed, payload)

	frame1 := make([]byte, 8)
	frame1[0] = uint8(v0crc & 0xFF)
	frame1[1] = uint8((uint(v0crc) >> 8) & 0xFF)
	copy(frame1[2:], payload[:5])
	frame1[7] = rxEdgeMakeV0Tail(true, false, false, 5)

	frame2 := make([]byte, 6)
	copy(frame2, payload[5:10])
	frame2[5] = rxEdgeMakeV0Tail(false, true, true, 5)

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1 = %d, want 0", cap.count)
	}

	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	// All 10 bytes must be delivered -- extent=10 means 10 bytes of user payload.
	if cap.payloadSize != 10 {
		t.Errorf("payloadSize = %d, want 10", cap.payloadSize)
	}
	for i := 0; i < 10; i++ {
		if cap.payloadBuf[i] != payload[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], payload[i])
		}
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: v0 extent truncation
// =====================================================================================================================

// TestRxV0ExtentTruncation tests v0 extent truncation.
func TestRxV0ExtentTruncation(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}

	dataTypeSignature := uint64(0xEE468A8121C46A9E)
	crcSeed := CrcSeedFromDataTypeSignature(dataTypeSignature)

	// extent=4 means at most 4 bytes of user payload. CRC overhead handled internally.
	if got := self.V0Subscribe(sub, 3000, crcSeed, 4, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("V0Subscribe returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	// 8-byte user payload -> after truncation only 4 bytes should be delivered.
	payload := []byte{0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7, 0xA8}
	canID := rxEdgeMakeV0MsgCANID(PrioNominal, 3000, 10)
	// CRC is over the full payload (not truncated).
	v0crc := rxEdgeCRC16CCITT(crcSeed, payload)

	// Stream: [crc_lo, crc_hi, payload[0..7]] = 10 bytes -> 2 frames.
	frame1 := make([]byte, 8)
	frame1[0] = uint8(v0crc & 0xFF)
	frame1[1] = uint8((uint(v0crc) >> 8) & 0xFF)
	copy(frame1[2:], payload[:5])
	frame1[7] = rxEdgeMakeV0Tail(true, false, false, 7)

	frame2 := make([]byte, 4)
	copy(frame2, payload[5:8])
	frame2[3] = rxEdgeMakeV0Tail(false, true, true, 7)

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if cap.count != 0 {
		t.Errorf("count after frame1 = %d, want 0", cap.count)
	}

	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	// Only 4 bytes of user data delivered due to extent truncation.
	if cap.payloadSize != 4 {
		t.Errorf("payloadSize = %d, want 4", cap.payloadSize)
	}
	expected := []byte{0xA1, 0xA2, 0xA3, 0xA4}
	for i := 0; i < 4; i++ {
		if cap.payloadBuf[i] != expected[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], expected[i])
		}
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: extent change between transfers
// =====================================================================================================================

// TestRxExtentChangeBetweenTransfers tests changing extent between two complete multiframe transfers.
func TestRxExtentChangeBetweenTransfers(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 1500, 5, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payload := []byte{0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1500, 10)

	// First transfer with extent=5. CRC over 8 payload + 4 padding.
	pad := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, pad)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 2)

	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[5] = crcHi
	frame2[6] = crcLo
	frame2[7] = rxEdgeMakeV1Tail(false, true, false, 2)

	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}
	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	if cap.payloadSize != 5 {
		t.Errorf("payloadSize = %d, want 5 (truncated to extent=5)", cap.payloadSize)
	}

	// Change extent to 100 and send a second transfer (TID=3).
	sub.Extent = 100
	cap.reset()

	frame3 := make([]byte, 8)
	copy(frame3, payload[:7])
	frame3[7] = rxEdgeMakeV1Tail(true, false, true, 3)

	frame4 := make([]byte, 8)
	frame4[0] = payload[7]
	frame4[5] = crcHi // Same CRC: same payload+padding.
	frame4[6] = crcLo
	frame4[7] = rxEdgeMakeV1Tail(false, true, false, 3)

	nowVal = 1000
	if !self.IngestFrame(1000, 0, canID, frame3) {
		t.Fatal("IngestFrame frame3 failed")
	}
	if !self.IngestFrame(1100, 0, canID, frame4) {
		t.Fatal("IngestFrame frame4 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	// With extent=100, all 12 bytes (8 payload + 4 padding) are delivered: min(14-2, 100) = 12.
	if cap.payloadSize != 12 {
		t.Errorf("payloadSize = %d, want 12", cap.payloadSize)
	}
	for i := 0; i < 8; i++ {
		if cap.payloadBuf[i] != payload[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], payload[i])
		}
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: extent shrink during inflight
// =====================================================================================================================

// TestRxExtentShrinkDuringInflight tests shrinking extent while a multiframe transfer is in flight.
func TestRxExtentShrinkDuringInflight(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 1600, 100, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payload := []byte{0xB0, 0xB1, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6, 0xB7}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1600, 10)

	pad := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, pad)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 4)

	// Feed frame 1, then shrink extent.
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}

	sub.Extent = 4 // Shrink from 100 to 4.

	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[5] = crcHi
	frame2[6] = crcLo
	frame2[7] = rxEdgeMakeV1Tail(false, true, false, 4)

	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	// The slot was allocated with extent=100. Truncation: min(14-2, 100) = 12.
	if cap.payloadSize != 12 {
		t.Errorf("payloadSize = %d, want 12", cap.payloadSize)
	}
	for i := 0; i < 8; i++ {
		if cap.payloadBuf[i] != payload[i] {
			t.Errorf("payloadBuf[%d] = 0x%02X, want 0x%02X", i, cap.payloadBuf[i], payload[i])
		}
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: extent grow during inflight
// =====================================================================================================================

// TestRxExtentGrowDuringInflight tests growing extent while a multiframe transfer is in flight.
func TestRxExtentGrowDuringInflight(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxEdgeCanard(self, &nowVal, 42)

	cap := &rxEdgeCapture{}
	sub := &Subscription{}
	if got := self.Subscribe16b(sub, 1700, 5, 2000000, rxEdgeCaptureSubVTable); got != sub {
		t.Errorf("Subscribe16b returned %p, want %p", got, sub)
	}
	sub.UserContext = cap

	payload := []byte{0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7}
	canID := rxEdgeMakeV1V1MsgCANID(PrioNominal, 1700, 10)

	pad := []byte{0, 0, 0, 0}
	crc := rxEdgeCRC16CCITT(crcInitial, payload)
	crc = rxEdgeCRC16CCITT(crc, pad)
	crcHi := uint8((uint(crc) >> 8) & 0xFF)
	crcLo := uint8(crc & 0xFF)

	frame1 := make([]byte, 8)
	copy(frame1, payload[:7])
	frame1[7] = rxEdgeMakeV1Tail(true, false, true, 5)

	// Feed frame 1, then grow extent.
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame1) {
		t.Fatal("IngestFrame frame1 failed")
	}

	sub.Extent = 256 // Grow from 5 to 256.

	frame2 := make([]byte, 8)
	frame2[0] = payload[7]
	frame2[5] = crcHi
	frame2[6] = crcLo
	frame2[7] = rxEdgeMakeV1Tail(false, true, false, 5)

	if !self.IngestFrame(200, 0, canID, frame2) {
		t.Fatal("IngestFrame frame2 failed")
	}
	if cap.count != 1 {
		t.Errorf("count = %d, want 1", cap.count)
	}
	// The slot was allocated with extent=5. Truncation: min(14-2, 5) = 5.
	if cap.payloadSize != 5 {
		t.Errorf("payloadSize = %d, want 5", cap.payloadSize)
	}
	if cap.payloadBuf[0] != 0xC0 {
		t.Errorf("payloadBuf[0] = 0x%02X, want 0xC0", cap.payloadBuf[0])
	}
	if cap.payloadBuf[4] != 0xC4 {
		t.Errorf("payloadBuf[4] = 0x%02X, want 0xC4", cap.payloadBuf[4])
	}

	self.Unsubscribe(sub)
	self.Destroy()
}
