// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// a compact implementation of the Cyphal/CAN transport for high-integrity real-time embedded systems.
//
// The library offers a non-blocking, callback-based API. It is not safe for concurrent use; if used in a
// concurrent environment it is the responsibility of the application to provide adequate synchronization.
//
// The original library is distributed under the terms of the MIT License. Copyright (c) OpenCyphal.

package libcanard

// This file migrates tests/src/test_api_rx.cpp to Go.
//
// It is an in-package white-box test that exercises the RX API: subscription management,
// message reception, service requests/responses, unsubscribe behavior, and v0 compatibility.
//
// See CONVERT_CPP.MD for the conversion guidelines.

import (
	"testing"
)

// =====================================================================================================================
// Infrastructure
// =====================================================================================================================

// rxTestCapture mirrors the C++ rx_capture_t.
type rxTestCapture struct {
	count        int
	timestamp    int64
	priority     Prio
	sourceNodeID uint8
	transferID   uint8
	payloadSize  int
	payloadBuf   [256]byte
}

func (cap *rxTestCapture) reset() {
	cap.count = 0
	cap.timestamp = 0
	cap.priority = 0
	cap.sourceNodeID = 0
	cap.transferID = 0
	cap.payloadSize = 0
	cap.payloadBuf = [256]byte{}
}

// rxTestCaptureOnMessage mirrors capture_on_message.
func rxTestCaptureOnMessage(self *Subscription, timestamp int64, priority Prio, sourceNodeID uint8, transferID uint8, payload Payload) {
	cap := self.UserContext.(*rxTestCapture)
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

var rxTestCaptureSubVTable = &SubscriptionVTable{OnMessage: rxTestCaptureOnMessage}

// rxTestSimpleCapture mirrors capture_on_message_simple.
type rxTestSimpleCapture struct {
	count        int
	sourceNodeID uint8
	transferID   uint8
	payloadSize  int
}

func rxTestSimpleCaptureOnMessage(self *Subscription, _ int64, _ Prio, sourceNodeID uint8, transferID uint8, payload Payload) {
	cap := self.UserContext.(*rxTestSimpleCapture)
	cap.count++
	cap.sourceNodeID = sourceNodeID
	cap.transferID = transferID
	cap.payloadSize = payload.View.Size
}

var rxTestSimpleSubVTable = &SubscriptionVTable{OnMessage: rxTestSimpleCaptureOnMessage}

// rxTestUnsubscribeCapture for the CN-04 regression test.
type rxTestUnsubscribeCapture struct {
	count int
}

func rxTestUnsubscribeOnMessage(self *Subscription, _ int64, _ Prio, _ uint8, _ uint8, payload Payload) {
	cap := self.UserContext.(*rxTestUnsubscribeCapture)
	cap.count++
	// single-frame transfer: no owned storage
	if payload.Origin.Size > 0 && payload.Origin.Data != nil {
		// This should not happen for single-frame
		t := &testing.T{}
		t.Errorf("single-frame transfer should have Origin.Size == 0")
	}
	// Unsubscribe from within the callback (CN-04 regression test)
	self.Owner.Unsubscribe(self)
}

var rxTestUnsubscribeSubVTable = &SubscriptionVTable{OnMessage: rxTestUnsubscribeOnMessage}

// mockNow for RX tests - returns the value stored in self.UserContext (*int64).
func rxTestMockNow(self *Canard) int64 {
	if self.UserContext != nil {
		return *(self.UserContext.(*int64))
	}
	return 0
}

func rxTestMockTX(_ *Canard, _ any, _ int64, _ uint8, _ bool, _ uint32, _ []byte) bool {
	return false
}

var rxTestVTable = &VTable{Now: rxTestMockNow, TX: rxTestMockTX, Filter: nil}

// initRxTestCanard initializes a Canard instance for RX testing with a controllable clock.
func initRxTestCanard(self *Canard, nowVal *int64, nodeID uint8) {
	*nowVal = 0
	if !self.Init(rxTestVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 1234, 0) {
		panic("Init failed")
	}
	if !self.SetNodeID(nodeID) {
		panic("SetNodeID failed")
	}
	self.UserContext = nowVal
}

// =====================================================================================================================
// CAN Frame Construction Helpers
// =====================================================================================================================

// v1.1 message: priority[28:26] | subject_id[25:8] | bit7=1(v1.1) | src[6:0]
func rxTestMakeV1V1MsgCANID(prio Prio, subjectID uint16, src uint8) uint32 {
	return (uint32(prio) << prioShift) | (uint32(subjectID) << 8) | (1 << 7) | (uint32(src) & 0x7F)
}

// v1.0 service: priority[28:26] | bit25=1(svc) | rnr[24] | service_id[23:14] | dst[13:7] | src[6:0]
func rxTestMakeV1SvcCANID(prio Prio, serviceID uint16, requestNotResponse bool, dst uint8, src uint8) uint32 {
	return (uint32(prio) << prioShift) | (1 << 25) |
		(boolToUint32(requestNotResponse) << 24) | (uint32(serviceID) << 14) | (uint32(dst) << 7) | (uint32(src) & 0x7F)
}

// Single-frame tail byte for v1: start=1, end=1, toggle=1 (v1 starts toggle=1).
func rxTestMakeV1SingleTail(tid uint8) uint8 {
	return uint8(tailSOT | tailEOT | tailToggle | (tid & transferIDMax))
}

// v0 message: priority[28:26] | data_type_id[23:8] | bit7=0 | src[6:0]
func rxTestMakeV0MsgCANID(prio Prio, dataTypeID uint16, src uint8) uint32 {
	return (uint32(prio) << prioShift) | (uint32(dataTypeID) << 8) | (uint32(src) & 0x7F)
}

// v0 service: priority[28:26] | data_type_id[23:16] | rnr[15] | dst[14:8] | bit7=1(svc) | src[6:0]
func rxTestMakeV0SvcCANID(prio Prio, dataTypeID uint8, requestNotResponse bool, dst uint8, src uint8) uint32 {
	return (uint32(prio) << prioShift) | (uint32(dataTypeID) << 16) |
		(boolToUint32(requestNotResponse) << 15) | (uint32(dst) << 8) | (1 << 7) | (uint32(src) & 0x7F)
}

// Single-frame tail byte for v0: start=1, end=1, toggle=0 (v0 starts toggle=0).
func rxTestMakeV0SingleTail(tid uint8) uint8 {
	return uint8(tailSOT | tailEOT | (tid & transferIDMax))
}

// =====================================================================================================================
// Argument Validation Tests
// =====================================================================================================================

func TestSubscribeNullArgs(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)
	sub := &Subscription{}

	// NULL self.
	if c := self.Subscribe16b(nil, 100, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != nil {
		t.Error("Subscribe16b with nil self should return nil")
	}
	// NULL sub.
	if c := self.Subscribe16b(sub, 100, 64, DefaultTransferIDTimeoutUs, nil); c != nil {
		t.Error("Subscribe16b with nil sub should return nil")
	}
	// NULL vtable.
	if c := self.Subscribe16b(sub, 100, 64, DefaultTransferIDTimeoutUs, &SubscriptionVTable{}); c != nil {
		t.Error("Subscribe16b with nil OnMessage should return nil")
	}

	self.Destroy()
}

func TestSubscribe13bNullArgs(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)
	sub := &Subscription{}

	// NULL self.
	if c := self.Subscribe13b(nil, 100, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != nil {
		t.Error("Subscribe13b with nil self should return nil")
	}
	// NULL sub.
	if c := self.Subscribe13b(sub, 100, 64, DefaultTransferIDTimeoutUs, nil); c != nil {
		t.Error("Subscribe13b with nil sub should return nil")
	}
	// NULL vtable.
	if c := self.Subscribe13b(sub, 100, 64, DefaultTransferIDTimeoutUs, &SubscriptionVTable{}); c != nil {
		t.Error("Subscribe13b with nil OnMessage should return nil")
	}

	self.Destroy()
}

func TestSubscribeRequestNullArgs(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)
	sub := &Subscription{}

	// NULL self.
	if c := self.SubscribeRequest(nil, 10, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != nil {
		t.Error("SubscribeRequest with nil self should return nil")
	}
	// NULL sub.
	if c := self.SubscribeRequest(sub, 10, 64, DefaultTransferIDTimeoutUs, nil); c != nil {
		t.Error("SubscribeRequest with nil sub should return nil")
	}
	// NULL vtable.
	if c := self.SubscribeRequest(sub, 10, 64, DefaultTransferIDTimeoutUs, &SubscriptionVTable{}); c != nil {
		t.Error("SubscribeRequest with nil OnMessage should return nil")
	}

	self.Destroy()
}

func TestSubscribeResponseNullArgs(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)
	sub := &Subscription{}

	// NULL self.
	if c := self.SubscribeResponse(nil, 10, 64, rxTestCaptureSubVTable); c != nil {
		t.Error("SubscribeResponse with nil self should return nil")
	}
	// NULL sub.
	if c := self.SubscribeResponse(sub, 10, 64, nil); c != nil {
		t.Error("SubscribeResponse with nil sub should return nil")
	}
	// NULL vtable.
	if c := self.SubscribeResponse(sub, 10, 64, &SubscriptionVTable{}); c != nil {
		t.Error("SubscribeResponse with nil OnMessage should return nil")
	}

	self.Destroy()
}

func TestSubscribePortIDRange(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	// v1.0 subject must be <= 8191.
	sub := &Subscription{}
	if c := self.Subscribe13b(sub, 8192, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != nil {
		t.Error("Subscribe13b with subject 8192 should return nil")
	}
	if c := self.Subscribe13b(sub, 8191, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub {
		t.Error("Subscribe13b with subject 8191 should succeed")
	}
	self.Unsubscribe(sub)

	// v1.1 subject: full 16-bit range is valid.
	sub2 := &Subscription{}
	if c := self.Subscribe16b(sub2, 0xFFFF, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub2 {
		t.Error("Subscribe16b with subject 0xFFFF should succeed")
	}
	self.Unsubscribe(sub2)

	// Service ID must be <= 511.
	sub3 := &Subscription{}
	if c := self.SubscribeRequest(sub3, 512, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != nil {
		t.Error("SubscribeRequest with service 512 should return nil")
	}
	if c := self.SubscribeRequest(sub3, 511, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub3 {
		t.Error("SubscribeRequest with service 511 should succeed")
	}
	self.Unsubscribe(sub3)

	sub4 := &Subscription{}
	if c := self.SubscribeResponse(sub4, 512, 64, rxTestCaptureSubVTable); c != nil {
		t.Error("SubscribeResponse with service 512 should return nil")
	}
	if c := self.SubscribeResponse(sub4, 511, 64, rxTestCaptureSubVTable); c != sub4 {
		t.Error("SubscribeResponse with service 511 should succeed")
	}
	self.Unsubscribe(sub4)

	self.Destroy()
}

// =====================================================================================================================
// Subscription Management Tests
// =====================================================================================================================

func TestSubscribeDuplicateRejection(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	sub1 := &Subscription{}
	sub2 := &Subscription{}
	if c := self.Subscribe16b(sub1, 100, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub1 {
		t.Fatal("first Subscribe16b should return sub1")
	}
	// Same subject-ID, same kind: must return the incumbent.
	if c := self.Subscribe16b(sub2, 100, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub1 {
		t.Error("duplicate Subscribe16b should return incumbent sub1")
	}
	if c := self.FindSubscription(KindMessage16b, 100); c != sub1 {
		t.Error("FindSubscription should return sub1")
	}
	self.Unsubscribe(sub1)

	// Same port-ID but different kind: must succeed (message vs request use separate trees).
	subMsg := &Subscription{}
	subReq := &Subscription{}
	if c := self.Subscribe16b(subMsg, 100, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != subMsg {
		t.Fatal("Subscribe16b for message should succeed")
	}
	if c := self.SubscribeRequest(subReq, 100, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != subReq {
		t.Fatal("SubscribeRequest for same port should succeed")
	}
	self.Unsubscribe(subReq)
	self.Unsubscribe(subMsg)

	self.Destroy()
}

func TestFindSubscription(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	// NULL self.
	if c := self.FindSubscription(KindMessage16b, 100); c != nil {
		t.Error("FindSubscription with nil self should return nil")
	}
	// Non-existent subscription.
	if c := self.FindSubscription(KindMessage13b, 8192); c != nil {
		t.Error("FindSubscription for non-existent should return nil")
	}
	if c := self.FindSubscription(KindRequest, 512); c != nil {
		t.Error("FindSubscription for non-existent should return nil")
	}
	if c := self.FindSubscription(KindV0Request, 256); c != nil {
		t.Error("FindSubscription for non-existent should return nil")
	}
	if c := self.FindSubscription(KindMessage16b, 100); c != nil {
		t.Error("FindSubscription for non-existent should return nil")
	}

	subMsg := &Subscription{}
	subReq := &Subscription{}
	subV0 := &Subscription{}
	if c := self.Subscribe16b(subMsg, 100, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != subMsg {
		t.Fatal("Subscribe16b failed")
	}
	if c := self.SubscribeRequest(subReq, 100, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != subReq {
		t.Fatal("SubscribeRequest failed")
	}
	if c := self.V0Subscribe(subV0, 100, 0x1234, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != subV0 {
		t.Fatal("V0Subscribe failed")
	}

	if c := self.FindSubscription(KindMessage16b, 100); c != subMsg {
		t.Error("FindSubscription for message 16b should return subMsg")
	}
	if c := self.FindSubscription(KindRequest, 100); c != subReq {
		t.Error("FindSubscription for request should return subReq")
	}
	if c := self.FindSubscription(KindV0Message, 100); c != subV0 {
		t.Error("FindSubscription for v0 message should return subV0")
	}
	if c := self.FindSubscription(KindResponse, 100); c != nil {
		t.Error("FindSubscription for response should return nil")
	}

	self.Unsubscribe(subV0)
	self.Unsubscribe(subReq)
	self.Unsubscribe(subMsg)

	if c := self.FindSubscription(KindMessage16b, 100); c != nil {
		t.Error("FindSubscription after unsubscribe should return nil")
	}

	self.Destroy()
}

func TestSubscribeUnsubscribeResubscribe(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	sub := &Subscription{}
	if c := self.Subscribe16b(sub, 200, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub {
		t.Fatal("Subscribe16b failed")
	}
	self.Unsubscribe(sub)
	// Re-subscribe to the same subject must succeed.
	if c := self.Subscribe16b(sub, 200, 64, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub {
		t.Error("Re-subscribe should return sub")
	}
	self.Unsubscribe(sub)

	self.Destroy()
}

// =====================================================================================================================
// End-to-End Message Reception Tests
// =====================================================================================================================

func TestV1V1MessageReception(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	cap := &rxTestCapture{}
	sub := &Subscription{}
	if c := self.Subscribe16b(sub, 1234, 256, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = cap

	// Construct a single-frame v1.1 message from node 10, priority nominal, transfer-ID 7.
	canID := rxTestMakeV1V1MsgCANID(PrioNominal, 1234, 10)
	// Payload: {0xDE, 0xAD} + tail byte.
	frame := []byte{0xDE, 0xAD, rxTestMakeV1SingleTail(7)}

	nowVal = 1000
	if !self.IngestFrame(1000, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}

	if cap.count != 1 {
		t.Fatalf("cap.count = %d, want 1", cap.count)
	}
	if cap.timestamp != 1000 {
		t.Errorf("timestamp = %d, want 1000", cap.timestamp)
	}
	if cap.priority != PrioNominal {
		t.Errorf("priority = %v, want PrioNominal", cap.priority)
	}
	if cap.sourceNodeID != 10 {
		t.Errorf("sourceNodeID = %d, want 10", cap.sourceNodeID)
	}
	if cap.transferID != 7 {
		t.Errorf("transferID = %d, want 7", cap.transferID)
	}
	if cap.payloadSize != 2 {
		t.Errorf("payloadSize = %d, want 2", cap.payloadSize)
	}
	if cap.payloadBuf[0] != 0xDE {
		t.Errorf("payloadBuf[0] = 0x%02X, want 0xDE", cap.payloadBuf[0])
	}
	if cap.payloadBuf[1] != 0xAD {
		t.Errorf("payloadBuf[1] = 0x%02X, want 0xAD", cap.payloadBuf[1])
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

func TestServiceRequestReception(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	cap := &rxTestCapture{}
	sub := &Subscription{}
	if c := self.SubscribeRequest(sub, 100, 256, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub {
		t.Fatal("SubscribeRequest failed")
	}
	sub.UserContext = cap

	// Service request from node 10 to node 42 (us), service-ID 100, transfer-ID 3.
	canID := rxTestMakeV1SvcCANID(PrioNominal, 100, true, 42, 10)
	frame := []byte{0xCA, 0xFE, rxTestMakeV1SingleTail(3)}

	nowVal = 2000
	if !self.IngestFrame(2000, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}

	if cap.count != 1 {
		t.Fatalf("cap.count = %d, want 1", cap.count)
	}
	if cap.sourceNodeID != 10 {
		t.Errorf("sourceNodeID = %d, want 10", cap.sourceNodeID)
	}
	if cap.transferID != 3 {
		t.Errorf("transferID = %d, want 3", cap.transferID)
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

	self.Unsubscribe(sub)
	self.Destroy()
}

func TestServiceResponseReception(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	cap := &rxTestCapture{}
	sub := &Subscription{}
	if c := self.SubscribeResponse(sub, 200, 256, rxTestCaptureSubVTable); c != sub {
		t.Fatal("SubscribeResponse failed")
	}
	sub.UserContext = cap

	// Service response from node 99 to node 42 (us), service-ID 200, transfer-ID 5.
	canID := rxTestMakeV1SvcCANID(PrioNominal, 200, false, 42, 99)
	frame := []byte{0xBE, 0xEF, rxTestMakeV1SingleTail(5)}

	nowVal = 3000
	if !self.IngestFrame(3000, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}

	if cap.count != 1 {
		t.Fatalf("cap.count = %d, want 1", cap.count)
	}
	if cap.sourceNodeID != 99 {
		t.Errorf("sourceNodeID = %d, want 99", cap.sourceNodeID)
	}
	if cap.transferID != 5 {
		t.Errorf("transferID = %d, want 5", cap.transferID)
	}
	if cap.payloadSize != 2 {
		t.Errorf("payloadSize = %d, want 2", cap.payloadSize)
	}
	if cap.payloadBuf[0] != 0xBE {
		t.Errorf("payloadBuf[0] = 0x%02X, want 0xBE", cap.payloadBuf[0])
	}
	if cap.payloadBuf[1] != 0xEF {
		t.Errorf("payloadBuf[1] = 0x%02X, want 0xEF", cap.payloadBuf[1])
	}

	self.Unsubscribe(sub)
	self.Destroy()
}

// =====================================================================================================================
// Unsubscribe Behavior Tests
// =====================================================================================================================

func TestUnsubscribeStopsDelivery(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	cap := &rxTestCapture{}
	sub := &Subscription{}
	if c := self.Subscribe16b(sub, 500, 256, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = cap

	// First frame: should be delivered.
	canID := rxTestMakeV1V1MsgCANID(PrioNominal, 500, 10)
	frame := []byte{0xAA, rxTestMakeV1SingleTail(0)}
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame) {
		t.Fatal("IngestFrame (first) failed")
	}
	if cap.count != 1 {
		t.Fatalf("cap.count = %d, want 1", cap.count)
	}

	// Unsubscribe, then ingest again: callback must NOT fire.
	self.Unsubscribe(sub)
	cap.reset()
	if !self.IngestFrame(200, 0, canID, frame) {
		t.Fatal("IngestFrame (second) failed")
	}
	if cap.count != 0 {
		t.Errorf("cap.count = %d, want 0 (unsubscribed)", cap.count)
	}

	self.Destroy()
}

func TestMultipleSubscriptionsRouting(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	capA := &rxTestSimpleCapture{}
	capB := &rxTestSimpleCapture{}

	subA := &Subscription{}
	subB := &Subscription{}
	if c := self.Subscribe16b(subA, 300, 256, DefaultTransferIDTimeoutUs, rxTestSimpleSubVTable); c != subA {
		t.Fatal("Subscribe16b A failed")
	}
	subA.UserContext = capA
	if c := self.Subscribe16b(subB, 400, 256, DefaultTransferIDTimeoutUs, rxTestSimpleSubVTable); c != subB {
		t.Fatal("Subscribe16b B failed")
	}
	subB.UserContext = capB

	// Message for subject 300.
	canID := rxTestMakeV1V1MsgCANID(PrioNominal, 300, 10)
	frame := []byte{0x11, rxTestMakeV1SingleTail(1)}
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame) {
		t.Fatal("IngestFrame (subject 300) failed")
	}

	// Message for subject 400.
	canID = rxTestMakeV1V1MsgCANID(PrioNominal, 400, 20)
	frame = []byte{0x22, rxTestMakeV1SingleTail(2)}
	nowVal = 200
	if !self.IngestFrame(200, 0, canID, frame) {
		t.Fatal("IngestFrame (subject 400) failed")
	}

	if capA.count != 1 {
		t.Fatalf("capA.count = %d, want 1", capA.count)
	}
	if capA.sourceNodeID != 10 {
		t.Errorf("capA.sourceNodeID = %d, want 10", capA.sourceNodeID)
	}
	if capA.transferID != 1 {
		t.Errorf("capA.transferID = %d, want 1", capA.transferID)
	}

	if capB.count != 1 {
		t.Fatalf("capB.count = %d, want 1", capB.count)
	}
	if capB.sourceNodeID != 20 {
		t.Errorf("capB.sourceNodeID = %d, want 20", capB.sourceNodeID)
	}
	if capB.transferID != 2 {
		t.Errorf("capB.transferID = %d, want 2", capB.transferID)
	}

	self.Unsubscribe(subB)
	self.Unsubscribe(subA)
	self.Destroy()
}

func TestUnsubscribeCleansUpSessions(t *testing.T) {
	self := &Canard{}
	nowVal := int64(0)
	initRxTestCanard(self, &nowVal, 42)

	cap := &rxTestCapture{}
	sub := &Subscription{}
	if c := self.Subscribe16b(sub, 600, 256, DefaultTransferIDTimeoutUs, rxTestCaptureSubVTable); c != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = cap

	// Receive messages from two different remote nodes to create two sessions.
	for src := uint8(10); src <= 11; src++ {
		canID := rxTestMakeV1V1MsgCANID(PrioNominal, 600, src)
		frame := []byte{0xFF, rxTestMakeV1SingleTail(0)}
		nowVal = 100
		if !self.IngestFrame(100, 0, canID, frame) {
			t.Fatal("IngestFrame failed")
		}
	}
	if cap.count != 2 {
		t.Fatalf("cap.count = %d, want 2", cap.count)
	}
	if sub.Sessions == nil {
		t.Error("Sessions should exist")
	}

	// Unsubscribe must destroy all sessions without leaking.
	self.Unsubscribe(sub)

	self.Destroy()
}

func TestUnsubscribeWithinCallback(t *testing.T) {
	// Regression (CN-04): unsubscribing the receiving subscription from within on_message must be safe.
	self := &Canard{}
	nowVal := int64(0)
	if !self.Init(rxTestVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 1234, 0) {
		t.Fatal("Init failed")
	}
	if !self.SetNodeID(42) {
		t.Fatal("SetNodeID failed")
	}
	self.UserContext = &nowVal

	cap := &rxTestUnsubscribeCapture{}
	sub := &Subscription{}
	if c := self.Subscribe16b(sub, 601, 64, DefaultTransferIDTimeoutUs, rxTestUnsubscribeSubVTable); c != sub {
		t.Fatal("Subscribe16b failed")
	}
	sub.UserContext = cap

	canID := rxTestMakeV1V1MsgCANID(PrioNominal, 601, 24)
	frame := []byte{0xAA, rxTestMakeV1SingleTail(0)}
	nowVal = 100
	if !self.IngestFrame(100, 0, canID, frame) {
		t.Fatal("IngestFrame failed")
	}

	if cap.count != 1 {
		t.Fatalf("cap.count = %d, want 1", cap.count)
	}
	if c := self.FindSubscription(KindMessage16b, 601); c != nil {
		t.Error("FindSubscription should return nil after unsubscribe from callback")
	}

	self.Destroy()
}

