// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// a compact implementation of the Cyphal/CAN transport for high-integrity real-time embedded systems.
//
// The library offers a non-blocking, callback-based API. It is not safe for concurrent use; if used in a
// concurrent environment it is the responsibility of the application to provide adequate synchronization.
//
// The original library is distributed under the terms of the MIT License. Copyright (c) OpenCyphal.

package libcanard

// This file migrates tests/src/test_api_tx.cpp to Go.
//
// It is an in-package white-box test that exercises the TX API: publish, request, respond,
// poll, interface availability, validation, OOM handling, and CAN-ID encoding.
//
// See CONVERT_CPP.MD for the conversion guidelines.

import (
	"testing"
)

// =====================================================================================================================
//                                         TX Capture Callback
// =====================================================================================================================

// txTestRecord mirrors the C++ tx_record_t.
type txTestRecord struct {
	deadline   int64
	ifaceIndex uint8
	fd         bool
	canID      uint32
	tail       uint8
}

// txTestCapture mirrors the C++ tx_capture_t.
type txTestCapture struct {
	now      int64
	acceptTX bool
	count    int
	records  [32]txTestRecord
}

func (cap *txTestCapture) reset() {
	cap.now = 0
	cap.acceptTX = true
	cap.count = 0
	cap.records = [32]txTestRecord{}
}

// txTestCaptureNow returns the controllable clock value stored in the instance user context.
func txTestCaptureNow(self *Canard) int64 {
	return self.UserContext.(*txTestCapture).now
}

// txTestCaptureTX records every transmitted frame into the capture structure stored in the instance user context.
func txTestCaptureTX(self *Canard, _ any, deadline int64, ifaceIndex uint8, fd bool, extendedCANID uint32, canData []byte) bool {
	cap := self.UserContext.(*txTestCapture)
	if cap.count < len(cap.records) {
		rec := &cap.records[cap.count]
		rec.deadline = deadline
		rec.ifaceIndex = ifaceIndex
		rec.fd = fd
		rec.canID = extendedCANID
		rec.tail = 0
		if len(canData) > 0 {
			rec.tail = canData[len(canData)-1]
		}
	}
	cap.count++
	return cap.acceptTX
}

var txTestCaptureVTable = &VTable{
	Now:    txTestCaptureNow,
	TX:     txTestCaptureTX,
	Filter: nil,
}

// =====================================================================================================================
//                                         Mock VTable
// =====================================================================================================================

// txTestMockNow returns 0.
func txTestMockNow(_ *Canard) int64 { return 0 }

// txTestMockTX always returns false.
func txTestMockTX(_ *Canard, _ any, _ int64, _ uint8, _ bool, _ uint32, _ []byte) bool { return false }

var txTestVTable = &VTable{
	Now:    txTestMockNow,
	TX:     txTestMockTX,
	Filter: nil,
}

// initTxAPITestCanard initializes a Canard instance for TX API testing with a capture.
func initTxAPITestCanard(self *Canard, cap *txTestCapture) {
	cap.reset()
	if !self.Init(txTestCaptureVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 1234, 0) {
		panic("Init failed")
	}
	if !self.SetNodeID(42) {
		panic("SetNodeID failed")
	}
	self.UserContext = cap
}

// initTxAPITestCanardNodeID initializes a Canard instance for TX API testing with a capture and specific node ID.
func initTxAPITestCanardNodeID(self *Canard, cap *txTestCapture, nodeID uint8) {
	cap.reset()
	if !self.Init(txTestCaptureVTable, NewDefaultMemSet(), IfaceBitmapAll, 16, 1234, 0) {
		panic("Init failed")
	}
	if !self.SetNodeID(nodeID) {
		panic("SetNodeID failed")
	}
	self.UserContext = cap
}

// =====================================================================================================================
//                                         Test: pending interface bitmap
// =====================================================================================================================

// TestCanardPendingIfaces tests the pending interface bitmap.
// Note: This test requires IfaceCount >= 2 to publish to interfaces 1 and 2.
func TestCanardPendingIfaces(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	mem := NewDefaultMemSet()
	if !self.Init(txTestVTable, mem, IfaceBitmapAll, 16, 1234, 0) {
		t.Fatal("Init failed")
	}

	// Empty payload
	payload := []byte{}
	if self.PendingIfaces() != 0 {
		t.Errorf("PendingIfaces = %d, want 0", self.PendingIfaces())
	}

	if !self.Publish16b(1000, 1, PrioNominal, 10, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if !self.Publish16b(1000, 2, PrioNominal, 11, 1, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.PendingIfaces() != 3 {
		t.Errorf("PendingIfaces = %d, want 3", self.PendingIfaces())
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test: interface availability masks (multi-iface)
// =====================================================================================================================

// TestTxIfaceAvailabilityMasksRequest tests that request is masked by interface availability.
// Note: This test is guarded with t.Skip if IfaceCount < 2.
func TestTxIfaceAvailabilityMasksRequest(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	if !self.Init(txTestVTable, NewDefaultMemSet(), 0b01, 16, 1234, 0) {
		t.Fatal("Init failed")
	}

	payload := []byte{}
	// canard_request targets ALL ifaces internally; availability masks it down to iface 0.
	if !self.Request(1000, PrioNominal, 5, 7, 0, payload, nil) {
		t.Fatal("Request failed")
	}
	if self.PendingIfaces() != 0b01 {
		t.Errorf("PendingIfaces = %d, want 0b01", self.PendingIfaces())
	}
	self.Destroy()
}

// TestTxIfaceAvailabilityPartialPublish tests partial publish masking.
// Note: This test is guarded with t.Skip if IfaceCount < 2.
func TestTxIfaceAvailabilityPartialPublish(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	if !self.Init(txTestVTable, NewDefaultMemSet(), 0b01, 16, 1234, 0) {
		t.Fatal("Init failed")
	}

	payload := []byte{}
	if !self.Publish16b(1000, 0b11, PrioNominal, 10, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}
	if self.PendingIfaces() != 0b01 {
		t.Errorf("PendingIfaces = %d, want 0b01", self.PendingIfaces())
	}
	self.Destroy()
}

// TestTxIfaceAvailabilityDisjointPublish tests disjoint publish rejection.
// Note: This test is guarded with t.Skip if IfaceCount < 2.
func TestTxIfaceAvailabilityDisjointPublish(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	if !self.Init(txTestVTable, NewDefaultMemSet(), 0b10, 16, 1234, 0) {
		t.Fatal("Init failed")
	}

	payload := []byte{}
	if self.Publish16b(1000, 0b01, PrioNominal, 10, 0, payload, nil) {
		t.Error("Publish16b should have failed (disjoint interfaces)")
	}
	if self.PendingIfaces() != 0 {
		t.Errorf("PendingIfaces = %d, want 0", self.PendingIfaces())
	}
	if self.tx.queueSize != 0 {
		t.Errorf("queueSize = %d, want 0", self.tx.queueSize)
	}
	self.Destroy()
}

// TestTxIfaceAvailabilityMasksV0Request tests v0 request masking.
// Note: This test is guarded with t.Skip if IfaceCount < 2.
func TestTxIfaceAvailabilityMasksV0Request(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	if !self.Init(txTestVTable, NewDefaultMemSet(), 0b01, 16, 1234, 0) {
		t.Fatal("Init failed")
	}
	if !self.SetNodeID(42) {
		t.Fatal("SetNodeID failed")
	}

	payload := []byte{}
	// canard_v0_request targets ALL ifaces internally; availability masks it down to iface 0.
	if !self.V0Request(1000, PrioNominal, 5, 0x1234, 7, 0, payload, nil) {
		t.Fatal("V0Request failed")
	}
	if self.PendingIfaces() != 0b01 {
		t.Errorf("PendingIfaces = %d, want 0b01", self.PendingIfaces())
	}
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: listen-only node
// =====================================================================================================================

// TestTxIfaceAvailabilityListenOnly tests that listen-only node drops every enqueue.
func TestTxIfaceAvailabilityListenOnly(t *testing.T) {
	self := &Canard{}
	if !self.Init(txTestVTable, NewDefaultMemSet(), 0, 16, 1234, 0) {
		t.Fatal("Init failed")
	}
	if !self.SetNodeID(42) {
		t.Fatal("SetNodeID failed")
	}

	payload := []byte{}
	if self.Publish16b(1000, IfaceBitmapAll, PrioNominal, 10, 0, payload, nil) {
		t.Error("Publish16b should have failed (listen-only node)")
	}
	if self.Request(1000, PrioNominal, 5, 7, 0, payload, nil) {
		t.Error("Request should have failed (listen-only node)")
	}
	if self.PendingIfaces() != 0 {
		t.Errorf("PendingIfaces = %d, want 0", self.PendingIfaces())
	}
	if self.tx.queueSize != 0 {
		t.Errorf("queueSize = %d, want 0", self.tx.queueSize)
	}
	if self.Err.TXCapacity != 0 {
		t.Errorf("TXCapacity errors = %d, want 0", self.Err.TXCapacity)
	}
	self.Destroy()
}

// =====================================================================================================================
//                                         Test: v0 CRC seed from data type signature
// =====================================================================================================================

// TestCanardV0CRCSeedFromDataTypeSignatureGolden tests v0 CRC seed computation with golden vectors.
func TestCanardV0CRCSeedFromDataTypeSignatureGolden(t *testing.T) {
	type testVector struct {
		signature    uint64
		expectedSeed uint16
	}
	vectors := []testVector{
		{0xD8A7486238EC3AF3, 0x5E37},
		{0x5E9BBA44FAF1EA04, 0x9A63},
		{0xE2A7D4A9460BC2F2, 0x037C},
		{0xB6AC0C442430297E, 0xBA25},
		{0x8280632C40E574B5, 0x4E3E},
		{0x72A63A3C6F41FA9B, 0x2055},
		{0xD5513C3F7AFAC74E, 0x94AD},
		{0x0A1892D72AB8945F, 0x02CF},
		{0xC77DF38BA122F5DA, 0x506E},
		{0x7B48E55FCFF42A57, 0xC02B},
		{0xCDC7C43412BDC89A, 0xEBF4},
		{0x49272A6477D96271, 0x17A4},
		{0x306F69E0A591AFAA, 0x472D},
		{0x4AF6E57B2B2BE29C, 0xCD1F},
		{0x9371428A92F01FD6, 0x87DB},
		{0xB9F127865BE0D61E, 0xEFD0},
		{0x70261C28A94144C6, 0x5D7E},
		{0xCE0F9F621CF7E70B, 0xF8B4},
		{0x217F5C87D7EC951D, 0xE4B8},
		{0xA9AF28AEA2FBB254, 0x1591},
		{0x9BE8BDC4C3DBBFD2, 0x24AE},
		{0x54C1572B9E07F297, 0x9510},
		{0xCA41E7000F37435F, 0xC798},
		{0x1F56030ECB171501, 0x897E},
		{0xA1A036268B0C3455, 0x3004},
		{0x624A519D42553D82, 0xAEE4},
		{0x286B4A387BA84BC4, 0xBF2E},
		{0xD38AA3EE75537EC6, 0x41D9},
		{0xBE9EA9FEC2B15D52, 0xDFBD},
		{0x2031D93C8BDD1EC4, 0x9E31},
		{0x249C26548A711966, 0xF674},
		{0x8313D33D0DDDA115, 0x2CB7},
		{0xBBA05074AD757480, 0x591F},
		{0x68FFFE70FC771952, 0x049C},
		{0x8700F375556A8003, 0x1890},
		{0x463B10CCCBE51C3D, 0x1D70},
	}

	for _, vector := range vectors {
		seed := CrcSeedFromDataTypeSignature(vector.signature)
		if seed != vector.expectedSeed {
			t.Errorf("CrcSeedFromDataTypeSignature(0x%016X) = 0x%04X, want 0x%04X",
				vector.signature, seed, vector.expectedSeed)
		}
	}
}

// =====================================================================================================================
//                                         Test: publish validation
// =====================================================================================================================

// TestCanardPublishValidation tests publish argument validation.
func TestCanardPublishValidation(t *testing.T) {
	self := &Canard{}

	// Invalid interface bitmap (self not initialized).
	payload := []byte{}
	if self.Publish16b(0, 0, PrioNominal, 0, 0, payload, nil) {
		t.Error("Publish16b with invalid self should fail")
	}

	// Note: The C++ test also checks for payload with size>0 and data==nullptr,
	// but in Go a non-empty []byte always has a non-nil backing array, so this case
	// cannot be represented. This validation path is always satisfied in Go.
}

// =====================================================================================================================
//                                         Test: publish OOM
// =====================================================================================================================

// TestCanardPublishOOM tests publish with out-of-memory condition.
// Note: In the Go port, the fixed-size TX transfer objects are allocated by the Go runtime,
// so a hard OOM path is not directly observable. We test with an invalid memory resource
// (nil vtable) which causes Init to fail.
func TestCanardPublishOOM(t *testing.T) {
	self := &Canard{}
	mem := NewDefaultMemSet()
	// Make the TX transfer and frame allocators invalid
	mem.TXTransfer.VTable = nil
	mem.TXFrame.VTable = nil

	if self.Init(txTestVTable, mem, IfaceBitmapAll, 16, 1234, 0) {
		t.Fatal("Init with invalid memory should fail")
	}

	// Without successful Init, self.VTable is nil, so any publish would panic.
	// This is expected behavior - the instance must be initialized before use.
	// We verify that Init correctly rejected the invalid memory configuration.
}

// =====================================================================================================================
//                                         Test: v0 publish requires node ID
// =====================================================================================================================

// TestCanardV0PublishRequiresNodeID tests that v0 publish requires a valid node ID.
func TestCanardV0PublishRequiresNodeID(t *testing.T) {
	self := &Canard{}

	// Node-ID zero should reject the request.
	payload := []byte{}
	if self.V0Publish(0, 1, PrioNominal, 1, 0xFFFF, 0, payload, nil) {
		t.Error("V0Publish with node-ID=0 should fail")
	}
}

// =====================================================================================================================
//                                         Test: publish max subject ID encoding
// =====================================================================================================================

// TestCanardPublishMaxSubjectIDEncoding tests CAN-ID encoding for max subject ID.
func TestCanardPublishMaxSubjectIDEncoding(t *testing.T) {
	self := &Canard{}
	cap := &txTestCapture{}
	initTxAPITestCanard(self, cap)

	payload := []byte{}
	if !self.Publish16b(1000, 1, PrioNominal, SubjectIDMax, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count = %d, want 1", cap.count)
	}

	canID := cap.records[0].canID
	// Check CAN-ID fields
	if (canID>>26)&7 != uint32(PrioNominal) {
		t.Errorf("priority = %d, want %d", (canID>>26)&7, PrioNominal)
	}
	if (canID>>25)&1 != 0 {
		t.Error("service bit should be 0")
	}
	if (canID>>24)&1 != 0 {
		t.Error("reserved bit should be 0")
	}
	if (canID>>8)&0xFFFF != uint32(SubjectIDMax) {
		t.Errorf("subject_id = 0x%04X, want 0x%04X", (canID>>8)&0xFFFF, SubjectIDMax)
	}
	if (canID>>7)&1 != 1 {
		t.Error("v1.1 message marker bit should be 1")
	}
	if canID&0x7F != 42 {
		t.Errorf("source node ID = %d, want 42", canID&0x7F)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test: poll ready bitmap
// =====================================================================================================================

// TestCanardPollReadyBitmap tests that poll only drives interfaces marked writable.
// Note: This test requires IfaceCount >= 2 to publish to interface bitmap 3.
func TestCanardPollReadyBitmap(t *testing.T) {
	if IfaceCount < 2 {
		t.Skip("requires IfaceCount >= 2 (Go port is built with IfaceCount=1)")
	}

	self := &Canard{}
	cap := &txTestCapture{}
	initTxAPITestCanard(self, cap)

	payload := []byte{}
	if !self.Publish16b(1000, 3, PrioNominal, 10, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count after first poll = %d, want 1", cap.count)
	}
	if cap.records[0].ifaceIndex != 0 {
		t.Errorf("ifaceIndex = %d, want 0", cap.records[0].ifaceIndex)
	}
	if cap.records[0].deadline != 1000 {
		t.Errorf("deadline = %d, want 1000", cap.records[0].deadline)
	}
	if !cap.records[0].fd {
		t.Error("fd should be true")
	}
	if self.PendingIfaces() != 2 {
		t.Errorf("PendingIfaces = %d, want 2", self.PendingIfaces())
	}

	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count after second poll = %d, want 1", cap.count)
	}
	if self.PendingIfaces() != 2 {
		t.Errorf("PendingIfaces = %d, want 2", self.PendingIfaces())
	}

	self.Poll(2)
	if cap.count != 2 {
		t.Fatalf("count after third poll = %d, want 2", cap.count)
	}
	if cap.records[1].ifaceIndex != 1 {
		t.Errorf("ifaceIndex = %d, want 1", cap.records[1].ifaceIndex)
	}
	if cap.records[1].deadline != 1000 {
		t.Errorf("deadline = %d, want 1000", cap.records[1].deadline)
	}
	if !cap.records[1].fd {
		t.Error("fd should be true")
	}
	if self.PendingIfaces() != 0 {
		t.Errorf("PendingIfaces = %d, want 0", self.PendingIfaces())
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test: poll backpressure
// =====================================================================================================================

// TestCanardPollBackpressure tests that poll keeps pending frames if TX callback reports backpressure.
func TestCanardPollBackpressure(t *testing.T) {
	self := &Canard{}
	cap := &txTestCapture{}
	initTxAPITestCanard(self, cap)

	payload := []byte{}
	if !self.Publish16b(1000, 1, PrioNominal, 10, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	cap.acceptTX = false
	self.Poll(1)
	if cap.count != 1 {
		t.Fatalf("count = %d, want 1", cap.count)
	}
	if self.PendingIfaces() != 1 {
		t.Errorf("PendingIfaces = %d, want 1", self.PendingIfaces())
	}

	if cap.count > 0 {
		cap.acceptTX = true
	}
	self.Poll(1)
	if cap.count != 2 {
		t.Fatalf("count = %d, want 2", cap.count)
	}
	if self.PendingIfaces() != 0 {
		t.Errorf("PendingIfaces = %d, want 0", self.PendingIfaces())
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test: poll expiration
// =====================================================================================================================

// TestCanardPollExpiration tests that poll retires expired transfers before transmitting.
func TestCanardPollExpiration(t *testing.T) {
	self := &Canard{}
	cap := &txTestCapture{}
	initTxAPITestCanard(self, cap)

	payload := []byte{}
	if !self.Publish16b(10, 1, PrioNominal, 10, 0, payload, nil) {
		t.Fatal("Publish16b failed")
	}

	cap.now = 11
	self.Poll(1)
	if cap.count != 0 {
		t.Errorf("count = %d, want 0 (expired)", cap.count)
	}
	if self.PendingIfaces() != 0 {
		t.Errorf("PendingIfaces = %d, want 0", self.PendingIfaces())
	}
	if self.Err.TXExpiration != 1 {
		t.Errorf("TXExpiration errors = %d, want 1", self.Err.TXExpiration)
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test: request unicast model validation
// =====================================================================================================================

// TestCanardRequestUnicastModelValidation tests request argument validation.
func TestCanardRequestUnicastModelValidation(t *testing.T) {
	self := &Canard{}
	cap := &txTestCapture{}
	initTxAPITestCanard(self, cap)

	payload := []byte{}

	// Invalid destination (NodeIDMax+1 = 128, which is out of range)
	if self.Request(0, PrioNominal, ServiceIDMax, NodeIDMax+1, 0, payload, nil) {
		t.Error("Request with invalid destination should fail")
	}

	// Invalid service ID (ServiceIDMax+1 = 512, which is out of range)
	if self.Request(0, PrioNominal, ServiceIDMax+1, 1, 0, payload, nil) {
		t.Error("Request with invalid service ID should fail")
	}

	// Note: The C++ test also checks for payload with size>0 and data==nullptr,
	// but in Go this case cannot be represented (see CONVERT_CPP.MD section 8.3).

	self.Destroy()
}

// =====================================================================================================================
//                                         Test: request unicast model encoding and transfer ID
// =====================================================================================================================

// TestCanardRequestUnicastModelEncodingAndTransferID tests request CAN-ID encoding and transfer ID tracking.
func TestCanardRequestUnicastModelEncodingAndTransferID(t *testing.T) {
	self := &Canard{}
	cap := &txTestCapture{}
	initTxAPITestCanard(self, cap)

	// Track transfer IDs per destination
	transferIDByDestination := make([]uint8, NodeIDCapacity)
	transferIDByDestination[10] = TransferIDMax

	payload := []byte{}
	if !self.Request(1000, PrioHigh, ServiceIDMax, 10, transferIDByDestination[10], payload, nil) {
		t.Fatal("Request failed")
	}
	transferIDByDestination[10]++

	if !self.Request(1000, PrioHigh, ServiceIDMax, 10, transferIDByDestination[10], payload, nil) {
		t.Fatal("Request failed")
	}
	transferIDByDestination[10]++

	if !self.Request(1000, PrioHigh, ServiceIDMax, 11, transferIDByDestination[11], payload, nil) {
		t.Fatal("Request failed")
	}
	transferIDByDestination[11]++

	if (transferIDByDestination[10] & TransferIDMax) != 1 {
		t.Errorf("transferIDByDestination[10] = %d, want 1", transferIDByDestination[10]&TransferIDMax)
	}
	if (transferIDByDestination[11] & TransferIDMax) != 1 {
		t.Errorf("transferIDByDestination[11] = %d, want 1", transferIDByDestination[11]&TransferIDMax)
	}

	self.Poll(1)
	self.Poll(1)
	self.Poll(1)
	if cap.count != 3 {
		t.Fatalf("count = %d, want 3", cap.count)
	}

	expectedDest := []uint8{10, 10, 11}
	expectedTID := []uint8{TransferIDMax, 0, 0}

	for i := 0; i < 3; i++ {
		canID := cap.records[i].canID
		if cap.records[i].deadline != 1000 {
			t.Errorf("record[%d].deadline = %d, want 1000", i, cap.records[i].deadline)
		}
		if !cap.records[i].fd {
			t.Errorf("record[%d].fd should be true", i)
		}
		if (canID>>26)&7 != uint32(PrioHigh) {
			t.Errorf("record[%d] priority = %d, want %d", i, (canID>>26)&7, PrioHigh)
		}
		if (canID>>25)&1 != 1 {
			t.Errorf("record[%d] service bit should be 1", i)
		}
		if (canID>>24)&1 != 1 {
			t.Errorf("record[%d] request bit should be 1", i)
		}
		if (canID>>23)&1 != 0 {
			t.Errorf("record[%d] reserved bit should be 0", i)
		}
		if (canID>>14)&ServiceIDMax != ServiceIDMax {
			t.Errorf("record[%d] service_id = 0x%04X, want 0x%04X", i, (canID>>14)&ServiceIDMax, ServiceIDMax)
		}
		if uint8((canID>>7)&NodeIDMax) != expectedDest[i] {
			t.Errorf("record[%d] dest = %d, want %d", i, (canID>>7)&NodeIDMax, expectedDest[i])
		}
		if canID&NodeIDMax != 42 {
			t.Errorf("record[%d] source = %d, want 42", i, canID&NodeIDMax)
		}
		if (cap.records[i].tail & TransferIDMax) != expectedTID[i] {
			t.Errorf("record[%d] tail TID = %d, want %d", i, cap.records[i].tail&TransferIDMax, expectedTID[i])
		}
	}

	self.Destroy()
}

// =====================================================================================================================
//                                         Test: v1.0 service CAN-ID golden
// =====================================================================================================================

// TestCanard1v0ServiceCANIDGolden tests v1.0 service CAN-ID encoding against specification examples.
func TestCanard1v0ServiceCANIDGolden(t *testing.T) {
	payload := []byte{}

	// Test request from node 123 to node 42
	reqSelf := &Canard{}
	reqCap := &txTestCapture{}
	initTxAPITestCanardNodeID(reqSelf, reqCap, 123)

	if !reqSelf.Request(1000, PrioNominal, 430, 42, 1, payload, nil) {
		t.Fatal("Request failed")
	}
	reqSelf.Poll(1)
	if reqCap.count != 1 {
		t.Fatalf("reqCap.count = %d, want 1", reqCap.count)
	}
	if reqCap.records[0].canID != 0x136B957B {
		t.Errorf("request CAN-ID = 0x%08X, want 0x136B957B", reqCap.records[0].canID)
	}
	if !reqCap.records[0].fd {
		t.Error("fd should be true")
	}
	if reqCap.records[0].tail != 0xE1 {
		t.Errorf("tail = 0x%02X, want 0xE1", reqCap.records[0].tail)
	}
	reqSelf.Destroy()

	// Test response from node 42 to node 123
	resSelf := &Canard{}
	resCap := &txTestCapture{}
	initTxAPITestCanardNodeID(resSelf, resCap, 42)

	if !resSelf.Respond(1000, PrioNominal, 430, 123, 1, payload, nil) {
		t.Fatal("Respond failed")
	}
	resSelf.Poll(1)
	if resCap.count != 1 {
		t.Fatalf("resCap.count = %d, want 1", resCap.count)
	}
	if resCap.records[0].canID != 0x126BBDAA {
		t.Errorf("response CAN-ID = 0x%08X, want 0x126BBDAA", resCap.records[0].canID)
	}
	if !resCap.records[0].fd {
		t.Error("fd should be true")
	}
	if resCap.records[0].tail != 0xE1 {
		t.Errorf("tail = 0x%02X, want 0xE1", resCap.records[0].tail)
	}
	resSelf.Destroy()
}

// =====================================================================================================================
//                                         Test: v0 service node ID rule and encoding
// =====================================================================================================================

// TestCanardV0ServiceNodeIDRuleAndEncoding tests v0 service rules and CAN-ID encoding.
func TestCanardV0ServiceNodeIDRuleAndEncoding(t *testing.T) {
	payload := []byte{}

	// Node-ID zero should reject
	selfZero := &Canard{}
	if selfZero.V0Request(0, PrioNominal, 0x37, 0xBEEF, 24, 5, payload, nil) {
		t.Error("V0Request with node-ID=0 should fail")
	}
	if selfZero.V0Respond(0, PrioNominal, 0x37, 0xBEEF, 24, 6, payload, nil) {
		t.Error("V0Respond with node-ID=0 should fail")
	}

	self := &Canard{}
	cap := &txTestCapture{}
	initTxAPITestCanardNodeID(self, cap, 11)

	if !self.V0Request(1000, PrioNominal, 0x37, 0xBEEF, 24, 5, payload, nil) {
		t.Fatal("V0Request failed")
	}
	self.Poll(1)

	if !self.V0Respond(1000, PrioNominal, 0x37, 0xBEEF, 24, 6, payload, nil) {
		t.Fatal("V0Respond failed")
	}
	self.Poll(1)

	if cap.count != 2 {
		t.Fatalf("count = %d, want 2", cap.count)
	}
	if cap.records[0].canID != 0x1037988B {
		t.Errorf("request CAN-ID = 0x%08X, want 0x1037988B", cap.records[0].canID)
	}
	if cap.records[1].canID != 0x1037188B {
		t.Errorf("response CAN-ID = 0x%08X, want 0x1037188B", cap.records[1].canID)
	}
	if cap.records[0].fd {
		t.Error("request fd should be false (Classic CAN)")
	}
	if cap.records[1].fd {
		t.Error("response fd should be false (Classic CAN)")
	}
	if cap.records[0].tail != 0xC5 {
		t.Errorf("request tail = 0x%02X, want 0xC5", cap.records[0].tail)
	}
	if cap.records[1].tail != 0xC6 {
		t.Errorf("response tail = 0x%02X, want 0xC6", cap.records[1].tail)
	}

	self.Destroy()
}
