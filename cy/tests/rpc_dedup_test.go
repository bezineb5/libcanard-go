package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestRequesterReliableDedupRetransmit faithfully verifies the requester-side reliable-response
// deduplication, mirroring C cy.c handle_response_message + request_ack_admit/test:
//  1. First reliable response -> delivered to app exactly once, response ACK emitted.
//  2. A lost ACK causes the responder to retransmit the SAME seqno -> re-ACKed but NOT re-delivered.
//  3. Advancing the seqno frontier, then a too-old seqno (>= REQUEST_FUTURE_HISTORY below the top)
//     -> NACK, not delivered.
//  4. (separate phase) After the RequestFuture is destroyed, a retransmitted reliable response that
//     is still within history is still ACKed from the handed-off record, without the app seeing it.
//  5. (separate phase) An orphan reliable response with no live future and no retained record -> NACK.
func TestRequesterReliableDedupRetransmit(t *testing.T) {
	const (
		remoteID  = uint64(7)
		topicHash = uint64(0x1122334455667788)
	)

	reqPlat := NewMockPlatform()
	requester, err := cy.New(reqPlat, "requester", "", "")
	if err != nil {
		t.Fatalf("requester New: %v", err)
	}
	defer requester.Destroy()

	pub, err := requester.Advertise("dedup.service")
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	defer pub.Destroy()

	// deliver feeds a reliable response wire (given seqno) to the requester and returns the
	// requester's ACK/NACK wire emitted in response (reqPlat.ReceivedMessages tail).
	deliver := func(fut *cy.RequestFuture, seqno uint64) []byte {
		before := len(reqPlat.ReceivedMessages)
		hdr := cy.MarshalRSPHeader(true, byte(seqno), seqno, topicHash, fut.Tag())
		wire := append(hdr, 0xAB, 0xCD)
		ts := cy.NewMessageTS(cy.Microsecond(1000), cy.NewMessage(wire))
		requester.HandleMessage(cy.Lane{ID: remoteID}, nil, *ts)
		if len(reqPlat.ReceivedMessages) <= before {
			t.Fatalf("expected a response ACK/NACK after delivering seqno %d", seqno)
		}
		return reqPlat.ReceivedMessages[len(reqPlat.ReceivedMessages)-1].Data
	}
	ackType := func(wire []byte) cy.HeaderType {
		p, err := cy.ParseResponseHeader(wire)
		if err != nil {
			t.Fatalf("parse ack: %v", err)
		}
		return p.Type
	}

	// ---- Phase A: live future deduplication ----
	fut := requester.RPC().Request(pub, requester.Now()+1000000, 1000000, []byte{0x01})
	if fut == nil {
		t.Fatal("Request returned nil")
	}
	messageTag := fut.Tag()

	// 1. First reliable response (seqno 0) -> delivered once, ACK.
	if typ := ackType(deliver(fut, 0)); typ != cy.HeaderTypeRspAck {
		t.Fatalf("first response should be ACKed, got type %d", typ)
	}
	if fut.ResponseCount() != 1 {
		t.Fatalf("expected 1 delivered response, got %d", fut.ResponseCount())
	}

	// 2. Retransmit the same seqno 0 (lost ACK). Re-ACKed but NOT re-delivered.
	if typ := ackType(deliver(fut, 0)); typ != cy.HeaderTypeRspAck {
		t.Fatalf("retransmit of seqno 0 should be re-ACKed, got type %d", typ)
	}
	if fut.ResponseCount() != 1 {
		t.Fatalf("retransmit must NOT be re-delivered; got %d responses", fut.ResponseCount())
	}

	// 3. Advance the frontier to seqno 200 (fresh, delivered), then a too-old retransmit of
	//    seqno 0 (200 below the top, >= 192 history) -> NACK, not delivered.
	if typ := ackType(deliver(fut, 200)); typ != cy.HeaderTypeRspAck {
		t.Fatalf("frontier-advancing seqno 200 should be ACKed, got type %d", typ)
	}
	if fut.ResponseCount() != 2 {
		t.Fatalf("seqno 200 should be delivered; got %d", fut.ResponseCount())
	}
	if typ := ackType(deliver(fut, 0)); typ != cy.HeaderTypeRspNack {
		t.Fatalf("too-old seqno 0 should be NACKed, got type %d", typ)
	}
	if fut.ResponseCount() != 2 {
		t.Fatalf("NACKed too-old response must not be delivered; got %d", fut.ResponseCount())
	}
	fut.Destroy()

	// 5. Orphan reliable response on a brand-new message_tag -> NACK.
	orphanHdr := cy.MarshalRSPHeader(true, 0, 0, topicHash, messageTag+999)
	orphanWire := append(orphanHdr, 0xDE, 0xAD)
	beforeOrphan := len(reqPlat.ReceivedMessages)
	ots := cy.NewMessageTS(cy.Microsecond(2000), cy.NewMessage(orphanWire))
	requester.HandleMessage(cy.Lane{ID: remoteID}, nil, *ots)
	if len(reqPlat.ReceivedMessages) <= beforeOrphan {
		t.Fatal("expected an orphan NACK")
	}
	if typ := ackType(reqPlat.ReceivedMessages[len(reqPlat.ReceivedMessages)-1].Data); typ != cy.HeaderTypeRspNack {
		t.Fatalf("orphan reliable response should be NACKed, got type %d", typ)
	}

	// ---- Phase B: post-destroy retained record still answers in-history retransmits ----
	fut2 := requester.RPC().Request(pub, requester.Now()+1000000, 1000000, []byte{0x03})
	if fut2 == nil {
		t.Fatal("Request2 returned nil")
	}
	if typ := ackType(deliver(fut2, 0)); typ != cy.HeaderTypeRspAck {
		t.Fatalf("phase B first response should be ACKed, got type %d", typ)
	}
	if fut2.ResponseCount() != 1 {
		t.Fatalf("phase B: expected 1 delivered response, got %d", fut2.ResponseCount())
	}
	fut2.Destroy()
	// Retransmit seqno 0 after destroy: within history -> still ACKed from the retained record,
	// AND the (now de-indexed) future must NOT be re-delivered into. Count stays 1.
	if typ := ackType(deliver(fut2, 0)); typ != cy.HeaderTypeRspAck {
		t.Fatalf("post-destroy in-history retransmit should still be ACKed, got type %d", typ)
	}
	if fut2.ResponseCount() != 1 {
		t.Fatalf("post-destroy retransmit must not be re-delivered into a dead future; got %d", fut2.ResponseCount())
	}
}

// TestRequesterBestEffortAlwaysDelivered verifies best-effort responses are always delivered to the
// application and never answered with a wire ACK/NACK (faithful to C: best-effort -> response_rx_silent).
func TestRequesterBestEffortAlwaysDelivered(t *testing.T) {
	const (
		remoteID  = uint64(11)
		topicHash = uint64(0x9988776655443322)
	)

	reqPlat := NewMockPlatform()
	requester, err := cy.New(reqPlat, "requester-be", "", "")
	if err != nil {
		t.Fatalf("requester New: %v", err)
	}
	defer requester.Destroy()

	pub, err := requester.Advertise("be.service")
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	defer pub.Destroy()

	fut := requester.RPC().Request(pub, requester.Now()+1000000, 1000000, []byte{0x02})
	if fut == nil {
		t.Fatal("Request returned nil")
	}
	messageTag := fut.Tag()
	defer fut.Destroy()

	// Best-effort response header (reliable=false): tag byte is 0xFF.
	hdr := cy.MarshalRSPHeader(false, 0xFF, 0, topicHash, messageTag)
	wire := append(hdr, 0xBE, 0xEF)
	ts := cy.NewMessageTS(cy.Microsecond(500), cy.NewMessage(wire))
	requester.HandleMessage(cy.Lane{ID: remoteID}, nil, *ts)

	// Delivered to the app.
	if fut.ResponseCount() != 1 {
		t.Fatalf("best-effort response should be delivered, got %d", fut.ResponseCount())
	}
	// No wire ACK/NACK for best-effort.
	if len(reqPlat.ReceivedMessages) != 0 {
		t.Fatalf("best-effort must not emit an ACK/NACK, got %d messages", len(reqPlat.ReceivedMessages))
	}

	// A second best-effort response with the same seqno is also delivered (no dedup for best-effort).
	hdr2 := cy.MarshalRSPHeader(false, 0xFF, 0, topicHash, messageTag)
	wire2 := append(hdr2, 0xBE, 0xEF)
	ts2 := cy.NewMessageTS(cy.Microsecond(600), cy.NewMessage(wire2))
	requester.HandleMessage(cy.Lane{ID: remoteID}, nil, *ts2)
	if fut.ResponseCount() != 2 {
		t.Fatalf("second best-effort should also be delivered, got %d", fut.ResponseCount())
	}
}
