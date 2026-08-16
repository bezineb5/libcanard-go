package tests

import (
	"testing"

	cy "github.com/opencyphal/cy-go"
)

// TestRequestFutureDoneFlipsWithResponses verifies the sampling-port contract for request futures:
// Done() is false until a response arrives, becomes true on delivery, flips back to false after
// MoveResponse consumes it (while the liveness window is still open), and becomes true again on a
// subsequent response. Faithful to C request_future_done (done == has_response || !deadline_armed).
func TestRequestFutureDoneFlipsWithResponses(t *testing.T) {
	const (
		remoteID  = uint64(7)
		topicHash = uint64(0x1122334455667788)
	)

	plat := NewMockPlatform()
	requester, err := cy.New(plat, "req", "", "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer requester.Destroy()

	pub, err := requester.Advertise("flip.service")
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	defer pub.Destroy()

	fut := requester.RPC().Request(pub, requester.Now()+1000000, 1000000, []byte{0x01})
	if fut == nil {
		t.Fatal("Request returned nil")
	}
	defer fut.Destroy()

	// Initially not done (no response, liveness armed).
	if fut.Done() {
		t.Fatal("expected not done before any response")
	}
	if fut.ResponseCount() != 0 {
		t.Fatalf("expected 0 responses, got %d", fut.ResponseCount())
	}

	deliver := func(seqno uint64) []byte {
		before := len(plat.ReceivedMessages)
		hdr := cy.MarshalRSPHeader(true, byte(seqno), seqno, topicHash, fut.Tag())
		wire := append(hdr, 0xAB)
		ts := cy.NewMessageTS(cy.Microsecond(1000), cy.NewMessage(wire))
		requester.HandleMessage(cy.Lane{ID: remoteID}, nil, *ts)
		if len(plat.ReceivedMessages) <= before {
			t.Fatalf("expected a response ACK/NACK after delivering seqno %d", seqno)
		}
		return plat.ReceivedMessages[len(plat.ReceivedMessages)-1].Data
	}

	// First reliable response -> delivered once, Done true, ErrOK.
	if typ := ackTypeOf(t, deliver(0)); typ != cy.HeaderTypeRspAck {
		t.Fatalf("first response should be ACKed, got %d", typ)
	}
	if !fut.Done() {
		t.Fatal("expected done after first response")
	}
	if fut.Error() != cy.OK {
		t.Fatalf("expected OK, got %v", fut.Error())
	}
	if fut.ResponseCount() != 1 {
		t.Fatalf("expected 1 response, got %d", fut.ResponseCount())
	}

	// Consume the arrival: Done flips back to pending (liveness still armed).
	got := fut.MoveResponse()
	if got == nil {
		t.Fatal("MoveResponse returned nil")
	}
	if fut.Done() {
		t.Fatal("expected not done after MoveResponse (liveness still armed)")
	}

	// A second reliable response (fresh, within history) -> delivered again, Done true.
	if typ := ackTypeOf(t, deliver(1)); typ != cy.HeaderTypeRspAck {
		t.Fatalf("second response should be ACKed, got %d", typ)
	}
	if !fut.Done() {
		t.Fatal("expected done after second response")
	}
	if fut.ResponseCount() != 2 {
		t.Fatalf("expected 2 unique responses, got %d", fut.ResponseCount())
	}

	// A duplicate of seqno 1 -> re-ACKed but NOT re-delivered; Done stays true, count unchanged.
	if typ := ackTypeOf(t, deliver(1)); typ != cy.HeaderTypeRspAck {
		t.Fatalf("duplicate should be re-ACKed, got %d", typ)
	}
	if fut.ResponseCount() != 2 {
		t.Fatalf("duplicate must not be re-delivered; got %d", fut.ResponseCount())
	}
}

// TestRequestFutureLivenessTimeout verifies that the inter-response liveness deadline is re-armed
// per arrival (so a response always resets the window) and that once the window lapses with no new
// response the future completes with ErrLiveness. Uses the sim network for a controllable clock.
func TestRequestFutureLivenessTimeout(t *testing.T) {
	net := NewSimNetwork(2)
	defer net.ProcessMessages()
	nodeA := net.GetNode(0)
	nodeB := net.GetNode(1)

	pub, err := nodeA.Cy.Advertise("liveness.service")
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	defer pub.Destroy()

	const livenessTimeout = cy.Microsecond(50000)
	fut := nodeA.Cy.RPC().Request(pub, net.Now()+1000000, livenessTimeout, []byte{0x02})
	if fut == nil {
		t.Fatal("Request returned nil")
	}
	defer fut.Destroy()

	if fut.Done() {
		t.Fatal("expected not done before any response")
	}

	// Deliver one reliable response.
	hdr := cy.MarshalRSPHeader(true, 0, 0, pub.Topic().Hash(), fut.Tag())
	wire := append(hdr, 0xCD)
	ts := cy.NewMessageTS(net.Now(), cy.NewMessage(wire))
	nodeA.Cy.HandleMessage(cy.Lane{ID: nodeB.ID}, nil, *ts)

	if !fut.Done() {
		t.Fatal("expected done after response")
	}
	if fut.Error() != cy.OK {
		t.Fatalf("expected OK, got %v", fut.Error())
	}

	// Consume it; the liveness window is re-armed to (response timestamp + livenessTimeout).
	fut.MoveResponse()
	if fut.Done() {
		t.Fatal("expected not done after MoveResponse")
	}

	// A modest advance that is still WITHIN the re-armed liveness window must NOT trip the
	// timeout: the window was reset on the (single) response arrival.
	net.Advance(livenessTimeout / 2)
	nodeA.Cy.SpinUntil(net.Now())
	if fut.Done() {
		t.Fatal("liveness must NOT fire before the re-armed window lapses")
	}

	// Now advance beyond the re-armed window and run the scheduler: the timeout fires.
	net.Advance(livenessTimeout)
	nodeA.Cy.SpinUntil(net.Now())
	if !fut.Done() {
		t.Fatal("expected done after liveness timeout")
	}
	if fut.Error() != cy.ErrLiveness {
		t.Fatalf("expected ErrLiveness after timeout, got %v", fut.Error())
	}
}

// TestRequestFutureTypePredicates verifies cy_is_request / cy_is_subscriber equivalents.
func TestRequestFutureTypePredicates(t *testing.T) {
	plat := NewMockPlatform()
	node, err := cy.New(plat, "pred", "", "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer node.Destroy()

	pub, err := node.Advertise("pred.service")
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	defer pub.Destroy()

	reqFut := node.RPC().Request(pub, node.Now()+1000000, 1000000, []byte{0x01})
	defer reqFut.Destroy()

	subFut := cy.NewSubscriptionFuture(nil)
	defer subFut.Destroy()

	if !cy.IsRequest(reqFut) {
		t.Error("IsRequest should be true for a request future")
	}
	if cy.IsRequest(subFut) {
		t.Error("IsRequest should be false for a subscription future")
	}
	if !cy.IsSubscriber(subFut) {
		t.Error("IsSubscriber should be true for a subscription future")
	}
	if cy.IsSubscriber(reqFut) {
		t.Error("IsSubscriber should be false for a request future")
	}
	if cy.IsRequest(nil) || cy.IsSubscriber(nil) {
		t.Error("IsRequest/IsSubscriber should be false for a nil future")
	}
}

// ackTypeOf parses the tail wire emitted by the requester and returns its header type.
func ackTypeOf(t *testing.T, wire []byte) cy.HeaderType {
	t.Helper()
	p, err := cy.ParseResponseHeader(wire)
	if err != nil {
		t.Fatalf("parse ack: %v", err)
	}
	return p.Type
}
