package can

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencyphal/cy-go"
	"go.einride.tech/can"
)

// pipe is an in-memory frameTransport connecting two platforms without real CAN hardware.
// Transmit pushes a frame to the peer's inbound channel.
type pipe struct {
	in     chan can.Frame
	out    func(can.Frame)
	closed atomic.Bool
}

func (p *pipe) Receive() (can.Frame, error) {
	f, ok := <-p.in
	if !ok {
		return can.Frame{}, io.EOF
	}
	return f, nil
}

func (p *pipe) Transmit(f can.Frame) error {
	if p.closed.Load() {
		return io.ErrClosedPipe
	}
	p.out(f)
	return nil
}

// link wires two pipes so A's TX reaches B's RX and vice versa.
func link(a, b *pipe) {
	a.out = func(f can.Frame) { b.in <- f }
	b.out = func(f can.Frame) { a.in <- f }
}

func newPipePlatform(t *testing.T, p *pipe) *Platform {
	t.Helper()
	plat, err := newPlatform(p, 256, 0, 0)
	if err != nil {
		t.Fatalf("newPlatform: %v", err)
	}
	return plat
}

// runLoop drives both platforms by polling frequently with a short horizon, so that
// libcanard's TX pipeline transmits transfers before their (short) deadlines expire.
// Mirrors a real event loop calling canardPoll periodically.
func runLoop(t *testing.T, a, b *Platform, end cy.Microsecond, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		horizon := a.Now() + 200_000 // 200ms poll horizon
		if horizon > end {
			horizon = end
		}
		a.Spin(horizon)
		b.Spin(horizon)
		if done() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for delivery")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestCANTransportRoundTrip16b(t *testing.T) {
	a := &pipe{in: make(chan can.Frame, 1024)}
	b := &pipe{in: make(chan can.Frame, 1024)}
	link(a, b)

	platA := newPipePlatform(t, a)
	platB := newPipePlatform(t, b)
	defer platA.Destroy()
	defer platB.Destroy()

	nodeA, err := cy.New(platA, "nodeA", "", "")
	if err != nil {
		t.Fatalf("cy.New A: %v", err)
	}
	nodeB, err := cy.New(platB, "nodeB", "", "")
	if err != nil {
		t.Fatalf("cy.New B: %v", err)
	}

	const topicName = "test.topic.16b"
	pub, err := nodeA.Advertise(topicName)
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	sub, err := nodeB.Subscribe(topicName, 256)
	got := make(chan []byte, 1)
	sub.SetCallback(func(arrival *cy.Arrival) {
		payload := arrival.Message.Content.Payload()
		cp := make([]byte, len(payload))
		copy(cp, payload)
		select {
		case got <- cp:
		default:
		}
	})

	app := []byte("hello cyphal can 16b transport")
	// Publish only the application payload; the Cy layer prepends the 24-byte header.
	if err := pub.Publish(nodeA.Now()+1_000_000, app); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	delivered := false
	runLoop(t, platA, platB, nodeA.Now()+2_000_000, func() bool {
		select {
		case r := <-got:
			// The Cy session layer strips the 24-byte header before delivery, so the
			// subscriber sees the application payload only.
			if len(r) != len(app) {
				t.Fatalf("payload length = %d, want %d", len(r), len(app))
			}
			if string(r) != string(app) {
				t.Fatalf("app mismatch: got %q want %q", r, app)
			}
			delivered = true
			return true
		default:
			return false
		}
	})
	if !delivered {
		t.Fatal("no delivery")
	}
}

// TestCANTransportRoundTrip13b verifies a pinned (13-bit, headerless) topic is carried
// without the Cy header on the wire and that the receiver fabricates the phony header
// so the Cy layer still sees a 24-byte header on delivery. This mirrors cy_can.c.
func TestCANTransportRoundTrip13b(t *testing.T) {
	a := &pipe{in: make(chan can.Frame, 1024)}
	b := &pipe{in: make(chan can.Frame, 1024)}
	link(a, b)

	platA := newPipePlatform(t, a)
	platB := newPipePlatform(t, b)
	defer platA.Destroy()
	defer platB.Destroy()

	nodeA, err := cy.New(platA, "nodeA", "", "")
	if err != nil {
		t.Fatalf("cy.New A: %v", err)
	}
	nodeB, err := cy.New(platB, "nodeB", "", "")
	if err != nil {
		t.Fatalf("cy.New B: %v", err)
	}

	// Pinned topic: name equals a decimal subject-ID, so the hash matches compat_topic_hash.
	const topicName = "1234"
	pub, err := nodeA.Advertise(topicName)
	if err != nil {
		t.Fatalf("Advertise: %v", err)
	}
	sub, err := nodeB.Subscribe(topicName, 256)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	got := make(chan []byte, 1)
	sub.SetCallback(func(arrival *cy.Arrival) {
		payload := arrival.Message.Content.Payload()
		cp := make([]byte, len(payload))
		copy(cp, payload)
		select {
		case got <- cp:
		default:
		}
	})

	app := []byte("pinned 13b payload!")
	if err := pub.Publish(nodeA.Now()+1_000_000, app); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	delivered := false
	runLoop(t, platA, platB, nodeA.Now()+2_000_000, func() bool {
	select {
		case r := <-got:
			// The Cy session layer strips the 24-byte phony header before delivery,
			// so the subscriber sees the application payload only.
			if len(r) != len(app) {
				t.Fatalf("payload length = %d, want %d", len(r), len(app))
			}
			if string(r) != string(app) {
				t.Fatalf("app mismatch: got %q want %q", r, app)
			}
			delivered = true
			return true
		default:
			return false
		}
	})
	if !delivered {
		t.Fatal("no delivery")
	}
}

// TestBuildPhonyHeader locks the 13-bit RX phony header format (cy_can.c build_phony_header):
// lage byte = 0xFF (=-1), evictions = le32(0xFFFFFFFF - subjectID), and the hash equals
// the compat topic hash for the pinned subject-ID.
func TestBuildPhonyHeader(t *testing.T) {
	const sid uint32 = 1234
	var h [headerBytes]byte
	buildPhonyHeader(&h, sid)
	if h[3] != 0xFF {
		t.Fatalf("lage byte = %d, want 0xFF", h[3])
	}
	ev := uint32(h[4]) | uint32(h[5])<<8 | uint32(h[6])<<16 | uint32(h[7])<<24
	if ev != ^uint32(0)-sid {
		t.Fatalf("evictions = %#x, want %#x", ev, ^uint32(0)-sid)
	}
	got := uint64(h[8]) | uint64(h[9])<<8 | uint64(h[10])<<16 | uint64(h[11])<<24 |
		uint64(h[12])<<32 | uint64(h[13])<<40 | uint64(h[14])<<48 | uint64(h[15])<<56
	if got != compatTopicHash(sid) {
		t.Fatalf("hash = %#x, want compatTopicHash(%d) = %#x", got, sid, compatTopicHash(sid))
	}
}

// TestCANTransportUnicast verifies the v1.1 unicast path: a Unicast TX via service 511
// is received and delivered as a unicast (subjectID == nil), exercising libcanard's
// SubscribeRequest RX, the per-remote TID counter, and enqueueUnicast -> handleUnicastMessage.
func TestCANTransportUnicast(t *testing.T) {
	a := &pipe{in: make(chan can.Frame, 1024)}
	b := &pipe{in: make(chan can.Frame, 1024)}
	link(a, b)
	platA := newPipePlatform(t, a)
	platB := newPipePlatform(t, b)
	platA.SetUnicastExtent(256)
	platB.SetUnicastExtent(256)
	// Cyphal unicast is node-addressed: libcanard's rxRoute drops frames whose
	// destination node-ID != local node-ID, so the receiver must adopt the ID that
	// the request is sent to. The sender's own node-ID is OR'd into the frame as
	// the source; we pin both so the assertions are deterministic.
	platA.canard.SetNodeID(1)
	platB.canard.SetNodeID(2)

	var gotMu sync.Mutex
	var gotLane cy.Lane
	var gotSID *uint32
	var gotPayload []byte
	platB.PlatformBase.OnMessage = func(lane cy.Lane, subjectID *uint32, message cy.MessageTS) {
		gotMu.Lock()
		defer gotMu.Unlock()
		gotLane = lane
		if subjectID != nil {
			s := *subjectID
			gotSID = &s
		} else {
			gotSID = nil
		}
		gotPayload = append([]byte(nil), message.Content.Payload()...)
	}

	payload := []byte("unicast ping over service 511")
	// Send to B's node-ID (2). The frame's source node-ID is A's (1), set above.
	lane := cy.Lane{ID: 2, Priority: cy.PriorityNominal}
	lane.Context[0] = 2
	if err := platA.Unicast(lane, platA.Now()+1_000_000, payload); err != nil {
		t.Fatalf("Unicast: %v", err)
	}

	delivered := false
	runLoop(t, platA, platB, platA.Now()+2_000_000, func() bool {
		gotMu.Lock()
		defer gotMu.Unlock()
		if gotPayload == nil {
			return false
		}
		// Unicast delivery must carry a nil subject-ID.
		if gotSID != nil {
			t.Fatalf("unicast delivered with subjectID = %d, want nil", *gotSID)
		}
		// Source node-ID is the sender's (A == 1), OR'd into the frame by libcanard.
		if gotLane.ID != 1 {
			t.Fatalf("lane.ID = %d, want 1 (source node-ID)", gotLane.ID)
		}
		if !bytes.Equal(gotPayload, payload) {
			t.Fatalf("payload = %q, want %q", gotPayload, payload)
		}
		delivered = true
		return true
	})
	if !delivered {
		t.Fatal("no unicast delivery")
	}
}
