package libcanard

import (
	"context"
	"io"
	"testing"
	"time"

	"go.einride.tech/can"
)

// fakeWriter records every frame handed to it, acting as a go.einride.tech/can FrameWriter sink.
type fakeWriter struct {
	frames []can.Frame
}

func (w *fakeWriter) TransmitFrame(_ context.Context, f can.Frame) error {
	w.frames = append(w.frames, f)
	return nil
}

// fakeReader replays a fixed set of frames once, then reports EOF.
type fakeReader struct {
	frames []can.Frame
	idx    int
}

func (r *fakeReader) Receive() bool {
	if r.idx >= len(r.frames) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeReader) Frame() can.Frame { return r.frames[r.idx-1] }

func (r *fakeReader) Err() error { return nil }

// blockingReader blocks in Receive until stopped, then reports a cancellation error. It is used to
// exercise the context-cancellation path of Conn.Run.
type blockingReader struct {
	stop chan struct{}
}

func (r *blockingReader) Receive() bool {
	<-r.stop
	return false
}

func (r *blockingReader) Frame() can.Frame { return can.Frame{} }

func (r *blockingReader) Err() error { return context.Canceled }

// adapterInstance builds a Canard with the given node-ID, using the supplied TX callback and a
// fake clock so timestamps are deterministic. FD is disabled because can.Frame models Classic CAN.
func adapterInstance(t *testing.T, nodeID uint8, tx func(*Canard, any, int64, uint8, bool, uint32, []byte) bool) *Canard {
	t.Helper()
	now := int64(0)
	c := &Canard{}
	ok := c.Init(NewPlatform(func(self *Canard) int64 { return now }, tx, FilterAcceptAll,), NewDefaultMemSet(), IfaceBitmapAll, 1000, 12345, 0)
	if !ok {
		t.Fatal("init failed")
	}
	c.SetNodeID(nodeID)
	c.tx.FD = false // Classic CAN only, matching can.Frame.
	return c
}

func TestCanFrameTXRXRoundTrip(t *testing.T) {
	w := &fakeWriter{}
	sender := adapterInstance(t, 42, TXToFrameWriter(w, context.Background()))
	receiver := adapterInstance(t, 43, TXToFrameWriter(w, context.Background()))

	var got []byte
	sub := &Subscription{}
	if receiver.Subscribe16b(sub, 1234, 64, DefaultTransferIDTimeoutUs, &SubscriptionVTable{
		OnMessage: func(s *Subscription, ts int64, p Prio, src uint8, tid uint8, payload Payload) {
			got = make([]byte, payload.View.Size)
			copy(got, unsafeSlice(payload.View))
		},
	}) == nil {
		t.Fatal("subscribe failed")
	}

	payload := []byte("hi!!") // fits a single Classic CAN frame.
	if !sender.Publish16b(1000, IfaceBitmapAll, PrioNominal, 1234, 7, payload, nil) {
		t.Fatal("publish failed")
	}
	sender.FlushTX()

	if len(w.frames) == 0 {
		t.Fatal("no frames transmitted")
	}
	// Frames captured by the writer must be extended 29-bit frames.
	for _, f := range w.frames {
		if !f.IsExtended {
			t.Fatalf("frame %v is not extended", f)
		}
		if f.Length > uint8(len(f.Data)) {
			t.Fatalf("frame %v length exceeds data", f)
		}
	}
	// Feed them into the receiver through the adapter.
	if n := receiver.IngestCANFrames(1000, 0, w.frames); n != len(w.frames) {
		t.Fatalf("ingested %d/%d frames", n, len(w.frames))
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %q want %q", got, payload)
	}
}

func TestPumpRXAndConnRun(t *testing.T) {
	w := &fakeWriter{}
	sender := adapterInstance(t, 42, TXToFrameWriter(w, context.Background()))
	receiver := adapterInstance(t, 43, TXToFrameWriter(w, context.Background()))

	var got []byte
	sub := &Subscription{}
	if receiver.Subscribe13b(sub, 777, 64, DefaultTransferIDTimeoutUs, &SubscriptionVTable{
		OnMessage: func(s *Subscription, ts int64, p Prio, src uint8, tid uint8, payload Payload) {
			got = make([]byte, payload.View.Size)
			copy(got, unsafeSlice(payload.View))
		},
	}) == nil {
		t.Fatal("subscribe failed")
	}

	payload := make([]byte, 20) // forces a multi-frame Classic CAN transfer.
	for i := range payload {
		payload[i] = byte(i)
	}
	if !sender.Publish13b(1000, IfaceBitmapAll, PrioNominal, 777, 3, payload, nil) {
		t.Fatal("publish failed")
	}
	sender.FlushTX()

	adapter := &Conn{Instance: receiver, Reader: &fakeReader{frames: w.frames}, FlushInterval: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// PumpRX (driven by Conn.Run) should ingest both frames and then hit EOF, returning io.EOF.
	if err := adapter.Run(ctx); err != io.EOF {
		t.Fatalf("Run returned %v, want io.EOF", err)
	}
	if len(got) != len(payload) || string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %v want %v", got, payload)
	}
}

func TestConnRunCancellation(t *testing.T) {
	receiver := adapterInstance(t, 43, TXToFrameWriter(&fakeWriter{}, context.Background()))
	br := &blockingReader{stop: make(chan struct{})}
	adapter := &Conn{Instance: receiver, Reader: br, FlushInterval: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		close(br.stop)
		cancel()
	}()
	if err := adapter.Run(ctx); err != context.Canceled {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}
