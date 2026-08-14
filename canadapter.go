// Canard <-> go.einride.tech/can adapter.
//
// This file bridges the libcanard callback API to the go.einride.tech/can ecosystem.
// The central currency is can.Frame: received frames are converted into libcanard's
// internal representation, and transmitted frames are emitted as can.Frame values.
//
// Two small structural interfaces, FrameReader (RX) and FrameWriter (TX), decouple the
// adapter from any concrete I/O backend. They are satisfied directly by the socketcan
// helpers, for example:
//
//	conn, _ := socketcan.Dial("can", "can0")        // go.einride.tech/can/pkg/socketcan
//	tx := socketcan.NewTransmitter(conn)            // implements FrameWriter
//	rx := socketcan.NewReceiver(conn)               // implements FrameReader
//
// The adapter only depends on the root can package, which carries at most
// can.MaxDataLength (8) data bytes per frame; go.einride.tech/can therefore models
// Classic CAN. When bridging to it, configure the instance for Classic CAN via SetClassicCAN(true)
// (or use V0Publish / Publish13b with the Classic MTU).
package libcanard

import (
	"context"
	"io"
	"time"

	"go.einride.tech/can"
)

// ---------------------------------------------------------------------------------------------------------------------
// RX / TX backend interfaces.
// ---------------------------------------------------------------------------------------------------------------------

// FrameWriter transmits a single CAN frame. It is satisfied by *socketcan.Transmitter, whose
// TransmitFrame(ctx, frame) method it mirrors.
type FrameWriter interface {
	TransmitFrame(ctx context.Context, f can.Frame) error
}

// FrameReader yields received CAN frames. It is satisfied by *socketcan.Receiver, whose
// Receive/Frame/Err method triple it mirrors.
type FrameReader interface {
	Receive() bool
	Frame() can.Frame
	Err() error
}

// ---------------------------------------------------------------------------------------------------------------------
// RX: ingesting received frames.
// ---------------------------------------------------------------------------------------------------------------------

// IngestCANFrame feeds a single received can.Frame into the instance. It returns false if the
// frame cannot be a Cyphal/CAN frame (remote frame, or a standard 11-bit ID, or a length that
// exceeds the data buffer) and true otherwise. The frame's ID is taken as the 29-bit extended
// CAN ID and its data as the payload.
func (self *Canard) IngestCANFrame(timestamp int64, ifaceIndex uint8, frame can.Frame) bool {
	if frame.IsRemote || !frame.IsExtended {
		return false
	}
	if frame.Length > uint8(len(frame.Data)) {
		return false
	}
	return self.IngestFrame(timestamp, ifaceIndex, frame.ID, frame.Data[:frame.Length])
}

// IngestCANFrames feeds a batch of received CAN frames into the instance, using the same
// timestamp and interface index for every frame. It returns the number of frames that were
// successfully ingested.
func (self *Canard) IngestCANFrames(timestamp int64, ifaceIndex uint8, frames []can.Frame) int {
	accepted := 0
	for i := range frames {
		if self.IngestCANFrame(timestamp, ifaceIndex, frames[i]) {
			accepted++
		}
	}
	return accepted
}

// PumpRX reads frames from r and feeds each one into the instance until r reports an error or
// EOF, or ctx is cancelled. It blocks and is meant to be run in its own goroutine.
//
// The timestamp for each frame is taken from now (defaulting to time.Now().UnixMicro, i.e.
// microseconds since the Unix epoch). Frames are reported on ifaceIndex.
//
// The underlying reader is not context-aware: if Receive blocks on I/O, PumpRX will only notice
// cancellation once the read returns. Pair it with a reader that honours read deadlines, or rely
// on the Conn.Run driver which isolates the pump in its own goroutine.
//
// It returns the reader error (io.EOF on a clean end-of-stream) or ctx.Err() if cancelled.
func PumpRX(ctx context.Context, inst *Canard, r FrameReader, now func() int64, ifaceIndex uint8) error {
	if now == nil {
		now = func() int64 { return time.Now().UnixMicro() }
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !r.Receive() {
			if err := r.Err(); err != nil {
				return err
			}
			return io.EOF
		}
		inst.IngestCANFrame(now(), ifaceIndex, r.Frame())
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// TX: emitting transmitted frames.
// ---------------------------------------------------------------------------------------------------------------------

// CanFrameFromTX builds a can.Frame from the arguments of the Platform.TX callback. Cyphal/CAN
// frames are always extended (29-bit) and never remote, so the result always has IsExtended set.
//
// Note that can.Frame carries at most can.MaxDataLength (8) bytes of data; any additional bytes
// in canData are dropped. This is only correct for Classic CAN transfers -- disable FD
// (instance.tx.FD = false) when bridging to go.einride.tech/can.
func CanFrameFromTX(id uint32, data []byte) can.Frame {
	n := min(len(data), can.MaxDataLength)
	f := can.Frame{ID: id, Length: uint8(n), IsExtended: true}
	copy(f.Data[:n], data)
	return f
}

// TXToChannel returns a Platform.TX callback that delivers transmitted frames as can.Frame values
// to ch. The channel must have enough capacity to avoid blocking the TX pipeline.
//
// Example:
//
//	ch := make(chan can.Frame, 128)
//	instance.Init(&VTable{
//	    TX:    libcanard.TXToChannel(ch),
//	    Now:   libcanard.NowFunc(time.Now().UnixMicro),
//	    Filter: libcanard.FilterAcceptAll,
//	}, memSet, ifaceBitmap, deadline, nodeID, 0)
//	go func() {
//	    for frame := range ch {
//	        canDriver.Write(frame)
//	    }
//	}()
func TXToChannel(ch chan<- can.Frame) func(*Canard, any, int64, uint8, bool, uint32, []byte) bool {
	return func(_ *Canard, _ any, _ int64, _ uint8, _ bool, id uint32, data []byte) bool {
		ch <- CanFrameFromTX(id, data)
		return true
	}
}

// TXToFrameWriter returns a Platform.TX callback that writes each transmitted frame to w via
// TransmitFrame, using ctx for the write. It reports success (true) on a successful write, and
// false on a write error so the frame is retried later. The callback forwards the frame verbatim,
// including its 29-bit extended ID.
//
// Example:
//
//	tx := socketcan.NewTransmitter(conn)
//	vtable.TX = libcanard.TXToFrameWriter(tx, ctx)
func TXToFrameWriter(w FrameWriter, ctx context.Context) func(*Canard, any, int64, uint8, bool, uint32, []byte) bool {
	return func(_ *Canard, _ any, _ int64, _ uint8, _ bool, id uint32, data []byte) bool {
		if err := w.TransmitFrame(ctx, CanFrameFromTX(id, data)); err != nil {
			return false
		}
		return true
	}
}

// TXFunc is the signature of a user callback invoked for every frame submitted for transmission.
// Return false to signal a busy interface (the frame is retried later).
type TXFunc func(frame can.Frame) bool

// TXToFunc returns a Platform.TX callback that forwards each transmitted frame to fn. This is handy
// for custom sinks (logging, virtual CAN, test harnesses) without introducing a channel.
func TXToFunc(fn TXFunc) func(*Canard, any, int64, uint8, bool, uint32, []byte) bool {
	return func(_ *Canard, _ any, _ int64, _ uint8, _ bool, id uint32, data []byte) bool {
		return fn(CanFrameFromTX(id, data))
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// Platform helpers.
// ---------------------------------------------------------------------------------------------------------------------

// NowFunc returns a Platform.Now implementation backed by the provided clock. The clock must yield
// a non-negative, non-decreasing monotonic time in microseconds.
func NowFunc(now func() int64) func(self *Canard) int64 {
	return func(self *Canard) int64 { return now() }
}

// FilterAcceptAll returns a Platform.Filter implementation that reports success unconditionally.
// Use it when the CAN controller performs no hardware acceptance filtering (libcanard then does
// all routing in software, which is the case when reading every frame through go.einride.tech/can).
func FilterAcceptAll(self *Canard, count int, filters []Filter) bool { return true }

// ---------------------------------------------------------------------------------------------------------------------
// Driving the TX pipeline.
// ---------------------------------------------------------------------------------------------------------------------

// FlushTX drives the TX pipeline, attempting to transmit every pending frame on the interfaces
// that currently have pending transmissions. Call it periodically and whenever a previously busy
// interface becomes writable. It is a thin wrapper around Poll(PendingIfaces()).
func (self *Canard) FlushTX() {
	self.Poll(self.PendingIfaces())
}

// ---------------------------------------------------------------------------------------------------------------------
// Full-duplex connection driver.
// ---------------------------------------------------------------------------------------------------------------------

// Conn couples a Canard instance with a CAN interface for bidirectional transfer. The Reader
// supplies received frames (ingested via PumpRX); the Writer consumes transmitted frames (the
// instance's Platform.TX should be set to TXToFrameWriter(Conn.Writer, ...)).
//
// Example:
//
//	conn, _ := socketcan.Dial("can", "can0") // *socketcan.Transmitter + *socketcan.Receiver
//	tx := socketcan.NewTransmitter(conn)
//	rx := socketcan.NewReceiver(conn)
//	inst, _ := libcanard.New(&libcanard.VTable{
//	    Now:    libcanard.NowFunc(time.Now().UnixMicro),
//	    TX:     libcanard.TXToFrameWriter(tx, ctx),
//	    Filter: libcanard.FilterAcceptAll,
//	}, libcanard.NewDefaultMemSet(), libcanard.IfaceBitmapAll, 256, 0, 0)
//	inst.tx.FD = false // go.einride.tech/can models Classic CAN frames
//	adapter := &libcanard.Conn{Instance: inst, Reader: rx, Writer: tx, FlushInterval: time.Millisecond * 10}
//	go adapter.Run(ctx)
type Conn struct {
	Instance       *Canard
	Reader         FrameReader
	Writer         FrameWriter
	InterfaceIndex uint8

	// FlushInterval, if positive, makes Run periodically call FlushTX so queued transfers are
	// sent even when no external "interface writable" event occurs. A small interval (e.g.
	// 1-10 ms) is appropriate for software-backed interfaces.
	FlushInterval time.Duration

	// Now returns the monotonic time in microseconds for ingested frames. Defaults to
	// time.Now().UnixMicro.
	Now func() int64
}

// Run drives both transfer directions until ctx is cancelled. It starts the RX pump in a
// goroutine and, when FlushInterval is positive, a periodic TX flush. It returns the RX reader
// error (io.EOF on clean end-of-stream) or ctx.Err() once the context is done.
//
// Run is not safe to call concurrently with itself; it is safe to cancel the context from
// another goroutine.
func (c *Conn) Run(ctx context.Context) error {
	now := c.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixMicro() }
	}
	errCh := make(chan error, 1)
	go func() { errCh <- PumpRX(ctx, c.Instance, c.Reader, now, c.InterfaceIndex) }()

	if c.FlushInterval <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		}
	}

	ticker := time.NewTicker(c.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case <-ticker.C:
			c.Instance.FlushTX()
		}
	}
}
