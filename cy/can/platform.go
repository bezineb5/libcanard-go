// Package can provides a CAN transport layer implementation for Cyphal.
//
// It is a faithful Pure-Go port of the C Cyphal/CAN transport (cy_can.c). Framing,
// transfer reassembly, dedup, and the 16-bit/13-bit subject-ID split are delegated to
// the sibling libcanard package (a Go port of OpenCyphal/libcanard). SocketCAN I/O is
// performed through go.einride.tech/can, bridged via a small frameTransport shim so the
// transport can be exercised without real CAN hardware.
package can

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/opencyphal/cy-go"
	"go.dw1.io/rapidhash"
	"go.einride.tech/can"
	"go.einride.tech/can/pkg/socketcan"

	"github.com/bezineb5/libcanard-go"
)

// UnicastServiceID is the reserved Cyphal/CAN service-ID for v1.1 unicast transfers.
const UnicastServiceID = 511

// headerBytes is the fixed Cy session-layer header size (HEADER_BYTES in cy_can.c).
const headerBytes = 24

// frameTransport abstracts CAN frame I/O so the platform can run against real
// SocketCAN or a mock in tests. It is satisfied by socketcan.Receiver/Transmitter.
type frameTransport interface {
	Receive() (can.Frame, error)
	Transmit(frame can.Frame) error
}

// pending holds one reassembled transfer awaiting delivery to the Cy instance.
type pending struct {
	lane      cy.Lane
	subjectID *uint32 // nil for unicast
	message   *cy.MessageTS
}

// Platform implements the cy.Platform interface for CAN transport.
type Platform struct {
	cy.PlatformBase

	canard *libcanard.Canard

	transport frameTransport
	rxCh      chan can.Frame
	closed    atomic.Bool

	// mu guards the non-goroutine-safe *libcanard.Canard and the pending queue.
	mu      sync.Mutex
	pending []pending

	// writers/readers keyed by subject-ID (Cy guarantees at most one of each per ID).
	writers map[uint32]*subjectWriter
	readers map[uint32]*subjectReader

	// unicastTID holds the per-remote transfer-ID used for v1.1 unicast transfers.
	unicastTID [libcanard.NodeIDMax + 1]uint8

	// unicastSub is the persistent libcanard subscription for service 511. It is
	// reused across SetUnicastExtent calls so that re-subscribing with a larger
	// extent updates the same libcanard node (rxSubscribe only refreshes an
	// existing port-ID node when the same subscription pointer is passed).
	unicastSub *libcanard.Subscription

	// unicastExtent is the maximum size of incoming unicast transfers (service 511).
	unicastExtent int

	randState uint64

	subjectIDModulus uint32
}

type subjectWriter struct {
	subjectID uint32
	platform  *Platform
}

func (w *subjectWriter) SubjectID() uint32 { return w.subjectID }

type subjectReader struct {
	subjectID uint32
	extent    int
	platform  *Platform
}

func (r *subjectReader) SubjectID() uint32    { return r.subjectID }
func (r *subjectReader) Extent() int          { return r.extent }
func (r *subjectReader) SetExtent(extent int) { r.extent = extent }

// readerCtx is stored as the libcanard subscription UserContext so the OnMessage
// callback can recover the subject-ID and (for 13-bit subjects) the phony header state.
type readerCtx struct {
	subjectID uint32
	pinned    bool // 13-bit (headerless) subject
	phony     [headerBytes]byte
	phonyTag  uint64
}

// New creates a new CAN platform bound to the named SocketCAN interface.
func New(ifaceName string, txQueueCapacity, filterCount int, prngSeed uint64) (cy.Platform, error) {
	if ifaceName == "" {
		return nil, errors.New("CAN interface name is required")
	}
	conn, err := socketcan.Dial("can", ifaceName)
	if err != nil {
		return nil, err
	}
	transport := &socketcanTransport{
		receiver:    socketcan.NewReceiver(conn),
		transmitter: socketcan.NewTransmitter(conn),
	}
	return newPlatform(transport, txQueueCapacity, filterCount, prngSeed)
}

// socketcanTransport adapts socketcan.Receiver/Transmitter to frameTransport.
type socketcanTransport struct {
	receiver    *socketcan.Receiver
	transmitter *socketcan.Transmitter
}

func (t *socketcanTransport) Receive() (can.Frame, error) {
	if !t.receiver.Receive() {
		if err := t.receiver.Err(); err != nil {
			return can.Frame{}, err
		}
		return can.Frame{}, errors.New("socketcan receive closed")
	}
	return t.receiver.Frame(), nil
}

func (t *socketcanTransport) Transmit(frame can.Frame) error {
	return t.transmitter.TransmitFrame(context.Background(), frame)
}

// newPlatform builds a platform from a frameTransport (real or mock).
func newPlatform(transport frameTransport, txQueueCapacity, filterCount int, prngSeed uint64) (*Platform, error) {
	if txQueueCapacity <= 0 {
		txQueueCapacity = 256
	}
	if filterCount <= 0 {
		filterCount = 0
	}

	p := &Platform{
		transport:        transport,
		rxCh:             make(chan can.Frame, 1024),
		writers:          make(map[uint32]*subjectWriter),
		readers:          make(map[uint32]*subjectReader),
		randState:        prngSeed,
		subjectIDModulus: cy.SubjectIDModulus16bit,
	}

	p.canard, _ = libcanard.New(
		libcanard.NewPlatform(p.now, p.tx, libcanard.FilterAcceptAll),
		libcanard.NewDefaultMemSet(),
		libcanard.IfaceBitmapAll,
		txQueueCapacity,
		prngSeed,
		filterCount,
	)
	if p.canard == nil {
		return nil, errors.New("libcanard init failed")
	}
	// go.einride.tech/can models Classic CAN (8-byte MTU). Force the Classic CAN
	// MTU so libcanard emits wire frames compatible with it; otherwise it defaults
	// to CAN FD (64-byte MTU) which can.Frame cannot carry.
	p.canard.SetClassicCAN(true)

	// Subscribe to service 511 for incoming v1.1 unicast transfers. The Cy session
	// header is carried intact in the payload; the callback enqueues a unicast
	// delivery (subjectID == nil) so HandleMessage routes it to handleUnicastMessage.
	// A zero extent would make libcanard allocate an empty reassembly buffer, so use a
	// non-zero default unless the application raised it via SetUnicastExtent.
	ue := p.unicastExtent
	if ue <= 0 {
		ue = 256
	}
	sub := &libcanard.Subscription{}
	if p.canard.SubscribeRequest(sub, UnicastServiceID, ue, libcanard.DefaultTransferIDTimeoutUs, &libcanard.SubscriptionVTable{
		OnMessage: func(self *libcanard.Subscription, timestamp int64, priority libcanard.Prio, sourceNodeID uint8, transferID uint8, payload libcanard.Payload) {
			buf := copyPayload(&payload)
			p.enqueueUnicast(sourceNodeID, priority, timestamp, buf)
		},
	}) == nil {
		return nil, errors.New("libcanard unicast subscribe failed")
	}
	p.unicastSub = sub

	go p.recvLoop()
	return p, nil
}

// now implements the libcanard Platform.Now callback.
func (p *Platform) tx(self *libcanard.Canard, userContext any, deadline int64, ifaceIndex uint8, fd bool, extendedCANID uint32, canData []byte) bool {
	_ = deadline
	_ = ifaceIndex
	var data can.Data
	copy(data[:], canData)
	frame := can.Frame{
		ID:         extendedCANID,
		Length:     uint8(len(canData)),
		Data:       data,
		IsExtended: true,
	}
	if err := p.transport.Transmit(frame); err != nil {
		return false
	}
	return true
}

func (p *Platform) ingest(frame can.Frame) {
	data := frame.Data[:frame.Length]
	p.canard.IngestFrame(time.Now().UnixMicro(), 0, frame.ID, data)
}

func (p *Platform) recvLoop() {
	for {
		frame, err := p.transport.Receive()
		if err != nil {
			return
		}
		select {
		case p.rxCh <- frame:
		default:
		}
	}
}

func (p *Platform) Destroy() {
	if p.closed.Swap(true) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writers = nil
	p.readers = nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Multicast: subject writers.
// ---------------------------------------------------------------------------------------------------------------------

// NewSubjectWriter creates a new subject writer for the specified subject-ID.
func (p *Platform) NewSubjectWriter(subjectID uint32) (cy.SubjectWriter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writers == nil {
		return nil, cy.ErrArgument
	}
	if w, ok := p.writers[subjectID]; ok {
		return w, nil
	}
	w := &subjectWriter{subjectID: subjectID, platform: p}
	p.writers[subjectID] = w
	return w, nil
}

// DestroySubjectWriter destroys a subject writer. The Cy core owns the single
// writer per subject-ID; this only clears the local handle so a later
// NewSubjectWriter re-creates it.
func (p *Platform) DestroySubjectWriter(writer cy.SubjectWriter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writers == nil {
		return
	}
	if w, ok := writer.(*subjectWriter); ok {
		delete(p.writers, w.subjectID)
	}
}

// SubjectWriterSend publishes a message on the subject. The data passed in already
// carries the 24-byte Cy session header (prepended by the Cy layer in publishImpl).
//
// Faithful to cy_can.c v_subject_writer_send: a subject is carried over the 13-bit
// (headerless) path iff it is pinned (subject-ID <= SubjectIDPinnedMax), best-effort
// (header type == msg_be), and its header hash equals the compat topic hash for the
// subject-ID (the N#N idiom). Otherwise it uses the 16-bit path with the header intact.
func (p *Platform) SubjectWriterSend(writer cy.SubjectWriter, deadline cy.Microsecond, priority cy.Priority, data []byte) error {
	w, ok := writer.(*subjectWriter)
	if !ok || len(data) < headerBytes {
		return cy.ErrArgument
	}
	p.mu.Lock()
	sid := uint16(w.subjectID)
	pinned := w.subjectID <= cy.SubjectIDPinnedMax
	bestEffort := data[0] == 0
	use13b := pinned && bestEffort && topicIsCompatNamed(w.subjectID, data)
	defer p.mu.Unlock()
	if p.canard == nil {
		return cy.ErrArgument
	}
	var okTx bool
	if use13b {
		stripped := data[headerBytes:]
		okTx = p.canard.Publish13b(int64(deadline), libcanard.IfaceBitmapAll, libcanard.Prio(priority), sid, 0, stripped, nil)
	} else {
		okTx = p.canard.Publish16b(int64(deadline), libcanard.IfaceBitmapAll, libcanard.Prio(priority), sid, 0, data, nil)
	}
	if !okTx {
		return cy.ErrMemory
	}
	return nil
}

// topicIsCompatNamed mirrors cy_can.c topic_is_compat_named: true iff the outgoing
// header's hash field (bytes 8..16) equals the compat topic hash for the subject-ID.
func topicIsCompatNamed(sid uint32, header []byte) bool {
	want := compatTopicHash(sid)
	got := uint64(header[8]) | uint64(header[9])<<8 | uint64(header[10])<<16 | uint64(header[11])<<24 |
		uint64(header[12])<<32 | uint64(header[13])<<40 | uint64(header[14])<<48 | uint64(header[15])<<56
	return got == want
}

// compatTopicHash mirrors cy_can.c compat_topic_hash: rapidhash of the subject-ID's
// decimal spelling. This MUST agree with the 13-bit RX phony header and with a pinned
// topic's hash in the Cy layer (a pinned topic named "1234" hashes to rapidhash("1234")).
func compatTopicHash(sid uint32) uint64 {
	return rapidhash.Hash([]byte(strconv.FormatUint(uint64(sid), 10)))
}

// ---------------------------------------------------------------------------------------------------------------------
// Multicast: subject readers.
func (p *Platform) NewSubjectReader(subjectID uint32, extent int) (cy.SubjectReader, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.readers == nil {
		return nil, cy.ErrArgument
	}
	if r, ok := p.readers[subjectID]; ok {
		return r, nil
	}
	sid := uint16(subjectID)
	ctx := &readerCtx{subjectID: subjectID, pinned: subjectID <= cy.SubjectIDPinnedMax}
	if ctx.pinned {
		buildPhonyHeader(&ctx.phony, subjectID)
	}

	// libcanard's rxSubscribe resets sub.UserContext, so capture ctx in the closure
	// rather than relying on the subscription's UserContext field.
	on16 := func(self *libcanard.Subscription, timestamp int64, priority libcanard.Prio, sourceNodeID uint8, transferID uint8, payload libcanard.Payload) {
		buf := copyPayload(&payload)
		p.enqueue(ctx.subjectID, sourceNodeID, priority, timestamp, buf)
	}
	if p.canard.Subscribe16b(&libcanard.Subscription{}, sid, extent, libcanard.DefaultTransferIDTimeoutUs, &libcanard.SubscriptionVTable{
		OnMessage: on16,
	}) == nil {
		return nil, cy.ErrArgument
	}
	if ctx.pinned {
		extent13 := extent
		if extent13 > headerBytes {
			extent13 -= headerBytes
		}
		on13 := func(self *libcanard.Subscription, timestamp int64, priority libcanard.Prio, sourceNodeID uint8, transferID uint8, payload libcanard.Payload) {
			ctx.phonyTag++
			buf := make([]byte, headerBytes+libcanardPayloadLen(&payload))
			copy(buf[:headerBytes], ctx.phony[:])
			binaryLEPutUint64(buf[16:24], ctx.phonyTag)
			copyPayloadInto(&payload, buf[headerBytes:])
			p.enqueue(ctx.subjectID, sourceNodeID, priority, timestamp, buf)
		}
		if p.canard.Subscribe13b(&libcanard.Subscription{}, sid, extent13, libcanard.DefaultTransferIDTimeoutUs, &libcanard.SubscriptionVTable{
			OnMessage: on13,
		}) == nil {
			return nil, cy.ErrArgument
		}
	}
	r := &subjectReader{subjectID: subjectID, extent: extent, platform: p}
	p.readers[subjectID] = r
	return r, nil
}

// DestroySubjectReader destroys a subject reader. libcanard does not expose an
// unsubscribe primitive, so we detach the local handle and rely on instance
// teardown to release the subscription; the Cy core owns the single reader per
// subject-ID and issues a final Destroy on teardown.
func (p *Platform) DestroySubjectReader(reader cy.SubjectReader) {
	r, ok := reader.(*subjectReader)
	if !ok {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.readers == nil {
		return
	}
	delete(p.readers, r.subjectID)
}

// SetSubjectReaderExtent updates the maximum extent of incoming messages.
func (p *Platform) SetSubjectReaderExtent(reader cy.SubjectReader, extent int) {
	if r, ok := reader.(*subjectReader); ok {
		r.extent = extent
	}
}

// buildPhonyHeader mirrors cy_can.c build_phony_header: the fabricated Cy session
// header imputed to a headerless 13-bit subject.
func buildPhonyHeader(out *[headerBytes]byte, sid uint32) {
	for i := range out {
		out[i] = 0
	}
	out[3] = 0xFF // lage = -1 (int8_t)
	ev := uint32(^uint32(0) - sid)
	out[4] = byte(ev)
	out[5] = byte(ev >> 8)
	out[6] = byte(ev >> 16)
	out[7] = byte(ev >> 24)
	h := compatTopicHash(sid)
	for i := range 8 {
		out[8+i] = byte(h >> (8 * uint(i)))
	}
}

// enqueue appends a pending delivery under p.mu. Called from within libcanard
// callbacks, so it must not reenter the library.
func (p *Platform) enqueue(subjectID uint32, src uint8, prio libcanard.Prio, ts int64, buf []byte) {
	lane := cy.Lane{
		ID:       uint64(src),
		Priority: cy.Priority(prio),
	}
	lane.Context[0] = src // mirrors C lane->ctx.state[0] = remote node-ID
	sid := subjectID
	p.pending = append(p.pending, pending{
		lane:      lane,
		subjectID: &sid,
		message:   cy.NewMessageTS(cy.Microsecond(ts), cy.NewMessage(buf)),
	})
}

// enqueueUnicast appends a pending unicast delivery (subjectID == nil) so the Cy
// instance routes it to handleUnicastMessage. Remote node-ID goes in lane.ID and
// lane.Context[0], mirroring the multicast path.
func (p *Platform) enqueueUnicast(src uint8, prio libcanard.Prio, ts int64, buf []byte) {
	lane := cy.Lane{
		ID:       uint64(src),
		Priority: cy.Priority(prio),
	}
	lane.Context[0] = src
	p.pending = append(p.pending, pending{
		lane:      lane,
		subjectID: nil,
		message:   cy.NewMessageTS(cy.Microsecond(ts), cy.NewMessage(buf)),
	})
}

// ---------------------------------------------------------------------------------------------------------------------
// Unicast.
// ---------------------------------------------------------------------------------------------------------------------

// Unicast sends a unicast transfer to the specified remote node via service 511.
// Mirrors cy_can.c v_unicast_send: a per-remote transfer-ID counter modulo 32.
func (p *Platform) Unicast(lane cy.Lane, deadline cy.Microsecond, data []byte) error {
	remote := lane.ID
	if remote > libcanard.NodeIDMax || remote == 0 {
		return cy.ErrArgument
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.canard == nil {
		return cy.ErrArgument
	}
	tid := p.unicastTID[remote]
	p.unicastTID[remote] = uint8((uint32(tid) + 1) % libcanard.TransferIDModulo)
	ok := p.canard.Request(int64(deadline), libcanard.Prio(lane.Priority), UnicastServiceID, uint8(remote), tid, data, nil)
	if !ok {
		return cy.ErrMemory
	}
	return nil
}

// SetUnicastExtent sets the maximum extent of incoming unicast transfers.
func (p *Platform) SetUnicastExtent(extent int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// C cy_platform_vtable sets the extent unconditionally and tracks the max.
	if extent <= p.unicastExtent {
		return
	}
	p.unicastExtent = extent
	// Re-subscribe on the same subscription pointer so libcanard refreshes the
	// existing service-511 node's extent (rxSubscribe ignores a different pointer).
	if p.unicastSub != nil {
		if p.canard.SubscribeRequest(p.unicastSub, UnicastServiceID, extent, libcanard.DefaultTransferIDTimeoutUs, &libcanard.SubscriptionVTable{
			OnMessage: func(self *libcanard.Subscription, timestamp int64, priority libcanard.Prio, sourceNodeID uint8, transferID uint8, payload libcanard.Payload) {
				buf := copyPayload(&payload)
				p.enqueueUnicast(sourceNodeID, priority, timestamp, buf)
			},
		}) == nil {
			// Non-fatal: old extent stays in effect; surface nothing to caller.
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// Event loop.
// ---------------------------------------------------------------------------------------------------------------------

// Spin runs the event loop until the deadline, ingesting received frames, flushing
// the TX queue, and delivering reassembled transfers to the Cy instance.
func (p *Platform) Spin(deadline cy.Microsecond) error {
	for {
		// Drain any frames already buffered from the receive loop. This must happen
		// before the deadline check: frames that arrived after the previous poll but
		// before this call would otherwise sit un-ingested if the deadline already passed.
		select {
		case frame, ok := <-p.rxCh:
			if !ok {
				return nil
			}
			p.mu.Lock()
			p.ingest(frame)
			p.mu.Unlock()
			continue
		default:
		}

		remaining := deadline - p.Now()
		if remaining <= 0 {
			break
		}
		select {
		case frame, ok := <-p.rxCh:
			if !ok {
				return nil
			}
			p.mu.Lock()
			p.ingest(frame)
			p.mu.Unlock()
		case <-time.After(time.Duration(remaining) * time.Microsecond):
			goto drain
		}
	}
drain:
	p.mu.Lock()
	p.canard.Poll(libcanard.IfaceBitmapAll)
	pending := p.pending
	p.pending = nil
	p.mu.Unlock()
	// Deliver outside the lock: the Cy handler may re-enter the platform
	// (e.g. ACK -> Unicast -> canard.Request).
	onMsg := p.PlatformBase.OnMessage
	if onMsg == nil {
		return nil
	}
	for i := range pending {
		pd := pending[i]
		onMsg(pd.lane, pd.subjectID, *pd.message)
	}
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Misc platform callbacks.
// ---------------------------------------------------------------------------------------------------------------------

// now implements the libcanard Platform.Now callback (func(*Canard) int64).
// Now (below) implements the cy.Platform interface (func() Microsecond).
func (p *Platform) now(self *libcanard.Canard) int64 {
	return time.Now().UnixMicro()
}

// Now returns the current monotonic time in microseconds.
func (p *Platform) Now() cy.Microsecond {
	return cy.Microsecond(time.Now().UnixMicro())
}

// Realloc reallocates memory.
func (p *Platform) Realloc(ptr unsafe.Pointer, size int) unsafe.Pointer {
	if size == 0 {
		return nil
	}
	if ptr == nil {
		b := make([]byte, size)
		return unsafe.Pointer(&b[0])
	}
	oldSlice := (*[1 << 30]byte)(ptr)[:size:size]
	newSlice := make([]byte, size)
	copy(newSlice, oldSlice)
	return unsafe.Pointer(&newSlice[0])
}

// Random returns a deterministic 64-bit pseudo-random value (LCG).
func (p *Platform) Random() uint64 {
	prev := atomic.LoadUint64(&p.randState)
	next := prev*6364136223846793005 + 1442695040888963407
	atomic.StoreUint64(&p.randState, next)
	return next
}

// SetCy sets the Cy instance reference (called by cy.New).
func (p *Platform) SetCy(cyInstance *cy.Cy) {
	p.PlatformBase.Cy = cyInstance
}

// ---------------------------------------------------------------------------------------------------------------------
// Payload helpers (libcanard.Payload lifetime ends on return from OnMessage).
// ---------------------------------------------------------------------------------------------------------------------

func libcanardPayloadLen(payload *libcanard.Payload) int {
	return payload.View.Size
}

func copyPayload(payload *libcanard.Payload) []byte {
	if payload.View.Size <= 0 {
		return nil
	}
	src := unsafe.Slice((*byte)(payload.View.Data), payload.View.Size)
	out := make([]byte, payload.View.Size)
	copy(out, src)
	return out
}

func copyPayloadInto(payload *libcanard.Payload, dst []byte) {
	if payload.View.Size <= 0 {
		return
	}
	src := unsafe.Slice((*byte)(payload.View.Data), payload.View.Size)
	n := copy(dst, src)
	_ = n
}

func binaryLEPutUint64(buf []byte, v uint64) {
	for i := range 8 {
		buf[i] = byte(v >> (8 * uint(i)))
	}
}

// Ensure subject writer/reader types satisfy the cy interfaces.
var _ cy.SubjectWriter = (*subjectWriter)(nil)
var _ cy.SubjectReader = (*subjectReader)(nil)
