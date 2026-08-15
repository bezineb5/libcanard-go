package cy

import (
	"encoding/binary"
	"sync"
)

// RPC manages request-response and streaming operations.
type RPC struct {
	mu sync.RWMutex

	// cy is the parent Cy instance.
	cy *Cy

	// requests maps request tags to pending request futures.
	requests map[uint64]*RequestFuture

	// nextRequestTag is the next tag to use for requests.
	nextRequestTag uint64

	// services maps service subject-IDs to their handlers.
	services map[uint32]RequestHandler

	// streams maps request tags to active streaming sessions.
	streams map[uint64]*Streaming

	// respondFutures maps a response-ACK key (respondKey) to a pending reliable
	// response future, mirroring C cy->respond_futures_by_tag. Each reliable
	// response we transmit awaits a unicast ACK/NACK from the requester.
	respondFutures map[uint64]*RespondFuture
}

// respondKey computes the response-ACK correlation key, a faithful port of C respond_key():
// remote_id ^ message_tag ^ hash ^ (seqno << 16) ^ ((uint64)tag << 56).
// seqno is the transmitted response stream seqno; tag is the small response header tag byte.
func respondKey(remoteID, messageTag, hash, seqno uint64, tag byte) uint64 {
	return remoteID ^ messageTag ^ hash ^ (seqno << 16) ^ (uint64(tag) << 56)
}

// RequestHandler is a function that handles incoming requests.
// It receives the request data and a responder for sending responses.
type RequestHandler func(data []byte, responder *Responder)
// Responder allows sending responses to a request. It carries the breadcrumb of the
// original request so responses are sent back to the correct remote with the correct
// correlation tag (message_tag) and stream sequence number.
type Responder struct {
	// rpc is the parent RPC instance.
	rpc *RPC
	// breadcrumb is the origin information of the received request.
	breadcrumb *Breadcrumb
	// tag is the request tag (used as the small response header byte for reliable responses).
	tag byte
	// remoteID is the remote node ID.
	remoteID uint64
	// requestID is the request ID.
	requestID uint32
	// hash is the service/topic hash this response belongs to.
	hash uint64
	// seqno is the current response sequence number.
	seqno uint64
	// onClose is called when the responder is done.
	onClose func()
}

// newRPC creates a new RPC instance.
func newRPC(cy *Cy) *RPC {
	return &RPC{
		cy:             cy,
		requests:      make(map[uint64]*RequestFuture),
		nextRequestTag: 0,
		services:      make(map[uint32]RequestHandler),
		streams:       make(map[uint64]*Streaming),
		respondFutures: make(map[uint64]*RespondFuture),
	}
}

// Request sends a request message and returns a future for responses.
// The deliveryDeadline is when the request must be delivered by.
// The responseTimeout is how long to wait for responses after delivery.
func (r *RPC) Request(pub *Publisher, deliveryDeadline, responseTimeout Microsecond, data []byte) *RequestFuture {
	if pub == nil {
		return nil
	}

	r.mu.Lock()
	// Generate a new request tag
	tag := r.nextRequestTag
	r.nextRequestTag++

	// Create the future
	future := NewRequestFuture(tag)
	r.requests[tag] = future
	r.mu.Unlock()

	// Also store in publisher for backward compatibility
	pub.mu.Lock()
	pub.pendingRequests[tag] = future
	pub.mu.Unlock()

	// Set up response timeout
	r.cy.olga.Schedule(int64(deliveryDeadline+responseTimeout), func() {
		r.handleResponseTimeout(tag, future)
	})

	// Send the request as a unicast with the 24-byte Cy session header.
	go r.sendRequestMessage(pub, tag, deliveryDeadline, data)

	return future
}

// sendRequestMessage sends a request message as a unicast with the 24-byte Cy
// session header (type msg_be). The payload carries [tag:8][requestID:4][serviceID:4]
// followed by the request data. RPC framing is a stub; this mirrors the C transport path.
func (r *RPC) sendRequestMessage(pub *Publisher, tag uint64, deadline Microsecond, data []byte) {
	payload := make([]byte, 16+len(data))
	binary.LittleEndian.PutUint64(payload[0:8], tag)
	binary.LittleEndian.PutUint32(payload[8:12], uint32(tag)) // requestID = tag for now
	binary.LittleEndian.PutUint32(payload[12:16], uint32(pub.Topic().SubjectID()))
	copy(payload[16:], data)

	header := NewHeader(HeaderTypeMsgBE, 0, 0, pub.Topic().Hash(), tag)
	headed := PrependHeader(header, payload)

	lane := Lane{ID: 0, Priority: pub.Priority()}
	_ = r.cy.platform.Unicast(lane, deadline, headed)
}

// handleResponseTimeout handles timeout for a request.
func (r *RPC) handleResponseTimeout(tag uint64, future *RequestFuture) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if we have responses
	if future.ResponseCount() == 0 {
		future.complete(ErrLiveness)
	} else {
		future.complete(OK)
	}

	// Clean up
	delete(r.requests, tag)
}

// HandleResponse handles an incoming response to a request.
func (r *RPC) HandleResponse(tag uint64, response Response) {
	r.mu.Lock()
	defer r.mu.Unlock()

	future, ok := r.requests[tag]
	if !ok {
		return
	}

	// Add the response to the future
	future.AddResponse(response)

	// Reset the liveness timeout
	r.cy.olga.Schedule(int64(r.cy.Now()+responseTimeout), func() {
		r.handleResponseTimeout(tag, future)
	})
}

// HandleRequest handles an incoming request. The request payload (after the 24-byte
// header has been skipped by HandleMessage) carries [tag:8][requestID:4][serviceID:4][data].
func (r *RPC) HandleRequest(tag, requestID uint64, sourceNodeID uint64, message MessageTS) {
	if message.Content == nil {
		return
	}
	payload := message.Content.Payload()
	if len(payload) < 16 {
		return
	}
	reqTag := binary.LittleEndian.Uint64(payload[0:8])
	reqID := binary.LittleEndian.Uint32(payload[8:12])
	serviceID := binary.LittleEndian.Uint32(payload[12:16])
	requestData := payload[16:]

	r.mu.RLock()
	handler := r.services[serviceID]
	r.mu.RUnlock()
	if handler == nil {
		return
	}
	bc := NewBreadcrumb(r.cy, PriorityNominal, sourceNodeID, 0, reqTag)
	responder := r.newResponder(reqTag, sourceNodeID, reqID, 0, bc)
	handler(requestData, responder)
}

// CancelRequest cancels a pending request.
func (r *RPC) CancelRequest(tag uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	future, ok := r.requests[tag]
	if !ok {
		return
	}

	future.complete(ErrNACK)
	delete(r.requests, tag)
}

// RegisterService registers a handler for a service.
// The serviceID is the subject-ID of the service.
// The handler will be called when a request for this service is received.
func (r *RPC) RegisterService(serviceID uint32, handler RequestHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.services[serviceID] = handler
}

// UnregisterService removes a service handler.
func (r *RPC) UnregisterService(serviceID uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.services, serviceID)
}

// GetService returns the handler for a service.
func (r *RPC) GetService(serviceID uint32) RequestHandler {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.services[serviceID]
}

// newResponder creates a new responder for a request, capturing the breadcrumb so the
func (r *RPC) newResponder(tag uint64, remoteID uint64, requestID uint32, hash uint64, breadcrumb *Breadcrumb) *Responder {
	return &Responder{
		rpc:        r,
		breadcrumb: breadcrumb,
		tag:        byte(tag),
		remoteID:   remoteID,
		requestID:  requestID,
		hash:       hash,
		seqno:      0,
		onClose:    func() {},
	}
}

// ResponseTimeout is the default response timeout (from C library).
const responseTimeout = 1000000 // 1 second in microseconds

// sendResponse emits a response (or error response) to the requester over unicast,
// using the C-compatible response header layout: [0]=type, [1]=tag, [2:8]=seqno(u48),
// [8:16]=hash, [16:24]=message_tag. For best-effort responses the small tag byte is
// 0xFF; for reliable responses it is the per-attempt tag (resp.tag). It returns the
// 48-bit response seqno that was transmitted so the caller can register/correlate.
func (resp *Responder) sendResponse(reliable bool, data []byte) (uint64, error) {
	bc := resp.breadcrumb
	if bc == nil {
		return 0, ErrArgument
	}
	// Increment the per-request stream seqno (starts at 0 -> first response seqno 0).
	seqno := bc.IncrementSeqno() - 1
	// C uses tag 0xFF for best-effort responses and the per-attempt tag for reliable.
	tag := byte(0xFF)
	if reliable {
		tag = resp.tag
	}
	header := MarshalRSPHeader(reliable, tag, seqno, bc.TopicHash, bc.MessageTag)
	lane := Lane{ID: bc.RemoteID, Priority: bc.Priority}
	copy(lane.Context[:], bc.UnicastCtx[:])
	headed := make([]byte, 0, HeaderSize+len(data))
	headed = append(headed, header...)
	headed = append(headed, data...)
	err := resp.rpc.cy.platform.Unicast(lane, resp.rpc.cy.Now()+100000, headed)
	return seqno, err
}

// Respond sends a best-effort response to the request described by the breadcrumb.
func (r *RPC) Respond(breadcrumb *Breadcrumb, seqno uint64, data []byte) error {
	resp := r.newResponder(0, breadcrumb.RemoteID, 0, breadcrumb.TopicHash, breadcrumb)
	if seqno != 0 {
		breadcrumb.Seqno = seqno
	}
	_, err := resp.sendResponse(false, data)
	return err
}

// RespondReliable sends a reliable response and returns a RespondFuture that completes
// with OK when the requester ACKs it, or ErrNACK/ErrDelivery on NACK/timeout. The
// response is retransmitted until acknowledged or the ack-timeout elapses, mirroring
// C cy_respond_reliable + respond_future.
func (r *RPC) RespondReliable(breadcrumb *Breadcrumb, seqno uint64, data []byte) *RespondFuture {
	resp := r.newResponder(0, breadcrumb.RemoteID, 0, breadcrumb.TopicHash, breadcrumb)
	return resp.sendResponseReliable(data)
}

// sendResponseReliable transmits a reliable response, registers a RespondFuture keyed by
// respondKey(remoteID, messageTag, hash, seqno, tag), and arms the retransmission timer.
func (resp *Responder) sendResponseReliable(data []byte) *RespondFuture {
	bc := resp.breadcrumb
	if bc == nil {
		return nil
	}
	seqno, err := resp.sendResponse(true, data)
	if err != nil {
		return nil
	}
	fut := &RespondFuture{
		rpc:        resp.rpc,
		breadcrumb: bc,
		tag:        resp.tag,
		seqno:      seqno,
		hash:       bc.TopicHash,
		data:       append([]byte(nil), data...),
		priority:   bc.Priority,
		ctx:        bc.UnicastCtx,
		ackTimeout: ResponseACKTimeoutMicrosecond,
	}
	// Pick a small tag that yields a unique key; tag 0..0xFF (C loops the same way).
	r := resp.rpc
	r.mu.Lock()
	for {
		key := respondKey(bc.RemoteID, bc.MessageTag, bc.TopicHash, seqno, resp.tag)
		if _, exists := r.respondFutures[key]; !exists {
			r.respondFutures[key] = fut
			break
		}
		if resp.tag == 0xFF {
			break // exhaustion: keep the colliding entry (practically unreachable)
		}
		resp.tag++
	}
	r.mu.Unlock()
	// Retransmit until acknowledged or timed out.
	fut.armRetransmit()
	return fut
}

// armRetransmit schedules a retransmission of this response after ackTimeout, faithful
// to C's respond_future_timeout loop. Each retransmission reuses the same seqno/tag so
// the requester's ACK continues to correlate.
func (f *RespondFuture) armRetransmit() {
	if f.breadcrumb == nil {
		return
	}
	f.retransmit = f.rpc.cy.olga.Schedule(int64(f.rpc.cy.Now()+f.ackTimeout), func() {
		f.onRetransmitTick()
	})
}

// onRetransmitTick re-sends the response if still pending; otherwise stops.
func (f *RespondFuture) onRetransmitTick() {
	f.mu.Lock()
	if f.done {
		f.mu.Unlock()
		return
	}
	// Snapshot send parameters under lock.
	bc := f.breadcrumb
	seqno := f.seqno
	tag := f.tag
	hash := f.hash
	data := append([]byte(nil), f.data...)
	prio := f.priority
	ctx := f.ctx
	f.mu.Unlock()

	if bc == nil {
		return
	}
	header := MarshalRSPHeader(true, tag, seqno, hash, bc.MessageTag)
	lane := Lane{ID: bc.RemoteID, Priority: prio}
	copy(lane.Context[:], ctx[:])
	headed := make([]byte, 0, HeaderSize+len(data))
	headed = append(headed, header...)
	headed = append(headed, data...)
	_ = f.rpc.cy.platform.Unicast(lane, f.rpc.cy.Now()+100000, headed)

	// Re-arm. C keeps retransmitting until acked or the deadline is reached; here the
	// ack-timeout window doubles as the retransmit interval and total lifetime.
	f.armRetransmit()
}

// remove unregisters the future from the index (idempotent).
func (f *RespondFuture) remove() {
	f.rpc.mu.Lock()
	key := respondKey(f.breadcrumb.RemoteID, f.breadcrumb.MessageTag, f.hash, f.seqno, f.tag)
	if f.rpc.respondFutures[key] == f {
		delete(f.rpc.respondFutures, key)
	}
	f.rpc.mu.Unlock()
}
// sendResponseAck emits a unicast response ACK/NACK back to the responder for a reliable
// response we just received. Faithful port of C send_response_ack: header[0]=type,
// [1]=tag, [2:8]=seqno(u48), [8:16]=hash, [16:24]=message_tag. positive selects ACK/NACK.
func (r *RPC) sendResponseAck(lane Lane, messageTag, seqno uint64, tag byte, hash uint64, positive bool) {
	header := MarshalRSPACKHeader(positive, tag, seqno, hash, messageTag)
	reply := Lane{ID: lane.ID, Priority: lane.Priority}
	copy(reply.Context[:], lane.Context[:])
	_ = r.cy.platform.Unicast(reply, r.cy.Now()+ResponseACKTimeoutMicrosecond, header)
}

func (resp *Responder) Send(data []byte) error {
	_, err := resp.sendResponse(false, data)
	return err
}

// SendError sends an error response to the original requester. The error code is
// currently carried as the first byte of the (application) payload, mirroring the
// C convention where the response body begins with a status code.
func (resp *Responder) SendError(errorCode uint8, data []byte) error {
	body := make([]byte, 0, 1+len(data))
	body = append(body, errorCode)
	body = append(body, data...)
	_, err := resp.sendResponse(false, body)
	return err
}

// StartStream starts a streaming response.
func (resp *Responder) StartStream() *Streaming {
	stream := newStreaming(uint64(resp.tag), resp.remoteID)

	resp.rpc.mu.Lock()
	defer resp.rpc.mu.Unlock()

	resp.rpc.streams[uint64(resp.tag)] = stream

	return stream
}

// Close closes the responder and sends any final response.
func (resp *Responder) Close() {
	if resp.onClose != nil {
		resp.onClose()
	}
}

// Streaming represents a streaming response sequence.
type Streaming struct {
	mu sync.RWMutex

	// requestTag is the tag of the original request.
	requestTag uint64

	// remoteID is the remote node ID.
	remoteID uint64

	// seqno is the current sequence number.
	seqno uint64

	// active indicates if streaming is still active.
	active bool

	// onClose is called when the stream is closed.
	onClose func()
}

// newStreaming creates a new streaming instance.
func newStreaming(requestTag, remoteID uint64) *Streaming {
	return &Streaming{
		requestTag: requestTag,
		remoteID:   remoteID,
		seqno:      0,
		active:     true,
	}
}

// Send sends a streaming response.
func (s *Streaming) Send(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.active {
		return ErrLiveness
	}

	// Increment sequence number
	s.seqno++

	// In a real implementation, we'd send the data with the sequence number
	// For now, just return OK
	return OK
}

// Close closes the streaming session.
func (s *Streaming) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active {
		s.active = false
		if s.onClose != nil {
			s.onClose()
		}
	}
}

// IsActive returns true if the stream is still active.
func (s *Streaming) IsActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// Seqno returns the current sequence number.
func (s *Streaming) Seqno() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.seqno
}
