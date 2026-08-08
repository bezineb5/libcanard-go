package cy

import (
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
}

// RequestHandler is a function that handles incoming requests.
// It receives the request data and a responder for sending responses.
type RequestHandler func(data []byte, responder *Responder)

// Responder allows sending responses to a request.
type Responder struct {
	// rpc is the parent RPC instance.
	rpc *RPC
	// tag is the request tag.
	tag uint64
	// remoteID is the remote node ID.
	remoteID uint64
	// requestID is the request ID.
	requestID uint32
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

	// Send the request as a protocol message
	go r.sendRequestMessage(pub, tag, deliveryDeadline, data)

	return future
}

// sendRequestMessage sends a request message as a protocol message.
func (r *RPC) sendRequestMessage(pub *Publisher, tag uint64, deadline Microsecond, data []byte) {
	// Create a request message
	request := &RequestMessage{
		Header: ProtocolHeader{
			MessageType: uint8(ProtocolMessageRequest),
		},
		Tag:          tag,
		SourceNodeID: 0, // Would be the local node ID
		ServiceID:    uint32(pub.Topic().SubjectID()),
		RequestID:    uint32(tag), // Use tag as request ID for now
	}
	
	// Marshal the request header
	requestHeader := request.MarshalBinary()
	
	// Combine header + payload
	payload := append(requestHeader, data...)
	
	// Send via the publisher
	pub.Publish(deadline, payload)
}

// handleResponseTimeout handles timeout for a request.
func (r *RPC) handleResponseTimeout(tag uint64, future *RequestFuture) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if we have responses
	if future.ResponseCount() == 0 {
		// No responses received
		future.complete(ErrLiveness)
	} else {
		// We have at least one response
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
	// The future will be completed when responses stop arriving
	r.cy.olga.Schedule(int64(r.cy.Now()+responseTimeout), func() {
		r.handleResponseTimeout(tag, future)
	})
}

// HandleRequest handles an incoming request.
func (r *RPC) HandleRequest(tag, requestID uint64, sourceNodeID uint64, message MessageTS) {
	// Extract the service ID from the request message
	// The service ID should be in the request header
	if message.Content == nil || len(message.Content.Payload()) < RequestMessageSize {
		return
	}
	
	// Parse the request message to get the service ID
	var req RequestMessage
	err := req.UnmarshalBinary(message.Content.Payload())
	if err != nil {
		return
	}
	
	// Find the service handler
	r.mu.RLock()
	handler := r.services[req.ServiceID]
	r.mu.RUnlock()
	
	if handler == nil {
		// No handler for this service - could send an error response
		return
	}
	
	// Extract the request data (payload after the request header)
	requestData := message.Content.Payload()[RequestMessageSize:]
	
	// Create a responder
	responder := r.newResponder(tag, sourceNodeID, uint32(requestID))
	
	// Invoke the handler
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

// newResponder creates a new responder for a request.
func (r *RPC) newResponder(tag uint64, remoteID uint64, requestID uint32) *Responder {
	return &Responder{
		rpc:      r,
		tag:      tag,
		remoteID:  remoteID,
		requestID: requestID,
		seqno:     0,
		onClose:   func() {},
	}
}

// ResponseTimeout is the default response timeout (from C library).
const responseTimeout = 1000000 // 1 second in microseconds

// Respond sends a response to a request.
// This is called by subscribers when they want to respond to a received request.
func (r *RPC) Respond(breadcrumb *Breadcrumb, seqno uint64, data []byte) error {
	// In a real implementation, we'd:
	// 1. Increment the breadcrumb's seqno
	// 2. Encode the response with the breadcrumb info
	// 3. Send as a unicast message to the requester
	
	// For now, just return OK
	return OK
}

// RespondReliable sends a reliable response to a request.
func (r *RPC) RespondReliable(breadcrumb *Breadcrumb, seqno uint64, data []byte) *PublicationFuture {
	// In a real implementation, we'd:
	// 1. Increment the breadcrumb's seqno
	// 2. Create a reliable message with the response
	// 3. Return a future that tracks delivery
	
	// For now, return nil
	return nil
}

// Send sends a response to the original requester.
func (resp *Responder) Send(data []byte) error {
	// Create a response message
	response := &ResponseMessage{
		Header: ProtocolHeader{
			MessageType: uint8(ProtocolMessageResponse),
		},
		Tag:          resp.tag,
		SourceNodeID: 0, // Would be the local node ID
		RequestID:    resp.requestID,
		Status:      0, // OK
	}
	
	// Marshal the response header
	responseHeader := response.MarshalBinary()
	
	// Combine header + payload
	payload := append(responseHeader, data...)
	
	// Create a lane for unicast to the requester
	lane := Lane{
		ID:       resp.remoteID,
		Priority: PriorityNominal,
	}
	
	// Send the response via unicast
	// Note: This would need the platform to support unicast
	// For now, we'll just return OK
	_ = resp.rpc.cy.platform.Unicast(lane, resp.rpc.cy.Now()+100000, payload)
	
	// Increment sequence number
	resp.seqno++
	
	return OK
}

// SendError sends an error response to the original requester.
func (resp *Responder) SendError(errorCode uint8, data []byte) error {
	// Create a response message with error status
	response := &ResponseMessage{
		Header: ProtocolHeader{
			MessageType: uint8(ProtocolMessageResponse),
		},
		Tag:          resp.tag,
		SourceNodeID: 0, // Would be the local node ID
		RequestID:    resp.requestID,
		Status:      errorCode,
	}
	
	// Marshal the response header
	responseHeader := response.MarshalBinary()
	
	// Combine header + payload
	payload := append(responseHeader, data...)
	
	// Create a lane for unicast to the requester
	lane := Lane{
		ID:       resp.remoteID,
		Priority: PriorityNominal,
	}
	
	// Send the response via unicast
	_ = resp.rpc.cy.platform.Unicast(lane, resp.rpc.cy.Now()+100000, payload)
	
	// Increment sequence number
	resp.seqno++
	
	return OK
}

// StartStream starts a streaming response.
func (resp *Responder) StartStream() *Streaming {
	stream := newStreaming(resp.tag, resp.remoteID)
	
	resp.rpc.mu.Lock()
	defer resp.rpc.mu.Unlock()
	
	resp.rpc.streams[resp.tag] = stream
	
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
		remoteID:    remoteID,
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
