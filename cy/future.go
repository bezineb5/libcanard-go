package cy

import (
	"sync"

	"github.com/opencyphal/cy-go/olga"
)

// Future represents an asynchronous operation that will complete later.
// The future object is passed to user-provided callbacks when the operation completes.
// Future instances are owned by the application; the application is responsible for
// destroying them using Destroy(). Destroying a pending future will cancel the
// associated operation.
type Future interface {
	// Done returns true if the future has completed (successfully or with an error).
	Done() bool
	
	// Error returns the error associated with the future.
	// If Done() is false, this returns the current transient error (if any).
	// If Done() is true, this returns the final error.
	Error() Error
	
	// SetCallback sets a callback to be invoked when the future completes or updates.
	// The callback may be invoked multiple times: once when the future completes,
	// and potentially multiple times for progress updates while pending.
	// If the future is already completed, the callback will be invoked immediately.
	SetCallback(callback func(Future))
	
	// Context returns the user context associated with the future.
	Context() UserContext
	
	// SetContext sets the user context for the future.
	SetContext(context UserContext)
	
	// Destroy destroys the future and cancels the associated operation if pending.
	// A future may be destroyed from within its own callback.
	Destroy()
}

// futureBase is the base implementation for all futures.
type futureBase struct {
	done      bool
	err       Error
	callback  func(Future)
	context   UserContext
	mu        sync.RWMutex
	destroyed bool
}

// Done returns true if the future has completed.
func (f *futureBase) Done() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.done
}

// Error returns the error associated with the future.
func (f *futureBase) Error() Error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.err
}

// SetCallback sets a callback for the future.
func (f *futureBase) SetCallback(callback func(Future)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if callback == nil {
		return
	}

	f.callback = callback

	// If the future is already done, invoke the callback immediately
	if f.done {
		// We need to unlock before calling the callback to avoid deadlocks
		cb := f.callback
		f.mu.Unlock()
		cb(f)
		f.mu.Lock()
	}
}

// Context returns the user context.
func (f *futureBase) Context() UserContext {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.context
}

// SetContext sets the user context.
func (f *futureBase) SetContext(context UserContext) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.context = context
}

// Destroy destroys the future.
func (f *futureBase) Destroy() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.destroyed {
		return
	}

	f.destroyed = true
	f.callback = nil
}

// complete marks the future as done with the specified error.
func (f *futureBase) complete(err Error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		return
	}

	f.done = true
	f.err = err

	// Invoke callback if set
	if f.callback != nil && !f.destroyed {
		cb := f.callback
		// We need to unlock before calling the callback
		f.mu.Unlock()
		cb(f)
		f.mu.Lock()
	}
}

// updateError updates the error without marking as done.
// This is for transient errors.
func (f *futureBase) updateError(err Error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		return
	}

	f.err = err

	// Invoke callback if set
	if f.callback != nil && !f.destroyed {
		cb := f.callback
		f.mu.Unlock()
		cb(f)
		f.mu.Lock()
	}
}

// SubscriptionFuture is a future for subscription operations.
// It represents a subscription that will receive messages.
type SubscriptionFuture struct {
	futureBase
	
	// sub is the associated subscriber.
	sub *Subscriber
	
	// arrival is the last received message arrival.
	arrival *Arrival
	
	// mu protects the arrival field.
	arrivalMu sync.RWMutex
}

// NewSubscriptionFuture creates a new subscription future.
func NewSubscriptionFuture(sub *Subscriber) *SubscriptionFuture {
	return &SubscriptionFuture{
		futureBase: futureBase{},
		sub:       sub,
	}
}

// Arrival returns the last received message arrival.
// Returns nil if no message has been received yet.
func (f *SubscriptionFuture) Arrival() *Arrival {
	f.arrivalMu.RLock()
	defer f.arrivalMu.RUnlock()
	return f.arrival
}

// SetArrival sets the last received message arrival.
func (f *SubscriptionFuture) SetArrival(arrival *Arrival) {
	f.arrivalMu.Lock()
	defer f.arrivalMu.Unlock()
	f.arrival = arrival
}

// Destroy destroys the subscription future.
func (f *SubscriptionFuture) Destroy() {
	f.futureBase.Destroy()
	f.arrivalMu.Lock()
	f.arrival = nil
	f.arrivalMu.Unlock()
}

// PublicationFuture is a future for publication operations.
// It represents a reliable publication that is waiting for acknowledgments.
type PublicationFuture struct {
	futureBase
	
	// tag is the message tag for this publication.
	tag uint64
	
	// ackedCount is the number of acknowledgments received.
	ackedCount int
	
	// totalCount is the total number of expected acknowledgments.
	totalCount int
}

// NewPublicationFuture creates a new publication future.
func NewPublicationFuture(tag uint64, totalCount int) *PublicationFuture {
	return &PublicationFuture{
		futureBase:  futureBase{},
		tag:        tag,
		totalCount: totalCount,
	}
}

// Tag returns the message tag.
func (f *PublicationFuture) Tag() uint64 {
	return f.tag
}

// AckedCount returns the number of acknowledgments received.
func (f *PublicationFuture) AckedCount() int {
	return f.ackedCount
}

// TotalCount returns the total number of expected acknowledgments.
func (f *PublicationFuture) TotalCount() int {
	return f.totalCount
}

// Ack records an acknowledgment from a remote.
func (f *PublicationFuture) Ack() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		return
	}

	f.ackedCount++
	
	// Check if all acknowledgments have been received
	if f.ackedCount >= f.totalCount {
		f.complete(OK)
	}
}

// Nack records a negative acknowledgment from a remote.
func (f *PublicationFuture) Nack() {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.done {
		return
	}

	// For now, we just update the error
	// In a real implementation, we might want to track which remotes NACK'd
	f.updateError(ErrNACK)
}

// from the requester. Faithful port of C respond_future_t: it is keyed by respondKey
// (remoteID, messageTag, hash, seqno, tag) and retransmits the response until the
// requester ACKs or the ack-timeout elapses.
type RespondFuture struct {
	futureBase

	// rpc is the owning RPC manager (used for map removal and retransmit scheduling).
	rpc *RPC

	// breadcrumb is the response origin; its RemoteID/MessageTag/TopicHash/Seqno/UnicastCtx
	// are the correlation fields matched against the incoming response-ACK.
	breadcrumb *Breadcrumb

	// tag is the small response header byte (the per-attempt tag) used in the key.
	tag byte

	// seqno is the response stream seqno that was transmitted.
	seqno uint64

	// hash is the service/topic hash transmitted in the response header.
	hash uint64

	// data is the response payload; retained for retransmission.
	data []byte

	// priority is the lane priority used when transmitting/retransmitting.
	priority Priority

	// ctx is the transport context carried in the lane when transmitting/retransmitting.
	ctx [24]byte

	// ackTimeout is the interval between retransmissions / total lifetime.
	ackTimeout Microsecond

	// retransmit is the scheduled retransmission task; nil when not pending.
	retransmit *olga.Task
}

// onAck is invoked when the requester ACKs (positive) or NACKs (negative) this response.
// Faithful port of C respond_future_on_ack: stop retransmitting, remove from the index,
// set the error, and complete.
func (f *RespondFuture) onAck(positive bool) {
	f.mu.Lock()
	if f.done {
		f.mu.Unlock()
		return
	}
	f.mu.Unlock()

	// Stop retransmissions and unregister before completing.
	if f.retransmit != nil {
		f.rpc.cy.olga.Cancel(f.retransmit)
		f.retransmit = nil
	}
	f.remove()

	var e Error = OK
	if !positive {
		e = ErrNACK
	}
	f.complete(e)
}

// ResponseACKTimeoutMicrosecond is the response-ACK retransmission/timeout window,
// mirroring C ACK_TX_TIMEOUT (1,000,000 us = 1 second).
const ResponseACKTimeoutMicrosecond = 1000000
// RequestFuture is a future for request operations.
// It represents a request that is waiting for responses.
type RequestFuture struct {
	futureBase

	// tag is the message tag for this request.
	tag uint64
	
	// responses contains received responses.
	responses []Response
	
	// responseCount is the total number of responses received.
	responseCount uint64
	
	// mu protects the responses slice.
	responseMu sync.RWMutex
}

// NewRequestFuture creates a new request future.
func NewRequestFuture(tag uint64) *RequestFuture {
	return &RequestFuture{
		futureBase: futureBase{},
		tag:       tag,
		responses: make([]Response, 0),
	}
}

// Tag returns the message tag.
func (f *RequestFuture) Tag() uint64 {
	return f.tag
}

// ResponseCount returns the number of responses received.
func (f *RequestFuture) ResponseCount() uint64 {
	f.responseMu.RLock()
	defer f.responseMu.RUnlock()
	return f.responseCount
}

// Responses returns all received responses.
func (f *RequestFuture) Responses() []Response {
	f.responseMu.RLock()
	defer f.responseMu.RUnlock()
	results := make([]Response, len(f.responses))
	copy(results, f.responses)
	return results
}

// AddResponse adds a new response to the future.
func (f *RequestFuture) AddResponse(response Response) {
	f.responseMu.Lock()
	defer f.responseMu.Unlock()

	f.responses = append(f.responses, response)
	f.responseCount++
	
	// Keep only the most recent response if we have more than one
	// (as per the C implementation, the queue length is 1)
	if len(f.responses) > 1 {
		f.responses = f.responses[len(f.responses)-1:]
	}
}

// BorrowResponse returns the last received response without removing it.
// Returns nil if no response has been received.
func (f *RequestFuture) BorrowResponse() *Response {
	f.responseMu.RLock()
	defer f.responseMu.RUnlock()
	
	if len(f.responses) == 0 {
		return nil
	}
	
	return &f.responses[len(f.responses)-1]
}

// MoveResponse returns the last received response and removes it from the queue.
// Returns nil if no response has been received.
func (f *RequestFuture) MoveResponse() *Response {
	f.responseMu.Lock()
	defer f.responseMu.Unlock()
	
	if len(f.responses) == 0 {
		return nil
	}
	
	// Get the last response
	response := f.responses[len(f.responses)-1]
	
	// Remove it from the slice
	f.responses = f.responses[:len(f.responses)-1]
	
	return &response
}

// Destroy destroys the request future.
func (f *RequestFuture) Destroy() {
	f.futureBase.Destroy()
	f.responseMu.Lock()
	f.responses = nil
	f.responseMu.Unlock()
}

// Response represents a response to a request.
type Response struct {
	// RemoteID uniquely identifies the source node within the network.
	RemoteID uint64
	// Seqno is the sequence number (0 = first response, 1 = second, etc.).
	Seqno uint64
	// Timestamp is the arrival time of the response.
	Timestamp Microsecond
	// Message is the response message.
	Message *MessageTS
}

// Arrival represents a received message from a topic subscription.
type Arrival struct {
	// Message is the received message with timestamp.
	Message MessageTS
	// Breadcrumb contains the origin information for response routing.
	Breadcrumb Breadcrumb
}

// Breadcrumb contains the origin information of a received message
// to allow sending a unicast response back to the sender.
type Breadcrumb struct {
	// Cy is the owning Cy instance.
	Cy *Cy
	// Priority is the priority of the original message.
	Priority Priority
	// RemoteID uniquely identifies the source node within the network.
	RemoteID uint64
	// TopicHash identifies the topic the original request message was received from.
	TopicHash uint64
	// MessageTag is the tag of the original request message this breadcrumb can respond to.
	MessageTag uint64
	// Seqno is incremented with each response sent; starts at zero.
	Seqno uint64
	// UnicastCtx is transport-specific context for sending responses.
	UnicastCtx [24]byte
}

// NewBreadcrumb creates a new breadcrumb.
func NewBreadcrumb(cy *Cy, priority Priority, remoteID, topicHash, messageTag uint64) *Breadcrumb {
	return &Breadcrumb{
		Cy:        cy,
		Priority:  priority,
		RemoteID:   remoteID,
		TopicHash:  topicHash,
		MessageTag: messageTag,
		Seqno:     0,
	}
}

// IncrementSeqno increments the sequence number and returns the new value.
func (b *Breadcrumb) IncrementSeqno() uint64 {
	b.Seqno++
	return b.Seqno
}

// SimpleFuture is a simple future implementation for one-shot operations.
type SimpleFuture struct {
	futureBase
}

// NewSimpleFuture creates a new simple future.
func NewSimpleFuture() *SimpleFuture {
	return &SimpleFuture{
		futureBase: futureBase{},
	}
}

// Complete marks the future as done with the specified error.
func (f *SimpleFuture) Complete(err Error) {
	f.complete(err)
}

// UpdateError updates the error without marking as done.
func (f *SimpleFuture) UpdateError(err Error) {
	f.updateError(err)
}

// IsSuccess returns true if the future completed successfully.
func (f *SimpleFuture) IsSuccess() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.done && f.err == OK
}

// IsError returns true if the future completed with an error.
func (f *SimpleFuture) IsError() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.done && f.err != OK
}
