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

// notifyCallback invokes the future's callback (if any and not destroyed),
// without marking the future done. Used by futures whose completion state is
// computed on demand (e.g. SubscriptionFuture). Caller must not hold f.mu.
func (f *futureBase) notifyCallback() {
	f.mu.Lock()
	cb := f.callback
	destroyed := f.destroyed
	f.mu.Unlock()
	if cb != nil && !destroyed {
		cb(f)
	}
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
// Its completion state is computed from the subscriber: a future is "done" when
// a message has been received (or, for liveness-monitored subscriptions, when no
// message arrives within the liveness timeout). Consuming the arrival via
// ArrivalMove flips it back to pending, faithfully modeling the sampling-port
// contract.
type SubscriptionFuture struct {
	futureBase

	// sub is the associated subscriber.
	sub *Subscriber
}

// NewSubscriptionFuture creates a new subscription future.
func NewSubscriptionFuture(sub *Subscriber) *SubscriptionFuture {
	return &SubscriptionFuture{
		futureBase: futureBase{},
		sub:        sub,
	}
}

// Done returns true if the subscription has a pending arrival or has timed out
// on liveness. It flips back to false after ArrivalMove consumes the arrival.
func (f *SubscriptionFuture) Done() bool { return f.sub.isDone() }

// Error returns the current (or final) error of the subscription.
func (f *SubscriptionFuture) Error() Error { return f.sub.error() }

// Arrival returns the last received message arrival (nil if none).
func (f *SubscriptionFuture) Arrival() *Arrival { return f.sub.lastArrivalSnapshot() }

// ArrivalMove moves the last arrival out of the future (transferring ownership)
// and clears the future's reference so Done() flips back to pending.
// Mirrors C cy_arrival_move.
func (f *SubscriptionFuture) ArrivalMove() *Arrival { return f.sub.arrivalMove() }

// ArrivalCount returns the number of messages delivered so far.
// Mirrors C cy_arrival_count.
func (f *SubscriptionFuture) ArrivalCount() uint64 { return f.sub.arrivalCount() }

// SubscriberName returns the subscription name (verbatim topic name or pattern).
// Mirrors C cy_subscriber_name.
func (f *SubscriptionFuture) SubscriberName() string { return f.sub.Name() }

// Substitutions returns the per-wildcard substitutions for the given matched
// topic. Mirrors C cy_subscriber_substitutions.
func (f *SubscriptionFuture) Substitutions(topic *Topic) []string {
	return f.sub.Substitutions(topic)
}

// SetCallback sets a callback to be invoked when the subscription updates
// (each new arrival / liveness timeout). If the subscription is already done, the
// callback is invoked immediately.
func (f *SubscriptionFuture) SetCallback(callback func(Future)) {
	f.futureBase.mu.Lock()
	f.futureBase.callback = callback
	f.futureBase.mu.Unlock()
	if f.Done() {
		f.futureBase.notifyCallback()
	}
}

// Destroy destroys the subscription future.
func (f *SubscriptionFuture) Destroy() {
	f.futureBase.Destroy()
}

// PublicationFuture is a future for publication operations.
// It represents a reliable publication that is waiting for acknowledgments.
//
// Faithful port of the C reliable-publish completion model: a publication
// completes with OK once it has been acknowledged (any ACK at all) AND every
// known subscriber association at publish time has explicitly ACKed it. NACKs
// are treated as gaps to be repaired by the retransmission cycle, not as
// terminal errors: they never complete or fail the future.
type PublicationFuture struct {
	futureBase

	// tag is the message tag for this publication (the wire tag).
	tag uint64

	// totalCount is the number of known subscriber associations captured at
	// publish time. It is the denominator for completion: all of them must
	// ACK before the future can succeed (alongside any confirming ACK).
	totalCount int

	// ackedCount is the number of known associations that have ACKed so far.
	ackedCount int

	// acknowledged is set true on the first (any) ACK, including from a remote
	// that was not a known association at publish time.
	acknowledged bool
}

// NewPublicationFuture creates a new publication future for the given wire tag
// and known-association count.
func NewPublicationFuture(tag uint64, totalCount int) *PublicationFuture {
	return &PublicationFuture{
		futureBase: futureBase{},
		tag:        tag,
		totalCount: totalCount,
	}
}

// NewFailedPublicationFuture returns a publication future that is already
// completed with the given error (e.g. when the initial send fails).
func NewFailedPublicationFuture(err Error) *PublicationFuture {
	f := &PublicationFuture{futureBase: futureBase{}, tag: 0, totalCount: 0}
	f.complete(err)
	return f
}

// Tag returns the message tag.
func (f *PublicationFuture) Tag() uint64 {
	return f.tag
}

// AckedCount returns the number of known associations that have acknowledged.
func (f *PublicationFuture) AckedCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.ackedCount
}

// TotalCount returns the number of known associations expected to acknowledge
// at publish time.
func (f *PublicationFuture) TotalCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.totalCount
}

// Acknowledged reports whether at least one ACK has been received for this
// publication (including from an unknown remote).
func (f *PublicationFuture) Acknowledged() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.acknowledged
}

// Ack records an acknowledgment from a known association and confirms the
// publication. Completes with OK once every known association has ACKed and at
// least one ACK (this or any other) has been seen.
func (f *PublicationFuture) Ack() {
	f.mu.Lock()
	if f.done {
		f.mu.Unlock()
		return
	}
	f.ackedCount++
	f.acknowledged = true
	should := f.acknowledged && f.ackedCount >= f.totalCount
	f.mu.Unlock()
	if should {
		f.complete(OK)
	}
}

// Acknowledge confirms the publication on any ACK (including from a remote that
// was not a known association at publish time). Completes with OK once the
// publication is acknowledged and every known association has also ACKed.
// Caller must NOT hold f.mu.
func (f *PublicationFuture) Acknowledge() {
	f.mu.Lock()
	if f.done {
		f.mu.Unlock()
		return
	}
	f.acknowledged = true
	should := f.acknowledged && f.ackedCount >= f.totalCount
	f.mu.Unlock()
	if should {
		f.complete(OK)
	}
}

// Nack is a no-op under the faithful C model: a NACK is a gap signal, repaired
// by the retransmission cycle, never a terminal error. It neither completes nor
// fails the future.
func (f *PublicationFuture) Nack() {
	f.mu.Lock()
	defer f.mu.Unlock()
	_ = f.done
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

	// rpc is the owning RPC, used to hand off the ack record at Destroy.
	rpc *RPC

	// responses contains received responses.
	responses []Response

	// responseCount is the total number of responses received.
	responseCount uint64

	// mu protects the responses slice.
	responseMu sync.RWMutex

	// ack is the requester-side reliable-response dedup record, lazily created on the first
	// reliable response. Handed off to RPC at Destroy so retransmits keep getting answered.
	ack *requestAck
}

// NewRequestFuture creates a new request future.
func NewRequestFuture(rpc *RPC, tag uint64) *RequestFuture {
	return &RequestFuture{
		futureBase: futureBase{},
		tag:        tag,
		rpc:        rpc,
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

// Destroy destroys the request future. Faithful to C request_future_dispose: the future is
// de-indexed from the RPC at dispose (so post-destroy retransmits are answered by the handed-off
// record, never re-delivered into a dead future), and any reliable-response dedup record that
// acked something is retained so retransmitted reliable responses keep getting answered.
func (f *RequestFuture) Destroy() {
	f.responseMu.Lock()
	ack := f.ack
	f.responses = nil
	f.responseMu.Unlock()

	if f.rpc != nil {
		f.rpc.mu.Lock()
		// De-index at dispose (C future_index_remove). Release responseMu first to avoid a
		// lock-order inversion with handleResponseCorrelation (which holds r.mu then responseMu).
		delete(f.rpc.requests, f.tag)
		if ack != nil && (ack.solo || ack.tree != nil) {
			f.rpc.retainRequestAck(ack, f.rpc.cy.Now())
		}
		f.ack = nil
		f.rpc.mu.Unlock()
	}

	f.futureBase.Destroy()
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
		Cy:         cy,
		Priority:   priority,
		RemoteID:   remoteID,
		TopicHash:  topicHash,
		MessageTag: messageTag,
		Seqno:      0,
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
