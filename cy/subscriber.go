package cy

import (
	"sync"

	"github.com/opencyphal/cy-go/olga"
)

// DefaultReorderingWindow is the reordering window used by ordered subscribers
// created without an explicit window (via NewOrderedSubscriber). It is the time a
// missing message is waited for before the gap is closed. Mirrors C's default.
const DefaultReorderingWindow = Microsecond(100000)

// Subscriber represents a subscriber to a specific topic (or topic pattern).
// Subscribers receive messages published to the topic.
type Subscriber struct {
	// cy is the owning Cy instance.
	cy *Cy
	// name is the subscription name (the resolved verbatim topic name, or the
	// pattern). Exposed via Name(); mirrors C cy_subscriber_name.
	name string
	// topic is the verbatim topic this subscriber is subscribed to (nil for
	// pattern subscribers).
	topic *Topic
	// extent is the maximum message size to accept.
	extent int
	// callback is the function to call when a message is received.
	callback func(*Arrival)
	// future is the future for this subscription.
	future *SubscriptionFuture

	// pattern is the pattern string for pattern subscribers ("" otherwise).
	pattern string

	// reorderingWindow is the ordered-delivery window in microseconds. A value of
	// -1 disables reordering (unordered delivery). A non-negative value enables
	// strictly-increasing in-order delivery per (remote, topic), mirroring C
	// subscriber_params_t.reordering_window (where -1 means unordered).
	reorderingWindow Microsecond
	// livenessTimeout is the liveness monitoring timeout. A value of 0 disables
	// liveness monitoring. Otherwise the future is marked failed with ErrLiveness
	// if no message arrives within the timeout. Mirrors C liveness_timeout.
	livenessTimeout Microsecond

	// delivery state (protected by mu).
	lastArrival  *Arrival
	messageCount uint64
	err          Error
	livenessTask *olga.Task

	// reordering holds per-(remote,topic) in-order delivery buffers.
	reordering map[reorderingKey]*reorderingState

	// mu protects the subscriber delivery state. Callbacks are invoked after this
	// lock is released to avoid reentrancy deadlocks.
	mu sync.RWMutex

	// disposed indicates whether the subscriber has been destroyed.
	disposed bool
}

// newSubscriber creates a verbatim subscriber for the given topic.
func newSubscriber(cy *Cy, topic *Topic, extent int, reorderingWindow Microsecond) *Subscriber {
	sub := &Subscriber{
		cy:               cy,
		name:             topic.name,
		topic:            topic,
		extent:           extent,
		reorderingWindow: reorderingWindow,
		reordering:       make(map[reorderingKey]*reorderingState),
	}
	sub.future = NewSubscriptionFuture(sub)
	return sub
}

// NewSubscriber creates a new (unordered) subscriber for the specified topic.
func NewSubscriber(cy *Cy, topic *Topic, extent int) *Subscriber {
	return newSubscriber(cy, topic, extent, -1)
}

// NewOrderedSubscriber creates a new ordered subscriber with the default window.
func NewOrderedSubscriber(cy *Cy, topic *Topic, extent int) *Subscriber {
	return newSubscriber(cy, topic, extent, DefaultReorderingWindow)
}

// newPatternSubscriber creates a pattern subscriber.
func newPatternSubscriber(cy *Cy, pattern string, extent int) *Subscriber {
	sub := &Subscriber{
		cy:               cy,
		name:             pattern,
		pattern:          pattern,
		extent:           extent,
		reorderingWindow: -1,
		reordering:       make(map[reorderingKey]*reorderingState),
	}
	sub.future = NewSubscriptionFuture(sub)
	return sub
}

// NewPatternSubscriber creates a new pattern subscriber.
func NewPatternSubscriber(cy *Cy, pattern string, extent int) *Subscriber {
	return newPatternSubscriber(cy, pattern, extent)
}

// Topic returns the verbatim topic this subscriber is subscribed to (nil for
// pattern subscribers).
func (s *Subscriber) Topic() *Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topic
}

// Name returns the subscription name: the verbatim topic name for ordinary
// subscribers, or the pattern for pattern subscribers. Mirrors C cy_subscriber_name.
func (s *Subscriber) Name() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.name
}

// Extent returns the maximum message size this subscriber accepts.
func (s *Subscriber) Extent() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.extent
}

// SetExtent sets the maximum message size this subscriber accepts.
func (s *Subscriber) SetExtent(extent int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extent = extent
}

// SetCallback sets the callback to be invoked when a message is received.
func (s *Subscriber) SetCallback(callback func(*Arrival)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callback = callback
}

// Future returns the future for this subscription.
func (s *Subscriber) Future() *SubscriptionFuture {
	return s.future
}

// IsPattern returns true if this is a pattern subscriber.
func (s *Subscriber) IsPattern() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pattern != ""
}

// Pattern returns the pattern string for pattern subscribers ("" otherwise).
func (s *Subscriber) Pattern() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pattern
}

// IsOrdered returns true if this is an ordered (reordering) subscriber.
func (s *Subscriber) IsOrdered() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reorderingWindow >= 0
}

// ReorderingWindow returns the reordering window (Microsecond); -1 if unordered.
func (s *Subscriber) ReorderingWindow() Microsecond {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reorderingWindow
}

// SetLivenessTimeout arms (timeout>0) or disarms (timeout==0) liveness monitoring.
// Mirrors C cy_subscriber_timeout_set: when disarmed the error is cleared; when
// armed the deadline is set relative to the last received message (or time 0).
func (s *Subscriber) SetLivenessTimeout(timeout Microsecond) {
	s.mu.Lock()
	s.livenessTimeout = timeout
	if timeout == 0 {
		s.disarmLivenessLocked()
		s.err = OK
		s.mu.Unlock()
		s.future.notifyCallback()
		return
	}
	base := Microsecond(0)
	if s.lastArrival != nil {
		base = s.lastArrival.Message.Timestamp
	}
	s.mu.Unlock()
	s.armLiveness(base + timeout)
	s.future.notifyCallback()
}

// Match checks if a topic name matches this subscriber's pattern (or equals the
// verbatim topic name). Returns substitutions for pattern subscribers.
func (s *Subscriber) Match(topicName string) (bool, []string) {
	if !s.IsPattern() {
		s.mu.RLock()
		t := s.topic
		s.mu.RUnlock()
		if t == nil {
			return false, nil
		}
		return t.name == topicName, nil
	}
	return MatchPattern(s.pattern, topicName)
}

// Substitutions returns the per-wildcard substitutions for the given matched
// topic, one entry per wildcard ordinal (with '>' joined by '/'). Returns nil
// for verbatim subscribers. Mirrors C cy_subscriber_substitutions.
func (s *Subscriber) Substitutions(topic *Topic) []string {
	if !s.IsPattern() || topic == nil {
		return nil
	}
	_, subs := MatchPattern(s.pattern, topic.Name())
	return subs
}

// deliver routes an incoming message to this subscriber, applying dedup
// (topic-level, done by the caller), reordering (if ordered), and delivery. It
// returns true if the message is accepted and should be acknowledged.
func (s *Subscriber) deliver(message MessageTS, lane Lane, msgTag uint64, topic *Topic) bool {
	s.mu.RLock()
	disposed := s.disposed
	window := s.reorderingWindow
	s.mu.RUnlock()
	if disposed {
		return false
	}

	if window >= 0 {
		key := reorderingKey{remoteID: lane.ID, topicHash: topic.Hash()}
		rr := s.getOrCreateReordering(key, lane.ID, topic.Hash(), msgTag)
		rr.unicastCtx = lane.Context
		rr.lastActiveAt = message.Timestamp
		return rr.push(msgTag, lane.Priority, message)
	}

	// Unordered delivery: deliver immediately.
	return s.notify(&Arrival{
		Message: message,
		Breadcrumb: Breadcrumb{
			Cy:         s.cy,
			Priority:   lane.Priority,
			RemoteID:   lane.ID,
			TopicHash:  topic.Hash(),
			MessageTag: msgTag,
			UnicastCtx: lane.Context,
		},
	})
}

// DeliverForTesting injects a fully-reassembled transfer directly into this
// subscriber, bypassing the transport layer and Cy's topic routing. It is an
// escape hatch for test harnesses and replay only; production code must never
// call it. In production, transfers arrive via the platform (cy_on_message).
// The subscriber's own topic is used for breadcrumb tagging.
func (s *Subscriber) DeliverForTesting(message MessageTS, lane Lane, msgTag uint64) bool {
	s.mu.RLock()
	topic := s.topic
	s.mu.RUnlock()
	return s.deliver(message, lane, msgTag, topic)
}

// notify records the arrival, advances delivery state, arms liveness, and invokes
// the delivery callback. Returns false if the subscriber was disposed.
func (s *Subscriber) notify(arrival *Arrival) bool {
	s.mu.Lock()
	if s.disposed {
		s.mu.Unlock()
		return false
	}
	s.lastArrival = arrival
	s.messageCount++
	if s.livenessTimeout > 0 {
		s.armLivenessLocked(arrival.Message.Timestamp + s.livenessTimeout)
	}
	s.err = OK
	cb := s.callback
	s.mu.Unlock()

	if cb != nil {
		cb(arrival)
	}
	s.future.notifyCallback()
	return true
}

// notifyError sets the future error and notifies the future callback.
func (s *Subscriber) notifyError(err Error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	s.future.notifyCallback()
}

// armLiveness schedules the liveness timer at the given absolute deadline.
func (s *Subscriber) armLiveness(deadline Microsecond) {
	s.mu.Lock()
	s.armLivenessLocked(deadline)
	s.mu.Unlock()
}

func (s *Subscriber) armLivenessLocked(deadline Microsecond) {
	if s.livenessTask != nil {
		s.cy.olga.Cancel(s.livenessTask)
	}
	s.livenessTask = s.cy.olga.Schedule(int64(deadline), s.onLivenessTimeout)
}

func (s *Subscriber) disarmLiveness() {
	s.mu.Lock()
	s.disarmLivenessLocked()
	s.mu.Unlock()
}

func (s *Subscriber) disarmLivenessLocked() {
	if s.livenessTask != nil {
		s.cy.olga.Cancel(s.livenessTask)
		s.livenessTask = nil
	}
}

// onLivenessTimeout is the liveness/window-deferred timer callback. It runs in
// the olga goroutine (no c.mu held).
func (s *Subscriber) onLivenessTimeout() {
	s.mu.Lock()
	s.livenessTask = nil
	disposed := s.disposed
	s.mu.Unlock()
	if disposed {
		s.teardown()
		return
	}
	s.notifyError(ErrLiveness)
}

// --- subscription-future state accessors ---

func (s *Subscriber) isDone() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastArrival != nil && s.lastArrival.Message.Content != nil {
		return true
	}
	return s.livenessTimeout > 0 && s.livenessTask == nil
}

func (s *Subscriber) error() Error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

func (s *Subscriber) lastArrivalSnapshot() *Arrival {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastArrival
}

// arrivalMove transfers the last arrival to the caller and clears the future's
// reference to it (so Done() flips back to pending), mirroring C cy_arrival_move.
func (s *Subscriber) arrivalMove() *Arrival {
	s.mu.Lock()
	out := s.lastArrival
	if out != nil {
		s.lastArrival = &Arrival{
			Message:    MessageTS{Timestamp: out.Message.Timestamp},
			Breadcrumb: out.Breadcrumb,
		}
	}
	s.mu.Unlock()
	return out
}

func (s *Subscriber) arrivalCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.messageCount
}

// --- reordering state management ---

func (s *Subscriber) getOrCreateReordering(key reorderingKey, remoteID, topicHash, tag uint64) *reorderingState {
	if rr, ok := s.reordering[key]; ok {
		return rr
	}
	rr := &reorderingState{
		subscriber: s,
		remoteID:   remoteID,
		topicHash:  topicHash,
		slots:      make(map[uint64]*reorderingSlot),
	}
	rr.resequence(tag)
	s.reordering[key] = rr
	return rr
}

func (s *Subscriber) removeReordering(key reorderingKey) {
	delete(s.reordering, key)
}

// dropStaleReordering reaps reordering state for remotes that have stopped
// publishing. Mirrors C reordering_drop_stale.
func (s *Subscriber) dropStaleReordering(now Microsecond) {
	var stale []reorderingKey
	for k, rr := range s.reordering {
		if now-rr.lastActiveAt > SessionLifetime {
			stale = append(stale, k)
		}
	}
	for _, k := range stale {
		if rr := s.reordering[k]; rr != nil {
			rr.destroy(true)
		}
	}
}

// Destroy destroys the subscriber, removing it from the topic/pattern registry.
func (s *Subscriber) Destroy() {
	s.mu.Lock()
	if s.disposed {
		s.mu.Unlock()
		return
	}
	s.disposed = true
	s.mu.Unlock()
	s.teardown()
}

// teardownLocked performs the irreversible removal of the subscriber. The caller
// MUST hold c.mu. Use Destroy() (public) or teardown() (acquires c.mu) otherwise.
func (s *Subscriber) teardownLocked() {
	s.mu.Lock()
	s.disarmLivenessLocked()
	for _, rr := range s.reordering {
		rr.destroy(true)
	}
	s.reordering = make(map[reorderingKey]*reorderingState)
	pattern := s.pattern
	topic := s.topic
	s.mu.Unlock()

	if pattern != "" {
		s.cy.patternMatcher.RemovePattern(pattern, s)
	} else if topic != nil {
		if subs, ok := s.cy.subscribers[topic]; ok {
			for i, sub := range subs {
				if sub == s {
					s.cy.subscribers[topic] = append(subs[:i], subs[i+1:]...)
					break
				}
			}
		}
	}

	if s.future != nil {
		s.future.notifyCallback()
	}
}

// teardown performs the irreversible removal of the subscriber, acquiring c.mu.
// It is used from the public Destroy() and the deferred-dispose timer (olga
// goroutine), neither of which holds c.mu.
func (s *Subscriber) teardown() {
	s.cy.mu.Lock()
	defer s.cy.mu.Unlock()
	s.teardownLocked()
}
