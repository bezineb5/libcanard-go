package cy

import (
	"sync"
)

// Subscriber represents a subscriber to a specific topic.
// Subscribers receive messages published to the topic.
type Subscriber struct {
	// cy is the owning Cy instance.
	cy *Cy
	// topic is the topic this subscriber is subscribed to.
	topic *Topic
	// extent is the maximum message size to accept.
	extent int
	// callback is the function to call when a message is received.
	callback func(*Arrival)
	// future is the future for this subscription.
	future *SubscriptionFuture
	
	// For pattern subscribers
	// pattern is the pattern string (if this is a pattern subscription).
	pattern string
	// compiledPattern is the parsed pattern for efficient matching.
	compiledPattern []PatternSegment

	// For ordered subscribers
	// ordered indicates whether this is an ordered subscriber.
	ordered bool
	// nextSeqno is the next expected sequence number for ordering.
	nextSeqno uint64
	
	// For reliable delivery
	// lastTag is the last received message tag (for deduplication).
	lastTag uint64
	// receivedTags tracks recently received tags to detect duplicates.
	// This is a simple implementation; a more efficient approach would use
	// a bitmap or circular buffer for large tag spaces.
	receivedTags map[uint64]bool
	
	// mu protects the subscriber state.
	mu sync.RWMutex
	
	// destroyed indicates whether the subscriber has been destroyed.
	destroyed bool
}

// NewSubscriber creates a new subscriber for the specified topic.
func NewSubscriber(cy *Cy, topic *Topic, extent int) *Subscriber {
	sub := &Subscriber{
		cy:           cy,
		topic:        topic,
		extent:       extent,
		receivedTags: make(map[uint64]bool),
	}
	
	// Create the future
	sub.future = NewSubscriptionFuture(sub)
	
	return sub
}

// NewPatternSubscriber creates a new pattern subscriber.
func NewPatternSubscriber(cy *Cy, pattern string, extent int) *Subscriber {
	sub := NewSubscriber(cy, nil, extent)
	sub.pattern = pattern
	sub.compiledPattern = compilePattern(pattern)
	return sub
}

// NewOrderedSubscriber creates a new ordered subscriber.
func NewOrderedSubscriber(cy *Cy, topic *Topic, extent int) *Subscriber {
	sub := NewSubscriber(cy, topic, extent)
	sub.ordered = true
	return sub
}

// compilePattern compiles a pattern string into segments.
func compilePattern(pattern string) []PatternSegment {
	segments := []PatternSegment{}
	
	// Split by '/' and process each segment
	parts := splitPattern(pattern)
	for _, part := range parts {
		if part == "*" {
			segments = append(segments, PatternSegment{
				Wildcard: true,
				Multi:    false,
			})
		} else if part == ">" {
			segments = append(segments, PatternSegment{
				Wildcard: true,
				Multi:    true,
			})
		} else {
			segments = append(segments, PatternSegment{
				Literal:  part,
				Wildcard: false,
				Multi:    false,
			})
		}
	}
	
	return segments
}

// splitPattern splits a pattern into segments, handling wildcards.
func splitPattern(pattern string) []string {
	var parts []string
	var current string
	
	for _, c := range pattern {
		if c == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else if c == '*' || c == '>' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
			parts = append(parts, string(c))
		} else {
			current += string(c)
		}
	}
	
	if current != "" {
		parts = append(parts, current)
	}
	
	return parts
}

// Topic returns the topic this subscriber is subscribed to.
func (s *Subscriber) Topic() *Topic {
	return s.topic
}

// Extent returns the maximum message size this subscriber accepts.
func (s *Subscriber) Extent() int {
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

// Destroy destroys the subscriber.
func (s *Subscriber) Destroy() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.destroyed {
		return
	}

	s.destroyed = true

	// Destroy the future
	if s.future != nil {
		s.future.Destroy()
		s.future = nil
	}

	s.callback = nil
}

// Deliver delivers a message to this subscriber. msgTag is the session-layer
// message tag, extracted by HandleMessage before the 24-byte header is skipped.
func (s *Subscriber) Deliver(message MessageTS, lane Lane, msgTag uint64) {
	s.mu.RLock()
	destroyed := s.destroyed
	callback := s.callback
	s.mu.RUnlock()

	if destroyed {
		return
	}

	// Deduplication check
	if msgTag != 0 {
		s.mu.Lock()
		// Check if we've seen this tag before
		if s.receivedTags[msgTag] {
			s.mu.Unlock()
			// Duplicate message - send ACK but don't deliver.
			// Async: sendAck re-enters the platform (Now + Unicast) and must
			// not run while the caller holds the network/platform lock.
			go s.sendAck(lane.ID, msgTag)
			return
		}
		
		// Check for gaps (missing messages)
		// If this tag is greater than lastTag+1, we have a gap
		if msgTag > s.lastTag+1 {
			// Send NACK for the missing messages
			s.mu.Unlock()
			go s.sendNack(lane.ID, s.lastTag+1)
			
			// Still mark this tag as received and deliver
			s.mu.Lock()
		}
		
		// Mark this tag as received
		s.receivedTags[msgTag] = true
		
		// Clean up old tags periodically
		const dedupWindowSize = 256
		if len(s.receivedTags) > dedupWindowSize {
			for tag := range s.receivedTags {
				if tag <= s.lastTag-dedupWindowSize {
					delete(s.receivedTags, tag)
				}
			}
		}
		
		// Update last tag
		if msgTag > s.lastTag {
			s.lastTag = msgTag
		}
		s.mu.Unlock()
		
		// Send ACK for reliable messages
		go s.sendAck(lane.ID, msgTag)
	}
	
	// Create an arrival
	arrival := &Arrival{
		Message: message,
		Breadcrumb: Breadcrumb{
			Cy:         s.cy,
			Priority:   lane.Priority,
			RemoteID:   lane.ID,
			TopicHash:  s.topic.hash,
			MessageTag: msgTag,
			Seqno:      0,
			UnicastCtx: lane.Context,
		},
	}

	// Update the future
	s.future.SetArrival(arrival)
	s.future.complete(OK)

	// Invoke the callback if set
	if callback != nil {
		callback(arrival)
	}
}

// sendAck sends a positive acknowledgement for a reliable message (C header_msg_ack).
func (s *Subscriber) sendAck(remoteID uint64, tag uint64) {
	header := NewACKHeader(true, s.topic.Hash(), tag)
	lane := Lane{ID: remoteID, Priority: PriorityNominal}
	_ = s.cy.platform.Unicast(lane, s.cy.Now()+100000, header.MarshalBinary())
}

// sendNack sends a negative acknowledgement for a missing message (C header_msg_nack).
func (s *Subscriber) sendNack(remoteID uint64, tag uint64) {
	header := NewACKHeader(false, s.topic.Hash(), tag)
	lane := Lane{ID: remoteID, Priority: PriorityNominal}
	_ = s.cy.platform.Unicast(lane, s.cy.Now()+100000, header.MarshalBinary())
}

// IsPattern returns true if this is a pattern subscriber.
func (s *Subscriber) IsPattern() bool {
	return s.pattern != ""
}

// Pattern returns the pattern string for pattern subscribers.
func (s *Subscriber) Pattern() string {
	return s.pattern
}

// IsOrdered returns true if this is an ordered subscriber.
func (s *Subscriber) IsOrdered() bool {
	return s.ordered
}

// Match checks if a topic name matches this subscriber's pattern.
// Returns true and substitutions if it matches.
func (s *Subscriber) Match(topicName string) (bool, []string) {
	if !s.IsPattern() {
		return s.topic.name == topicName, nil
	}
	
	return MatchPattern(s.pattern, topicName)
}

// Substitutions returns the substitutions for a matched topic.
// This is only valid for pattern subscribers after a match.
func (s *Subscriber) Substitutions(topic *Topic) []Substitution {
	// In a real implementation, this would return the substitutions
	// from the pattern match
	return nil
}
