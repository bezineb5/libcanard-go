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

// Deliver delivers a message to this subscriber.
func (s *Subscriber) Deliver(message MessageTS, lane Lane) {
	s.mu.RLock()
	destroyed := s.destroyed
	callback := s.callback
	s.mu.RUnlock()

	if destroyed {
		return
	}

	// Check for protocol messages (requests, responses)
	if message.Content != nil && len(message.Content.Payload()) >= ProtocolHeaderSize {
		msgType := ProtocolMessageType(message.Content.Payload()[0])
		
		// Handle request messages
		if msgType == ProtocolMessageRequest {
			// Parse the request message
			var req RequestMessage
			err := req.UnmarshalBinary(message.Content.Payload())
			if err == nil {
				// Forward to RPC handler
				s.cy.RPC().HandleRequest(req.Tag, uint64(req.RequestID), req.SourceNodeID, message)
				return // Don't deliver to regular callback
			}
		}
		
		// Handle response messages
		if msgType == ProtocolMessageResponse {
			// Parse the response message
			var resp ResponseMessage
			err := resp.UnmarshalBinary(message.Content.Payload())
			if err == nil {
				// Forward to RPC handler
				s.cy.RPC().HandleResponse(resp.Tag, Response{
					RemoteID:  resp.SourceNodeID,
					Seqno:     uint64(resp.RequestID),
					Timestamp: s.cy.Now(),
					Message:   &message,
				})
				return // Don't deliver to regular callback
			}
		}
	}
	
	// Check for duplicate message (reliable delivery)
	// Extract the tag from the message header if present
	var msgTag uint64 = 0
	if message.Content != nil && message.Content.Size() >= HeaderSize {
		// Try to parse the header
		if header, _, err := ExtractHeader(message.Content); err == nil {
			msgTag = header.Tag
		}
	}

	// Deduplication check
	if msgTag != 0 {
		s.mu.Lock()
		// Check if we've seen this tag before
		if s.receivedTags[msgTag] {
			s.mu.Unlock()
			// Duplicate message - send ACK but don't deliver
			s.sendAck(lane.ID, msgTag)
			return
		}
		
		// Check for gaps (missing messages)
		// If this tag is greater than lastTag+1, we have a gap
		if msgTag > s.lastTag+1 {
			// Send NACK for the missing messages
			// In a real implementation, we'd send NACK for the gap
			// For now, just send NACK for the previous tag
			s.mu.Unlock()
			go s.sendNack(lane.ID, s.lastTag+1)
			
			// Still mark this tag as received and deliver
			s.mu.Lock()
		}
		
		// Mark this tag as received
		s.receivedTags[msgTag] = true
		
		// Clean up old tags periodically
		// Keep only the last N tags (window size)
		const dedupWindowSize = 256
		if len(s.receivedTags) > dedupWindowSize {
			// Remove oldest tags
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
			Cy:        s.cy,
			Priority:  lane.Priority,
			RemoteID:   lane.ID,
			TopicHash:  s.topic.hash,
			MessageTag: msgTag,
			Seqno:     0,
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

// sendAck sends an acknowledgment for a reliable message.
func (s *Subscriber) sendAck(remoteID uint64, tag uint64) {
	// Create an ACK message
	ack := &ACKMessage{
		Header: ProtocolHeader{
			MessageType: uint8(ProtocolMessageACK),
		},
		Tag:          tag,
		SourceNodeID: 0, // Would be the local node ID
	}
	
	// Marshal the ACK message
	ackData := ack.MarshalBinary()
	
	// Send via unicast to the remote node
	lane := Lane{
		ID:       remoteID,
		Priority: PriorityNominal,
	}
	
	// Send the ACK message
	_ = s.cy.platform.Unicast(lane, s.cy.Now()+100000, ackData)
}

// sendNack sends a negative acknowledgment for a missing message.
func (s *Subscriber) sendNack(remoteID uint64, tag uint64) {
	// Create a NACK message
	nack := &NACKMessage{
		Header: ProtocolHeader{
			MessageType: uint8(ProtocolMessageNACK),
		},
		Tag:          tag,
		SourceNodeID: 0, // Would be the local node ID
		ErrorCode:    ErrNACK, // Indicate missing message
	}
	
	// Marshal the NACK message
	nackData := nack.MarshalBinary()
	
	// Send via unicast to the remote node
	lane := Lane{
		ID:       remoteID,
		Priority: PriorityNominal,
	}
	
	// Send the NACK message
	_ = s.cy.platform.Unicast(lane, s.cy.Now()+100000, nackData)
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
