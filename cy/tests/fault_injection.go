// Package tests provides fault injection capabilities for testing reliability.
// This framework allows simulating real-world failures like message loss,
// reordering, duplication, and delays.
package tests

import (
	"math/rand"
	"sync"

	"github.com/opencyphal/cy-go"
)

// FaultInjectionMode represents the type of fault to inject.
type FaultInjectionMode int

const (
	// FaultNone - No faults (normal operation)
	FaultNone FaultInjectionMode = iota
	// FaultMessageLoss - Randomly drop messages
	FaultMessageLoss
	// FaultMessageReordering - Deliver messages out of order
	FaultMessageReordering
	// FaultMessageDuplication - Deliver duplicate messages
	FaultMessageDuplication
	// FaultMessageDelay - Delay message delivery
	FaultMessageDelay
	// FaultAll - All faults combined
	FaultAll
)

// FaultInjector injects faults into message delivery.
type FaultInjector struct {
	mu sync.RWMutex
	
	// mode is the current fault injection mode.
	mode FaultInjectionMode
	
	// lossProbability is the probability (0.0 to 1.0) of dropping a message.
	lossProbability float64
	
	// reorderWindow is the maximum number of messages to reorder.
	reorderWindow int
	
	// dupProbability is the probability (0.0 to 1.0) of duplicating a message.
	dupProbability float64
	
	// delayRange is the range of delay in microseconds.
	delayRange cy.Microsecond
	
	// random is the random number generator.
	random *rand.Rand
	
	// messageQueue holds messages for reordering.
	messageQueue []*faultInjectionMessage
	
	// enabled indicates if fault injection is active.
	enabled bool
}

// faultInjectionMessage wraps a message with metadata for fault injection.
type faultInjectionMessage struct {
	// message is the original message.
	message SimMessage
	// originalIndex is the index when the message was queued.
	originalIndex int
	// delivered indicates if the message has been delivered.
	delivered bool
}

// NewFaultInjector creates a new fault injector with default settings.
func NewFaultInjector() *FaultInjector {
	return &FaultInjector{
		mode:            FaultNone,
		lossProbability: 0.1, // 10% loss by default
		reorderWindow:   5,
		dupProbability:  0.05, // 5% duplication by default
		delayRange:      100000, // 100ms delay by default
		random:          rand.New(rand.NewSource(42)),
		messageQueue:    make([]*faultInjectionMessage, 0),
		enabled:         false,
	}
}

// Enable enables fault injection.
func (fi *FaultInjector) Enable() {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.enabled = true
}

// Disable disables fault injection.
func (fi *FaultInjector) Disable() {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.enabled = false
	fi.messageQueue = make([]*faultInjectionMessage, 0)
}

// SetMode sets the fault injection mode.
func (fi *FaultInjector) SetMode(mode FaultInjectionMode) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.mode = mode
}

// SetLossProbability sets the probability of message loss (0.0 to 1.0).
func (fi *FaultInjector) SetLossProbability(prob float64) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if prob < 0 {
		prob = 0
	}
	if prob > 1 {
		prob = 1
	}
	fi.lossProbability = prob
}

// SetReorderWindow sets the maximum number of messages to reorder.
func (fi *FaultInjector) SetReorderWindow(window int) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if window < 0 {
		window = 0
	}
	fi.reorderWindow = window
}

// SetDupProbability sets the probability of message duplication (0.0 to 1.0).
func (fi *FaultInjector) SetDupProbability(prob float64) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if prob < 0 {
		prob = 0
	}
	if prob > 1 {
		prob = 1
	}
	fi.dupProbability = prob
}

// SetDelayRange sets the range of delay in microseconds.
func (fi *FaultInjector) SetDelayRange(delay cy.Microsecond) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.delayRange = delay
}

// InjectFault injects faults into a message based on the current mode.
// Returns true if the message should be delivered, false if it should be dropped.
// For messages that should be delivered, the message may be modified (duplicated, delayed).
func (fi *FaultInjector) InjectFault(msg SimMessage) ([]SimMessage, bool) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	
	if !fi.enabled {
		return []SimMessage{msg}, true
	}
	
	switch fi.mode {
	case FaultNone:
		return []SimMessage{msg}, true
		
	case FaultMessageLoss:
		if fi.random.Float64() < fi.lossProbability {
			// Drop the message
			return nil, false
		}
		return []SimMessage{msg}, true
		
	case FaultMessageReordering:
		// Queue the message for reordering
		fi.messageQueue = append(fi.messageQueue, &faultInjectionMessage{
			message:      msg,
			originalIndex: len(fi.messageQueue),
			delivered:    false,
		})
		
		// Deliver messages in random order
		if len(fi.messageQueue) >= fi.reorderWindow || fi.random.Float64() < 0.5 {
			return fi.deliverReorderedMessages(), true
		}
		return nil, false
		
	case FaultMessageDuplication:
		if fi.random.Float64() < fi.dupProbability {
			// Duplicate the message
			return []SimMessage{msg, msg}, true
		}
		return []SimMessage{msg}, true
		
	case FaultMessageDelay:
		// For delay, we would need to track timing
		// For now, just return the message
		return []SimMessage{msg}, true
		
	case FaultAll:
		// Apply all faults
		if fi.random.Float64() < fi.lossProbability {
			return nil, false
		}
		
		if fi.random.Float64() < fi.dupProbability {
			msg = SimMessage{
				From:      msg.From,
				To:        msg.To,
				SubjectID: msg.SubjectID,
				Data:      append([]byte(nil), msg.Data...),
				Timestamp: msg.Timestamp,
				Priority:  msg.Priority,
			}
			return []SimMessage{msg, msg}, true
		}
		
		// Queue for potential reordering
		fi.messageQueue = append(fi.messageQueue, &faultInjectionMessage{
			message:      msg,
			originalIndex: len(fi.messageQueue),
			delivered:    false,
		})
		
		if len(fi.messageQueue) >= fi.reorderWindow || fi.random.Float64() < 0.3 {
			return fi.deliverReorderedMessages(), true
		}
		return []SimMessage{msg}, true
	}
	
	return []SimMessage{msg}, true
}

// deliverReorderedMessages delivers queued messages in random order.
func (fi *FaultInjector) deliverReorderedMessages() []SimMessage {
	// Deliver all queued messages in random order
	if len(fi.messageQueue) == 0 {
		return nil
	}
	
	// Shuffle the queue
	for i := len(fi.messageQueue) - 1; i > 0; i-- {
		j := fi.random.Intn(i + 1)
		fi.messageQueue[i], fi.messageQueue[j] = fi.messageQueue[j], fi.messageQueue[i]
	}
	
	// Deliver all messages
	result := make([]SimMessage, 0, len(fi.messageQueue))
	for _, msg := range fi.messageQueue {
		if !msg.delivered {
			result = append(result, msg.message)
			msg.delivered = true
		}
	}
	
	// Clear the queue
	fi.messageQueue = make([]*faultInjectionMessage, 0)
	
	return result
}

// FaultInjectingNetwork extends SimNetwork with fault injection capabilities.
type FaultInjectingNetwork struct {
	*SimNetwork
	
	// injector is the fault injector.
	injector *FaultInjector
	
	// mu protects the fault injection state.
	mu sync.RWMutex
}

// NewFaultInjectingNetwork creates a new fault-injecting network.
func NewFaultInjectingNetwork(nodeCount int) *FaultInjectingNetwork {
	net := &FaultInjectingNetwork{
		SimNetwork: NewSimNetwork(nodeCount),
		injector:   NewFaultInjector(),
	}
	
	// Override the ProcessMessages to inject faults
	return net
}

// EnableFaultInjection enables fault injection.
func (net *FaultInjectingNetwork) EnableFaultInjection() {
	net.injector.Enable()
}

// DisableFaultInjection disables fault injection.
func (net *FaultInjectingNetwork) DisableFaultInjection() {
	net.injector.Disable()
}

// SetFaultMode sets the fault injection mode.
func (net *FaultInjectingNetwork) SetFaultMode(mode FaultInjectionMode) {
	net.injector.SetMode(mode)
}

// SetLossProbability sets the message loss probability.
func (net *FaultInjectingNetwork) SetLossProbability(prob float64) {
	net.injector.SetLossProbability(prob)
}

// SetReorderWindow sets the reorder window.
func (net *FaultInjectingNetwork) SetReorderWindow(window int) {
	net.injector.SetReorderWindow(window)
}

// SetDupProbability sets the duplication probability.
func (net *FaultInjectingNetwork) SetDupProbability(prob float64) {
	net.injector.SetDupProbability(prob)
}

// ProcessMessagesWithFaults processes messages with fault injection.
func (net *FaultInjectingNetwork) ProcessMessagesWithFaults() {
	net.mu.Lock()
	defer net.mu.Unlock()
	
	// Get messages from the parent network
	messages := net.messages
	net.messages = make([]SimMessage, 0)
	
	// Inject faults into each message
	for _, msg := range messages {
		deliverMsgs, shouldDeliver := net.injector.InjectFault(msg)
		if shouldDeliver {
			// Deliver all messages (original + duplicates)
			for _, deliverMsg := range deliverMsgs {
				net.deliverToNodeWithFault(deliverMsg)
			}
		}
		// If dropped, do nothing
	}
}

// deliverToNodeWithFault delivers a message to a node with fault injection.
func (net *FaultInjectingNetwork) deliverToNodeWithFault(msg SimMessage) {
	// For now, use the parent's deliverToNode
	// In a full implementation, we'd add more fault injection here
	for _, node := range net.nodes {
		if msg.To == 0 || msg.To == node.ID {
			// Skip the sender
			if msg.From == node.ID {
				continue
			}
			net.deliverToNode(node, msg)
		}
	}
}

// Messages returns the current message queue (for testing).
func (net *FaultInjectingNetwork) Messages() []SimMessage {
	net.mu.RLock()
	defer net.mu.RUnlock()
	return net.messages
}
