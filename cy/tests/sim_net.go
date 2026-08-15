// Package tests provides a simulation network for testing Cy without real hardware.
// This is inspired by the C++ e2e_sim_net but simplified for Go.
package tests

import (
	"sync"
	"time"
	"unsafe"

	"github.com/opencyphal/cy-go"
)

// SimNode represents a simulated node in the network.
type SimNode struct {
	// ID is the unique identifier for this node.
	ID uint64
	
	// Cy is the Cy instance for this node.
	Cy *cy.Cy
	
	// Platform is the mock platform for this node.
	Platform *SimPlatform
	
	// Name is the node name.
	Name string
	
	// mu protects the node state.
	mu sync.RWMutex
}

// SimNetwork represents a simulated network of nodes.
type SimNetwork struct {
	// nodes is the list of nodes in the network.
	nodes []*SimNode
	
	// messages is a queue of messages to be delivered.
	messages []SimMessage
	
	// mu protects the network state.
	mu sync.RWMutex
	
	// now is the current simulation time.
	now cy.Microsecond
	
	// randomState is the PRNG state.
	randomState uint64
}

// SimMessage represents a message in the simulation network.
type SimMessage struct {
	// From is the source node ID.
	From uint64
	// To is the destination node ID (0 for multicast).
	To uint64
	// SubjectID is the subject-ID.
	SubjectID uint32
	// Data is the message payload.
	Data []byte
	// Timestamp is when the message was sent.
	Timestamp cy.Microsecond
	// Priority is the message priority.
	Priority cy.Priority
}

// SimPlatform is a mock platform that connects to a simulation network.
type SimPlatform struct {
	// node is the node this platform belongs to.
	node *SimNode
	
	// network is the simulation network.
	network *SimNetwork
	
	// writers maps subject-IDs to subject writers.
	writers map[uint32]*SimSubjectWriter
	
	// readers maps subject-IDs to subject readers.
	readers map[uint32]*SimSubjectReader
	
	// mu protects the platform state.
	mu sync.RWMutex
}

// SimSubjectWriter implements cy.SubjectWriter for the simulation network.
type SimSubjectWriter struct {
	subjectID uint32
	platform  *SimPlatform
}

// SimSubjectReader implements cy.SubjectReader for the simulation network.
type SimSubjectReader struct {
	subjectID uint32
	extent   int
	platform  *SimPlatform
}

// NewSimNetwork creates a new simulation network with the specified number of nodes.
func NewSimNetwork(nodeCount int) *SimNetwork {
	network := &SimNetwork{
		nodes:       make([]*SimNode, nodeCount),
		messages:    make([]SimMessage, 0),
		now:        0,
		randomState: 0x1020304050607080,
	}
	
	// Create nodes
	for i := 0; i < nodeCount; i++ {
		name := "node" + string(rune('a'+i))
		platform := &SimPlatform{
			writers: make(map[uint32]*SimSubjectWriter),
			readers: make(map[uint32]*SimSubjectReader),
		}
		
		node := &SimNode{
			ID:       uint64(i + 1),
			Name:     name,
			Platform: platform,
		}
		
		platform.node = node
		platform.network = network
		
		// Create Cy instance
		cyInstance, err := cy.New(platform, name, "", "")
		if err != nil {
			panic(err)
		}
		
		node.Cy = cyInstance
		platform.node.Cy = cyInstance
		
		network.nodes[i] = node
	}
	
	return network
}

// NewSubjectWriter creates a new subject writer.
func (p *SimPlatform) NewSubjectWriter(subjectID uint32) (cy.SubjectWriter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	writer := &SimSubjectWriter{
		subjectID: subjectID,
		platform:  p,
	}
	p.writers[subjectID] = writer
	return writer, nil
}

// DestroySubjectWriter destroys a subject writer.
func (p *SimPlatform) DestroySubjectWriter(writer cy.SubjectWriter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if sw, ok := writer.(*SimSubjectWriter); ok {
		delete(p.writers, sw.subjectID)
	}
}

// SubjectID returns the subject-ID.
func (w *SimSubjectWriter) SubjectID() uint32 {
	return w.subjectID
}

// SubjectWriterSend sends a message via a subject writer.
func (p *SimPlatform) SubjectWriterSend(writer cy.SubjectWriter, deadline cy.Microsecond, priority cy.Priority, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if p.node == nil || p.network == nil {
		return cy.ErrArgument
	}
	
	// Get the subject-ID from the writer
	sw, ok := writer.(*SimSubjectWriter)
	if !ok {
		return cy.ErrArgument
	}
	
	// Create a message and add it to the network queue
	message := SimMessage{
		From:      p.node.ID,
		To:        0, // 0 means multicast
		SubjectID: sw.subjectID,
		Data:      append([]byte(nil), data...),
		Timestamp: p.network.Now(),
		Priority:  priority,
	}
	
	p.network.mu.Lock()
	p.network.messages = append(p.network.messages, message)
	p.network.mu.Unlock()
	
	return nil
}

// NewSubjectReader creates a new subject reader.
func (p *SimPlatform) NewSubjectReader(subjectID uint32, extent int) (cy.SubjectReader, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	reader := &SimSubjectReader{
		subjectID: subjectID,
		extent:   extent,
		platform:  p,
	}
	p.readers[subjectID] = reader
	return reader, nil
}

// DestroySubjectReader destroys a subject reader.
func (p *SimPlatform) DestroySubjectReader(reader cy.SubjectReader) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if sr, ok := reader.(*SimSubjectReader); ok {
		delete(p.readers, sr.subjectID)
	}
}

// SetSubjectReaderExtent sets the extent for a subject reader.
func (p *SimPlatform) SetSubjectReaderExtent(reader cy.SubjectReader, extent int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if sr, ok := reader.(*SimSubjectReader); ok {
		sr.extent = extent
	}
}

// Unicast sends a unicast message.
func (p *SimPlatform) Unicast(lane cy.Lane, deadline cy.Microsecond, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if p.node == nil || p.network == nil {
		return cy.ErrArgument
	}
	
	// Create a unicast message
	message := SimMessage{
		From:      p.node.ID,
		To:        lane.ID,
		SubjectID: 0, // Not used for unicast
		Data:      append([]byte(nil), data...),
		Timestamp: p.network.Now(),
		Priority:  lane.Priority,
	}
	
	p.network.mu.Lock()
	p.network.messages = append(p.network.messages, message)
	p.network.mu.Unlock()
	
	return nil
}

// SetUnicastExtent sets the unicast extent.
func (p *SimPlatform) SetUnicastExtent(extent int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// For now, just store in platform
	_ = extent
}

// Spin runs the event loop.
func (p *SimPlatform) Spin(deadline cy.Microsecond) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if p.node == nil || p.network == nil {
		return cy.ErrArgument
	}
	
	// Process messages in the network queue
	p.network.mu.Lock()
	defer p.network.mu.Unlock()
	
	for len(p.network.messages) > 0 {
		msg := p.network.messages[0]
		p.network.messages = p.network.messages[1:]
		
		// Deliver to the appropriate node
		if msg.To == 0 || msg.To == p.node.ID {
			// This message is for us (multicast or unicast to us)
			// In a real implementation, we'd deliver to the Cy instance
			// For now, just acknowledge receipt
		}
	}
	
	return nil
}

// Now returns the current time.
// SubjectIDModulus returns the subject-ID modulus configured for the simulation
// platform. It mirrors the C platform->subject_id_modulus field; the test network
// uses the default 16-bit modulus.
func (p *SimPlatform) SubjectIDModulus() uint32 {
	return cy.SubjectIDModulus16bit
}

func (p *SimPlatform) Now() cy.Microsecond {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if p.network != nil {
		p.network.mu.RLock()
		defer p.network.mu.RUnlock()
		return p.network.now
	}
	return cy.Microsecond(time.Now().UnixMicro())
}

// Realloc allocates memory.
func (p *SimPlatform) Realloc(ptr unsafe.Pointer, size int) unsafe.Pointer {
	if size == 0 {
		return nil
	}
	if ptr == nil {
		b := make([]byte, size)
		return unsafe.Pointer(&b[0])
	}
	return ptr
}

// Random returns a random value.
func (p *SimPlatform) Random() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.network != nil {
		p.network.mu.Lock()
		p.network.randomState++
		rand := p.network.randomState
		p.network.mu.Unlock()
		return rand
	}
	
	return 0
}

// SetCy sets the Cy instance.
func (p *SimPlatform) SetCy(cyInstance *cy.Cy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.node.Cy = cyInstance
}

// Now returns the current simulation time.
func (n *SimNetwork) Now() cy.Microsecond {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.now
}

// Advance advances the simulation time.
func (n *SimNetwork) Advance(delta cy.Microsecond) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.now += delta
}

// GetNode returns a node by index.
func (n *SimNetwork) GetNode(index int) *SimNode {
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	if index < 0 || index >= len(n.nodes) {
		return nil
	}
	return n.nodes[index]
}

// ProcessMessages processes all pending messages in the network.
// Messages are popped under the network lock and delivered outside it, so that a
// node's message handler may call back into the network (e.g. platform.Now())
// without deadlocking on the network mutex.
func (n *SimNetwork) ProcessMessages() {
	for {
		n.mu.Lock()
		if len(n.messages) == 0 {
			n.mu.Unlock()
			return
		}
		msg := n.messages[0]
		n.messages = n.messages[1:]
		n.mu.Unlock()

		// Deliver to all nodes (for multicast) or a specific node (for unicast).
		// Deliver outside the network lock to avoid lock-ordering deadlocks.
		if msg.To == 0 {
			for _, node := range n.nodes {
				if node.ID != msg.From {
					n.deliverToNode(node, msg)
				}
			}
		} else {
			for _, node := range n.nodes {
				if node.ID == msg.To {
					n.deliverToNode(node, msg)
					break
				}
			}
		}
	}
}

// deliverToNode delivers a message to a specific node.
func (n *SimNetwork) deliverToNode(node *SimNode, msg SimMessage) {
	// Create a MessageTS
	message := cy.NewMessage(msg.Data)
	messageTS := cy.NewMessageTS(msg.Timestamp, message)
	
	// Create a Lane
	lane := cy.Lane{
		ID:       msg.From,
		Priority: msg.Priority,
	}
	
	// Deliver to the Cy instance
	// This would normally be done by the platform
	if node.Cy != nil {
		// For now, just call HandleMessage directly
		subjectID := msg.SubjectID
		node.Cy.HandleMessage(lane, &subjectID, *messageTS)
	}
	
	// Clean up
	message.RefcountDec()
}

// SubjectID returns the subject-ID.
func (r *SimSubjectReader) SubjectID() uint32 {
	return r.subjectID
}

// Extent returns the extent.
func (r *SimSubjectReader) Extent() int {
	return r.extent
}

// SetExtent sets the extent.
func (r *SimSubjectReader) SetExtent(extent int) {
	r.extent = extent
}

// Ensure interfaces are satisfied
var _ cy.Platform = (*SimPlatform)(nil)
var _ cy.SubjectWriter = (*SimSubjectWriter)(nil)
var _ cy.SubjectReader = (*SimSubjectReader)(nil)
