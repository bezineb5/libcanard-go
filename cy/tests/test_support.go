// Package tests provides test utilities for the cy package.
// This includes mock platforms and test fixtures for testing the Cy library
// without requiring actual hardware.
package tests

import (
	"sync"
	"time"
	"unsafe"

	"github.com/opencyphal/cy-go"
)

// MockPlatform is a mock implementation of cy.Platform for testing.
// It simulates a transport layer without requiring actual hardware.
type MockPlatform struct {
	// cyInstance is the Cy instance (set by cy.New).
	cyInstance *cy.Cy
	
	// Messages received by this platform (for testing).
	ReceivedMessages []ReceivedMessage
	
	// mu protects the platform state.
	mu sync.RWMutex
	
	// closed indicates if the platform has been destroyed.
	closed bool
	
	// nowFunc returns the current time (can be mocked).
	nowFunc func() cy.Microsecond
	
	// prngSeed for random number generation.
	prngSeed uint64
	
	// unicastExtent is the maximum extent for unicast messages.
	unicastExtent int
}

// ReceivedMessage represents a message received by the mock platform.
type ReceivedMessage struct {
	Lane      cy.Lane
	SubjectID uint32
	Timestamp cy.Microsecond
	Data      []byte
}

// NewMockPlatform creates a new mock platform.
func NewMockPlatform() *MockPlatform {
	return &MockPlatform{
		ReceivedMessages: make([]ReceivedMessage, 0),
		nowFunc:          func() cy.Microsecond { return cy.Microsecond(time.Now().UnixMicro()) },
		prngSeed:        0xDEADBEEF,
	}
}

// NewSubjectWriter creates a new subject writer.
func (p *MockPlatform) NewSubjectWriter(subjectID uint32) (cy.SubjectWriter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return nil, cy.ErrArgument
	}
	
	return &mockSubjectWriter{
		subjectID: subjectID,
		platform:  p,
	}, nil
}

// DestroySubjectWriter destroys a subject writer.
func (p *MockPlatform) DestroySubjectWriter(writer cy.SubjectWriter) {
	// No-op for mock
}

// SubjectWriterSend sends a message via a subject writer.
func (p *MockPlatform) SubjectWriterSend(writer cy.SubjectWriter, deadline cy.Microsecond, priority cy.Priority, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return cy.ErrArgument
	}
	
	// For mock, we don't actually send, just record if needed
	// In a real test, we'd dispatch to subscribers
	return nil
}

// NewSubjectReader creates a new subject reader.
func (p *MockPlatform) NewSubjectReader(subjectID uint32, extent int) (cy.SubjectReader, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return nil, cy.ErrArgument
	}
	
	return &mockSubjectReader{
		subjectID: subjectID,
		extent:   extent,
		platform:  p,
	}, nil
}

// DestroySubjectReader destroys a subject reader.
func (p *MockPlatform) DestroySubjectReader(reader cy.SubjectReader) {
	// No-op for mock
}

// SetSubjectReaderExtent sets the extent for a subject reader.
func (p *MockPlatform) SetSubjectReaderExtent(reader cy.SubjectReader, extent int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return
	}
	
	if mr, ok := reader.(*mockSubjectReader); ok {
		mr.extent = extent
	}
}

// Unicast sends a unicast message.
func (p *MockPlatform) Unicast(lane cy.Lane, deadline cy.Microsecond, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return cy.ErrArgument
	}
	
	// Record the message
	p.ReceivedMessages = append(p.ReceivedMessages, ReceivedMessage{
		Lane:      lane,
		SubjectID: 0, // Not used for unicast
		Timestamp: p.nowFunc(),
		Data:      append([]byte(nil), data...),
	})
	
	return nil
}

// SetUnicastExtent sets the unicast extent.
func (p *MockPlatform) SetUnicastExtent(extent int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unicastExtent = extent
}

// Spin runs the event loop.
func (p *MockPlatform) Spin(deadline cy.Microsecond) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if p.closed {
		return cy.ErrArgument
	}
	
	// For mock, just return OK
	return cy.OK
}

// Now returns the current time.
func (p *MockPlatform) Now() cy.Microsecond {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nowFunc()
}

// Realloc allocates memory.
func (p *MockPlatform) Realloc(ptr unsafe.Pointer, size int) unsafe.Pointer {
	if size == 0 {
		return nil
	}
	if ptr == nil {
		b := make([]byte, size)
		return unsafe.Pointer(&b[0])
	}
	return ptr // For mock, just return the same pointer
}

// Random returns a random value.
func (p *MockPlatform) Random() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.prngSeed++
	return p.prngSeed
}

// SetCy sets the Cy instance.
func (p *MockPlatform) SetCy(cyInstance *cy.Cy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cyInstance = cyInstance
}

// Destroy cleans up the mock platform.
func (p *MockPlatform) Destroy() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.cyInstance = nil
	p.ReceivedMessages = nil
}

// mockSubjectWriter implements cy.SubjectWriter for testing.
type mockSubjectWriter struct {
	subjectID uint32
	platform  *MockPlatform
}

// SubjectID returns the subject-ID.
func (w *mockSubjectWriter) SubjectID() uint32 {
	return w.subjectID
}

// mockSubjectReader implements cy.SubjectReader for testing.
type mockSubjectReader struct {
	subjectID uint32
	extent   int
	platform  *MockPlatform
}

// SubjectID returns the subject-ID.
func (r *mockSubjectReader) SubjectID() uint32 {
	return r.subjectID
}

// Extent returns the extent.
func (r *mockSubjectReader) Extent() int {
	return r.extent
}

// SetExtent sets the extent.
func (r *mockSubjectReader) SetExtent(extent int) {
	r.extent = extent
}

// Ensure interfaces are satisfied
var _ cy.Platform = (*MockPlatform)(nil)
var _ cy.SubjectWriter = (*mockSubjectWriter)(nil)
var _ cy.SubjectReader = (*mockSubjectReader)(nil)

// TestMockPlatformBasic tests the mock platform creation.
