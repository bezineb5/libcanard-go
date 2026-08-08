// Package can provides a CAN transport layer implementation for Cyphal.
// It uses the go.einride.tech/can library for CAN I/O.
package can

import (
	"context"
	"errors"
	"sync"
	"time"
	"unsafe"

	"github.com/opencyphal/cy-go"
	"go.einride.tech/can"
	"go.einride.tech/can/pkg/socketcan"
)

// Platform implements the cy.Platform interface for CAN transport.
type Platform struct {
	cyInstance *cy.Cy
	receiver   *socketcan.Receiver
	transmitter *socketcan.Transmitter
	ifaceCount uint8
	txQueueCapacity int
	filterCount    int
	prngSeed       uint64
	writers        map[uint32]*subjectWriter
	readers        map[uint32]*subjectReader
	unicastExtent   int
	subjectIDModulus uint32
	mu             sync.RWMutex
	closed         bool
}

type subjectWriter struct {
	subjectID uint32
	platform  *Platform
}

func (w *subjectWriter) SubjectID() uint32 { return w.subjectID }

type subjectReader struct {
	subjectID uint32
	extent   int
	platform  *Platform
}

func (r *subjectReader) SubjectID() uint32    { return r.subjectID }
func (r *subjectReader) Extent() int          { return r.extent }
func (r *subjectReader) SetExtent(extent int) { r.extent = extent }

// New creates a new CAN platform.
func New(ifaceName string, txQueueCapacity, filterCount int, prngSeed uint64) (*Platform, error) {
	if ifaceName == "" {
		return nil, errors.New("CAN interface name is required")
	}

	// Dial the CAN interface
	conn, err := socketcan.Dial("can", ifaceName)
	if err != nil {
		return nil, err
	}

	p := &Platform{
		ifaceCount:      1,
		txQueueCapacity: txQueueCapacity,
		filterCount:     filterCount,
		prngSeed:        prngSeed,
		writers:        make(map[uint32]*subjectWriter),
		readers:        make(map[uint32]*subjectReader),
		subjectIDModulus: cy.SubjectIDModulus16bit,
	}

	// Create receiver and transmitter
	p.receiver = socketcan.NewReceiver(conn)
	p.transmitter = socketcan.NewTransmitter(conn)

	// Start receiving frames in a goroutine
	go p.receiveLoop()
	return p, nil
}

// Destroy cleans up the platform.
func (p *Platform) Destroy() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.receiver = nil
	p.transmitter = nil
	p.writers = nil
	p.readers = nil
	p.cyInstance = nil
}

func (p *Platform) receiveLoop() {
	for {
		p.mu.RLock()
		closed := p.closed
		receiver := p.receiver
		p.mu.RUnlock()
		if closed || receiver == nil {
			return
		}

		if !receiver.Receive() {
			// Error receiving
			time.Sleep(10 * time.Millisecond)
			continue
		}

		frame := receiver.Frame()
		p.processFrame(frame)
	}
}

func (p *Platform) processFrame(frame can.Frame) {
	subjectID := uint32(frame.ID & 0xFFFF)
	priority := cy.Priority((frame.ID >> 16) & 0x07)

	if (frame.ID>>16)&0x1FF == 511 {
		// Unicast - ignore for now
		return
	}

	p.handleMulticastFrame(frame, subjectID, priority)
}

func (p *Platform) handleMulticastFrame(frame can.Frame, subjectID uint32, priority cy.Priority) {
	p.mu.RLock()
	cyInstance := p.cyInstance
	p.mu.RUnlock()
	if cyInstance == nil {
		return
	}

	data := make([]byte, frame.Length)
	copy(data, frame.Data[:frame.Length])

	message := cy.AcquireMessage()
	message.SetData(data)

	msgTS := cy.NewMessageTS(cy.Microsecond(time.Now().UnixMicro()), message)
	lane := cy.Lane{Priority: priority}

	cyInstance.HandleMessage(lane, &subjectID, *msgTS)
	cy.ReleaseMessageTS(msgTS)
}

func (p *Platform) NewSubjectWriter(subjectID uint32) (cy.SubjectWriter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, cy.ErrArgument
	}
	if writer, ok := p.writers[subjectID]; ok {
		return writer, nil
	}
	writer := &subjectWriter{subjectID: subjectID, platform: p}
	p.writers[subjectID] = writer
	return writer, nil
}

func (p *Platform) DestroySubjectWriter(writer cy.SubjectWriter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if sw, ok := writer.(*subjectWriter); ok {
		delete(p.writers, sw.subjectID)
	}
}

func (p *Platform) SubjectWriterSend(writer cy.SubjectWriter, deadline cy.Microsecond, priority cy.Priority, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return cy.ErrArgument
	}
	sw, ok := writer.(*subjectWriter)
	if !ok {
		return cy.ErrArgument
	}
	canID := p.encodeCANID(sw.subjectID, priority, false)
	frame := can.Frame{
		ID:        canID,
		Length:    uint8(len(data)),
		IsExtended: true,
	}
	if len(data) > 0 {
		copy(frame.Data[:], data)
	}
	p.transmitter.TransmitFrame(context.Background(), frame)
	return nil
}

func (p *Platform) NewSubjectReader(subjectID uint32, extent int) (cy.SubjectReader, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, cy.ErrArgument
	}
	if reader, ok := p.readers[subjectID]; ok {
		return reader, nil
	}
	reader := &subjectReader{subjectID: subjectID, extent: extent, platform: p}
	p.readers[subjectID] = reader
	return reader, nil
}

func (p *Platform) DestroySubjectReader(reader cy.SubjectReader) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if sr, ok := reader.(*subjectReader); ok {
		delete(p.readers, sr.subjectID)
	}
}

func (p *Platform) SetSubjectReaderExtent(reader cy.SubjectReader, extent int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if sr, ok := reader.(*subjectReader); ok {
		sr.extent = extent
	}
}

func (p *Platform) Unicast(lane cy.Lane, deadline cy.Microsecond, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return cy.ErrArgument
	}
	canID := p.encodeCANID(511, lane.Priority, true)
	frame := can.Frame{
		ID:        canID,
		Length:    uint8(len(data)),
		IsExtended: true,
	}
	if len(data) > 0 {
		copy(frame.Data[:], data)
	}
	p.transmitter.TransmitFrame(context.Background(), frame)
	return nil
}

func (p *Platform) SetUnicastExtent(extent int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unicastExtent = extent
}

func (p *Platform) Spin(deadline cy.Microsecond) error {
	return cy.OK
}

func (p *Platform) Now() cy.Microsecond {
	return cy.Microsecond(time.Now().UnixMicro())
}

func (p *Platform) Realloc(ptr unsafe.Pointer, size int) unsafe.Pointer {
	if size == 0 {
		return nil
	}
	if ptr == nil {
		b := make([]byte, size)
		return unsafe.Pointer(&b[0])
	}
	oldSlice := (*[1 << 30]byte)(ptr)[:size:size]
	newSlice := make([]byte, size)
	copy(newSlice, oldSlice)
	return unsafe.Pointer(&newSlice[0])
}

func (p *Platform) Random() uint64 {
	p.prngSeed = p.prngSeed*6364136223846793005 + 1
	return p.prngSeed
}

func (p *Platform) SetCy(cyInstance *cy.Cy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cyInstance = cyInstance
}

func (p *Platform) encodeCANID(subjectID uint32, priority cy.Priority, unicast bool) uint32 {
	canID := subjectID & 0xFFFF
	canID |= uint32(priority) << 16
	if unicast {
		canID |= 511 << 16
	}
	return canID
}

// Frame returns the last received frame from the receiver.
// This is a helper for debugging.
func (p *Platform) Frame() can.Frame {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.receiver != nil {
		return p.receiver.Frame()
	}
	return can.Frame{}
}

// Ensure interfaces are satisfied
var _ cy.Platform = (*Platform)(nil)
var _ cy.SubjectWriter = (*subjectWriter)(nil)
var _ cy.SubjectReader = (*subjectReader)(nil)
