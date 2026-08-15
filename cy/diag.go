package cy

import (
	"fmt"
	"path/filepath"
	"runtime"
)

// DiagVTable holds the optional diagnostics callbacks. Any callback may be nil.
// The callbacks are invoked synchronously from within Cy API calls. They must not
// modify the diagnostics listener list (adding/removing a listener from within a
// callback is not supported), mirroring the C behavior.
type DiagVTable struct {
	// AsyncError reports an asynchronous error. These surface errors occurring
	// asynchronously with API invocations -- in particular, topic resubscription
	// failures during consensus updates and the inability to create an implicit
	// subscription on pattern match due to lack of memory. The site string
	// identifies the call site as "file:line". The topic may be nil depending on
	// the nature of the error. This is the Go equivalent of C's ON_ASYNC_ERROR /
	// ON_ASYNC_ERROR_IF.
	AsyncError func(diag *Diag, topic *Topic, err Error, site string)

	// TopicCreated is invoked immediately after a topic is created.
	TopicCreated func(diag *Diag, topic *Topic)

	// TopicDestroyed is invoked immediately before a topic is destroyed.
	TopicDestroyed func(diag *Diag, topic *Topic)

	// TopicReallocated is invoked immediately after a topic's allocation state
	// (subject-ID / evictions) is committed. This includes the initial allocation
	// performed during topic creation.
	TopicReallocated func(diag *Diag, topic *Topic, subjectID uint32, evictions uint32)

	// GossipProcessed is invoked immediately after a gossip message is processed.
	// topic is nil if the gossip is not associated with any locally known topic.
	// The name lifetime ends upon return from the handler.
	GossipProcessed func(diag *Diag, topic *Topic, name string, hash uint64)
}

// Diag is a diagnostics listener installed on a Cy instance via DiagAdd.
// The Context field is arbitrary user state shared with the callbacks.
// Once installed, the Diag instance must not be moved.
type Diag struct {
	next    *Diag
	Context interface{}
	VTable  *DiagVTable
}

// DiagAdd installs a diagnostics listener on the Cy instance. Duplicate addition
// is a no-op. Adding/removing from within a callback is not supported.
func (c *Cy) DiagAdd(d *Diag) {
	if d == nil {
		return
	}
	c.diagMu.Lock()
	defer c.diagMu.Unlock()
	for p := c.diags; p != nil; p = p.next {
		if p == d {
			return
		}
	}
	d.next = c.diags
	c.diags = d
}

// DiagRemove uninstalls a diagnostics listener. Nonexistent removal is a no-op.
func (c *Cy) DiagRemove(d *Diag) {
	if d == nil {
		return
	}
	c.diagMu.Lock()
	defer c.diagMu.Unlock()
	p := &c.diags
	for *p != nil {
		if *p == d {
			*p = d.next
			d.next = nil
			return
		}
		p = &(*p).next
	}
}

// errorToCyErr maps the standard-library error returned by platform operations
// and allocation helpers to the Cy error code expected by the diagnostics
// subsystem (matching C's cy_err_t). A nil error maps to OK, a wrapped cy.Error
// is preserved, and any other error is reported as ErrMedia (transport/media
// failure, the dominant class for the asynchronous send paths).
func errorToCyErr(e error) Error {
	if e == nil {
		return OK
	}
	if ce, ok := e.(Error); ok {
		return ce
	}
	return ErrMedia
}

// snapshotDiags returns a snapshot of the installed diagnostics listeners. The
// caller must NOT hold c.diagMu.
func (c *Cy) snapshotDiags() []*Diag {
	c.diagMu.Lock()
	out := make([]*Diag, 0)
	for d := c.diags; d != nil; d = d.next {
		out = append(out, d)
	}
	c.diagMu.Unlock()
	return out
}

// DiagAsyncError reports an asynchronous error to all installed listeners. It is
// the Go equivalent of C's ON_ASYNC_ERROR / ON_ASYNC_ERROR_IF. The call site is
// captured automatically (file:line). Calls with err == OK are ignored.
func (c *Cy) DiagAsyncError(topic *Topic, err Error) {
	if err == OK {
		return
	}
	_, file, line, _ := runtime.Caller(1)
	site := fmt.Sprintf("%s:%d", filepath.Base(file), line)
	for _, d := range c.snapshotDiags() {
		if d.VTable != nil && d.VTable.AsyncError != nil {
			d.VTable.AsyncError(d, topic, err, site)
		}
	}
}

// DiagTopicCreated reports topic creation to listeners (Go equivalent of C's
// diag_topic_created).
func (c *Cy) DiagTopicCreated(topic *Topic) {
	for _, d := range c.snapshotDiags() {
		if d.VTable != nil && d.VTable.TopicCreated != nil {
			d.VTable.TopicCreated(d, topic)
		}
	}
}

// DiagTopicDestroyed reports topic destruction to listeners.
func (c *Cy) DiagTopicDestroyed(topic *Topic) {
	for _, d := range c.snapshotDiags() {
		if d.VTable != nil && d.VTable.TopicDestroyed != nil {
			d.VTable.TopicDestroyed(d, topic)
		}
	}
}

// DiagTopicReallocated reports a committed topic reallocation to listeners.
func (c *Cy) DiagTopicReallocated(topic *Topic, subjectID uint32, evictions uint32) {
	for _, d := range c.snapshotDiags() {
		if d.VTable != nil && d.VTable.TopicReallocated != nil {
			d.VTable.TopicReallocated(d, topic, subjectID, evictions)
		}
	}
}

// DiagGossipProcessed reports a processed gossip message to listeners.
func (c *Cy) DiagGossipProcessed(topic *Topic, name string, hash uint64) {
	for _, d := range c.snapshotDiags() {
		if d.VTable != nil && d.VTable.GossipProcessed != nil {
			d.VTable.GossipProcessed(d, topic, name, hash)
		}
	}
}