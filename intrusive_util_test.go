// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// a compact implementation of the Cyphal/CAN transport for high-integrity real-time embedded systems.
//
// The library offers a non-blocking, callback-based API. It is not safe for concurrent use; if used in a
// concurrent environment it is the responsibility of the application to provide adequate synchronization.
//
// The original library is distributed under the terms of the MIT License. Copyright (c) OpenCyphal.

package libcanard

// This file migrates tests/src/test_intrusive_util.c to Go.
//
// It is an in-package white-box test that exercises the utility functions:
// popcount, random, chance, bitmapSet, bitmapTest, list operations, and CRC functions.

import (
	"testing"
)

// =============================================================================
// Popcount Tests
// =============================================================================

func TestPopcount(t *testing.T) {
	// Zero
	if popcount(0) != 0 {
		t.Errorf("popcount(0) = %d, want 0", popcount(0))
	}

	// Single bits at each nibble boundary
	if popcount(1) != 1 {
		t.Errorf("popcount(1) = %d, want 1", popcount(1))
	}
	if popcount(1<<4) != 1 {
		t.Errorf("popcount(1<<4) = %d, want 1", popcount(1<<4))
	}
	if popcount(1<<8) != 1 {
		t.Errorf("popcount(1<<8) = %d, want 1", popcount(1<<8))
	}
	if popcount(1<<16) != 1 {
		t.Errorf("popcount(1<<16) = %d, want 1", popcount(1<<16))
	}
	if popcount(1<<32) != 1 {
		t.Errorf("popcount(1<<32) = %d, want 1", popcount(1<<32))
	}
	if popcount(1<<48) != 1 {
		t.Errorf("popcount(1<<48) = %d, want 1", popcount(1<<48))
	}
	if popcount(1<<63) != 1 {
		t.Errorf("popcount(1<<63) = %d, want 1", popcount(1<<63))
	}

	// All bits set in progressively larger ranges
	if popcount(0xF) != 4 {
		t.Errorf("popcount(0xF) = %d, want 4", popcount(0xF))
	}
	if popcount(0xFF) != 8 {
		t.Errorf("popcount(0xFF) = %d, want 8", popcount(0xFF))
	}
	if popcount(0xFFF) != 12 {
		t.Errorf("popcount(0xFFF) = %d, want 12", popcount(0xFFF))
	}
	if popcount(0xFFFF) != 16 {
		t.Errorf("popcount(0xFFFF) = %d, want 16", popcount(0xFFFF))
	}
	if popcount(0xFFFFFF) != 24 {
		t.Errorf("popcount(0xFFFFFF) = %d, want 24", popcount(0xFFFFFF))
	}
	if popcount(0xFFFFFFFF) != 32 {
		t.Errorf("popcount(0xFFFFFFFF) = %d, want 32", popcount(0xFFFFFFFF))
	}
	if popcount(0xFFFFFFFFFFFF) != 48 {
		t.Errorf("popcount(0xFFFFFFFFFFFF) = %d, want 48", popcount(0xFFFFFFFFFFFF))
	}
	if popcount(0xFFFFFFFFFFFFFFFF) != 64 {
		t.Errorf("popcount(0xFFFFFFFFFFFFFFFF) = %d, want 64", popcount(0xFFFFFFFFFFFFFFFF))
	}

	// Alternating bit patterns
	if popcount(0xAAAAAAAAAAAAAAAA) != 32 {
		t.Errorf("popcount(0xAAAAAAAAAAAAAAAA) = %d, want 32", popcount(0xAAAAAAAAAAAAAAAA))
	}
	if popcount(0x5555555555555555) != 32 {
		t.Errorf("popcount(0x5555555555555555) = %d, want 32", popcount(0x5555555555555555))
	}
	if popcount(0xCCCCCCCCCCCCCCCC) != 32 {
		t.Errorf("popcount(0xCCCCCCCCCCCCCCCC) = %d, want 32", popcount(0xCCCCCCCCCCCCCCCC))
	}
	if popcount(0x3333333333333333) != 32 {
		t.Errorf("popcount(0x3333333333333333) = %d, want 32", popcount(0x3333333333333333))
	}
	if popcount(0xF0F0F0F0F0F0F0F0) != 32 {
		t.Errorf("popcount(0xF0F0F0F0F0F0F0F0) = %d, want 32", popcount(0xF0F0F0F0F0F0F0F0))
	}
	if popcount(0x0F0F0F0F0F0F0F0F) != 32 {
		t.Errorf("popcount(0x0F0F0F0F0F0F0F0F) = %d, want 32", popcount(0x0F0F0F0F0F0F0F0F))
	}

	// Byte patterns repeated
	if popcount(0x0101010101010101) != 8 {
		t.Errorf("popcount(0x0101010101010101) = %d, want 8", popcount(0x0101010101010101))
	}
	if popcount(0x0303030303030303) != 16 {
		t.Errorf("popcount(0x0303030303030303) = %d, want 16", popcount(0x0303030303030303))
	}
	if popcount(0x0707070707070707) != 24 {
		t.Errorf("popcount(0x0707070707070707) = %d, want 24", popcount(0x0707070707070707))
	}
	if popcount(0x7F7F7F7F7F7F7F7F) != 56 {
		t.Errorf("popcount(0x7F7F7F7F7F7F7F7F) = %d, want 56", popcount(0x7F7F7F7F7F7F7F7F))
	}

	// Sparse patterns
	if popcount(0x8000000000000001) != 2 {
		t.Errorf("popcount(0x8000000000000001) = %d, want 2", popcount(0x8000000000000001))
	}
	if popcount(0x0000000180000000) != 2 {
		t.Errorf("popcount(0x0000000180000000) = %d, want 2", popcount(0x0000000180000000))
	}
	if popcount(0x8000000180000001) != 4 {
		t.Errorf("popcount(0x8000000180000001) = %d, want 4", popcount(0x8000000180000001))
	}

	// Dense patterns
	if popcount(0xFFFFFFFFFFFFFFFE) != 63 {
		t.Errorf("popcount(0xFFFFFFFFFFFFFFFE) = %d, want 63", popcount(0xFFFFFFFFFFFFFFFE))
	}
	if popcount(0x7FFFFFFFFFFFFFFF) != 63 {
		t.Errorf("popcount(0x7FFFFFFFFFFFFFFF) = %d, want 63", popcount(0x7FFFFFFFFFFFFFFF))
	}
	if popcount(0x7FFFFFFFFFFFFFFE) != 62 {
		t.Errorf("popcount(0x7FFFFFFFFFFFFFFE) = %d, want 62", popcount(0x7FFFFFFFFFFFFFFE))
	}

	// Powers of two minus one
	if popcount((1<<1)-1) != 1 {
		t.Errorf("popcount(2^1-1) = %d, want 1", popcount((1<<1)-1))
	}
	if popcount((1<<7)-1) != 7 {
		t.Errorf("popcount(2^7-1) = %d, want 7", popcount((1<<7)-1))
	}
	if popcount((1<<15)-1) != 15 {
		t.Errorf("popcount(2^15-1) = %d, want 15", popcount((1<<15)-1))
	}
	if popcount((1<<31)-1) != 31 {
		t.Errorf("popcount(2^31-1) = %d, want 31", popcount((1<<31)-1))
	}
	if popcount((1<<63)-1) != 63 {
		t.Errorf("popcount(2^63-1) = %d, want 63", popcount((1<<63)-1))
	}

	// Small values
	if popcount(1) != 1 {
		t.Errorf("popcount(1) = %d, want 1", popcount(1))
	}
	if popcount(2) != 1 {
		t.Errorf("popcount(2) = %d, want 1", popcount(2))
	}
	if popcount(3) != 2 {
		t.Errorf("popcount(3) = %d, want 2", popcount(3))
	}
	if popcount(4) != 1 {
		t.Errorf("popcount(4) = %d, want 1", popcount(4))
	}
	if popcount(5) != 2 {
		t.Errorf("popcount(5) = %d, want 2", popcount(5))
	}
	if popcount(6) != 2 {
		t.Errorf("popcount(6) = %d, want 2", popcount(6))
	}
	if popcount(7) != 3 {
		t.Errorf("popcount(7) = %d, want 3", popcount(7))
	}
	if popcount(8) != 1 {
		t.Errorf("popcount(8) = %d, want 1", popcount(8))
	}

	// Specific known values
	if popcount(63) != 6 {
		t.Errorf("popcount(63) = %d, want 6", popcount(63))
	}
	if popcount(64) != 1 {
		t.Errorf("popcount(64) = %d, want 1", popcount(64))
	}
	if popcount(127) != 7 {
		t.Errorf("popcount(127) = %d, want 7", popcount(127))
	}
	if popcount(128) != 1 {
		t.Errorf("popcount(128) = %d, want 1", popcount(128))
	}
	if popcount(255) != 8 {
		t.Errorf("popcount(255) = %d, want 8", popcount(255))
	}
	if popcount(256) != 1 {
		t.Errorf("popcount(256) = %d, want 1", popcount(256))
	}
}

// =============================================================================
// CRC Tests
// =============================================================================

func TestCrcAdd(t *testing.T) {
	// Empty input returns initial CRC unchanged
	if crcAdd(crcInitial, 0, nil) != 0xFFFF {
		t.Errorf("crcAdd with empty input = 0x%04X, want 0xFFFF", crcAdd(crcInitial, 0, nil))
	}

	// Single bytes
	data := []byte{0x00}
	if crcAdd(crcInitial, 1, data) != 0xE1F0 {
		t.Errorf("crcAdd(0x00) = 0x%04X, want 0xE1F0", crcAdd(crcInitial, 1, data))
	}

	data = []byte{0xFF}
	if crcAdd(crcInitial, 1, data) != 0xFF00 {
		t.Errorf("crcAdd(0xFF) = 0x%04X, want 0xFF00", crcAdd(crcInitial, 1, data))
	}

	data = []byte{'A'}
	if crcAdd(crcInitial, 1, data) != 0xB915 {
		t.Errorf("crcAdd('A') = 0x%04X, want 0xB915", crcAdd(crcInitial, 1, data))
	}

	// Standard test vector: "123456789" yields 0x29B1
	vec := []byte{'1', '2', '3', '4', '5', '6', '7', '8', '9'}
	if crcAdd(crcInitial, len(vec), vec) != 0x29B1 {
		t.Errorf("crcAdd('123456789') = 0x%04X, want 0x29B1", crcAdd(crcInitial, len(vec), vec))
	}

	// Multi-byte patterns
	zeros := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	if crcAdd(crcInitial, len(zeros), zeros) != 0x313E {
		t.Errorf("crcAdd(8 zeros) = 0x%04X, want 0x313E", crcAdd(crcInitial, len(zeros), zeros))
	}

	ones := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	if crcAdd(crcInitial, len(ones), ones) != 0x97DF {
		t.Errorf("crcAdd(8 ones) = 0x%04X, want 0x97DF", crcAdd(crcInitial, len(ones), ones))
	}

	// Incremental computation must match full computation
	crcInc := uint16(crcInitial)
	for i := 0; i < len(vec); i++ {
		crcInc = crcAddByte(crcInc, vec[i])
	}
	if crcInc != 0x29B1 {
		t.Errorf("incremental crcAdd = 0x%04X, want 0x29B1", crcInc)
	}

	// Two-chunk computation
	crcChunks := crcAdd(crcInitial, 5, vec)
	crcChunks = crcAdd(crcChunks, 4, vec[5:])
	if crcChunks != 0x29B1 {
		t.Errorf("two-chunk crcAdd = 0x%04X, want 0x29B1", crcChunks)
	}
}

// In the Go port, crcAddChain is just a wrapper around crcAdd with byte slices
func TestCrcAddChain(t *testing.T) {
	// Single fragment - in Go this is just crcAdd
	data := []byte{'1', '2', '3', '4', '5', '6', '7', '8', '9'}
	if crcAddChain(crcInitial, data) != 0x29B1 {
		t.Errorf("crcAddChain single fragment = 0x%04X, want 0x29B1", crcAddChain(crcInitial, data))
	}

	// Multiple chunks - simulate by concatenating
	f1 := []byte{'1', '2', '3'}
	f2 := []byte{'4', '5'}
	f3 := []byte{'6', '7', '8', '9'}
	// In Go, we can just concatenate for testing
	combined := append(append(f1, f2...), f3...)
	if crcAddChain(crcInitial, combined) != 0x29B1 {
		t.Errorf("crcAddChain combined = 0x%04X, want 0x29B1", crcAddChain(crcInitial, combined))
	}
}

// =============================================================================
// Bytes Chain Tests - Simplified for Go
// In the Go port, BytesChain is retained for API compatibility but the reader functions
// from C are not implemented. We test the basic structure and validation.
// =============================================================================

func TestBytesChainValid(t *testing.T) {
	// In Go, bytesChainValid just checks if the byte slice is valid
	// data=nil, size=0 is valid (empty fragment)
	if !bytesChainValid(nil) {
		t.Error("bytesChainValid should return true for nil slice")
	}
	if !bytesChainValid([]byte{}) {
		t.Error("bytesChainValid should return true for empty slice")
	}
	if !bytesChainValid([]byte{1, 2, 3}) {
		t.Error("bytesChainValid should return true for valid slice")
	}
}

// =============================================================================
// List Tests
// =============================================================================

type testNode struct {
	value  int
	member cavlListed
}

func TestList(t *testing.T) {
	list := cavlList{head: nil, tail: nil}
	node1 := testNode{value: 1, member: cavlListed{next: nil, prev: nil, owner: nil}}
	node2 := testNode{value: 2, member: cavlListed{next: nil, prev: nil, owner: nil}}
	node3 := testNode{value: 3, member: cavlListed{next: nil, prev: nil, owner: nil}}
	// Set the owner pointers
	node1.member.owner = &node1
	node2.member.owner = &node2
	node3.member.owner = &node3

	// Empty list
	if list.head != nil {
		t.Error("empty list head should be nil")
	}
	if list.tail != nil {
		t.Error("empty list tail should be nil")
	}

	// Delist on empty list is a no-op
	delist(&list, &node1.member)
	if list.head != nil {
		t.Error("delist on empty list should not change head")
	}

	// Reset for next test
	list.head = nil
	list.tail = nil

	// Add single element
	enlistBefore(&list, list.head, &node1.member)
	if list.head != &node1.member {
		t.Error("head should be node1")
	}
	if list.tail != &node1.member {
		t.Error("tail should be node1")
	}
	if node1.member.next != nil {
		t.Error("node1.next should be nil")
	}
	if node1.member.prev != nil {
		t.Error("node1.prev should be nil")
	}

	// Add second element at head
	enlistBefore(&list, list.head, &node2.member)
	if list.head != &node2.member {
		t.Error("head should be node2")
	}
	if list.tail != &node1.member {
		t.Error("tail should be node1")
	}
	if node1.member.prev != &node2.member {
		t.Error("node1.prev should be node2")
	}
	if node2.member.next != &node1.member {
		t.Error("node2.next should be node1")
	}

	// Add third element at head. Order: node3 -> node2 -> node1
	enlistBefore(&list, list.head, &node3.member)
	if list.head != &node3.member {
		t.Error("head should be node3")
	}
	if list.tail != &node1.member {
		t.Error("tail should be node1")
	}

	// Delist middle
	delist(&list, &node2.member)
	if list.head != &node3.member {
		t.Error("head should be node3 after delisting middle")
	}
	if list.tail != &node1.member {
		t.Error("tail should be node1 after delisting middle")
	}
	if node1.member.prev != &node3.member {
		t.Error("node1.prev should be node3 after delisting middle")
	}
	if node3.member.next != &node1.member {
		t.Error("node3.next should be node1 after delisting middle")
	}

	// Re-add node2, then delist head
	enlistBefore(&list, list.head, &node2.member) // Order: node2 -> node3 -> node1
	delist(&list, &node2.member)
	if list.head != &node3.member {
		t.Error("head should be node3 after delisting head")
	}
	if node3.member.prev != nil {
		t.Error("node3.prev should be nil after delisting head")
	}

	// Delist tail
	delist(&list, &node1.member)
	if list.head != &node3.member {
		t.Error("head should be node3 after delisting tail")
	}
	if list.tail != &node3.member {
		t.Error("tail should be node3 after delisting tail")
	}

	// Delist last element
	delist(&list, &node3.member)
	if list.head != nil {
		t.Error("head should be nil after delisting last element")
	}
	if list.tail != nil {
		t.Error("tail should be nil after delisting last element")
	}

	// Re-enlist moves element to front
	enlistBefore(&list, list.head, &node1.member)
	enlistBefore(&list, list.head, &node2.member)
	enlistBefore(&list, list.head, &node3.member) // Order: node3 -> node2 -> node1
	enlistBefore(&list, list.head, &node1.member)  // Move tail to head. Order: node1 -> node3 -> node2
	if list.head != &node1.member {
		t.Error("head should be node1 after moving tail to head")
	}
	if list.tail != &node2.member {
		t.Error("tail should be node2 after moving tail to head")
	}
	if node1.member.next != &node3.member {
		t.Error("node1.next should be node3")
	}
	if node2.member.prev != &node3.member {
		t.Error("node2.prev should be node3")
	}

	// Test listHead and listTail helpers
	head := listHead[testNode](&list)
	if head == nil {
		t.Fatal("head should not be nil")
	}
	if head.value != 1 {
		t.Errorf("head value = %d, want 1", head.value)
	}
	// Access tail through listHead and iteration, or directly
	if list.tail == nil {
		t.Fatal("tail should not be nil")
	}
	tail := list.tail.owner.(*testNode)
	if tail == nil {
		t.Fatal("tail owner should not be nil")
	}
	if tail.value != 2 {
		t.Errorf("tail value = %d, want 2", tail.value)
	}
}

func TestListEnlistTail(t *testing.T) {
	list := cavlList{head: nil, tail: nil}
	node1 := testNode{value: 1, member: cavlListed{next: nil, prev: nil, owner: nil}}
	node2 := testNode{value: 2, member: cavlListed{next: nil, prev: nil, owner: nil}}
	node3 := testNode{value: 3, member: cavlListed{next: nil, prev: nil, owner: nil}}
	// Set the owner pointers
	node1.member.owner = &node1
	node2.member.owner = &node2
	node3.member.owner = &node3

	// Add single element at tail
	enlistTail(&list, &node1.member)
	if list.head != &node1.member {
		t.Error("head should be node1")
	}
	if list.tail != &node1.member {
		t.Error("tail should be node1")
	}

	// Add second element at tail. Order: node1 -> node2
	enlistTail(&list, &node2.member)
	if list.head != &node1.member {
		t.Error("head should be node1")
	}
	if list.tail != &node2.member {
		t.Error("tail should be node2")
	}
	if node2.member.prev != &node1.member {
		t.Error("node2.prev should be node1")
	}
	if node1.member.next != &node2.member {
		t.Error("node1.next should be node2")
	}

	// Add third element at tail. Order: node1 -> node2 -> node3
	enlistTail(&list, &node3.member)
	if list.head != &node1.member {
		t.Error("head should be node1")
	}
	if list.tail != &node3.member {
		t.Error("tail should be node3")
	}
	if node2.member.next != &node3.member {
		t.Error("node2.next should be node3")
	}
	if node3.member.prev != &node2.member {
		t.Error("node3.prev should be node2")
	}

	// Re-enlist moves element to back
	enlistTail(&list, &node1.member) // Move head to tail. Order: node2 -> node3 -> node1
	if list.head != &node2.member {
		t.Error("head should be node2")
	}
	if list.tail != &node1.member {
		t.Error("tail should be node1")
	}
	if node3.member.next != &node1.member {
		t.Error("node3.next should be node1")
	}
	if node1.member.prev != &node3.member {
		t.Error("node1.prev should be node3")
	}
	if node2.member.prev != nil {
		t.Error("node2.prev should be nil")
	}
	if node1.member.next != nil {
		t.Error("node1.next should be nil")
	}

	// Re-enlist tail is a no-op on ordering
	enlistTail(&list, &node1.member) // Order unchanged: node2 -> node3 -> node1
	if list.head != &node2.member {
		t.Error("head should still be node2")
	}
	if list.tail != &node1.member {
		t.Error("tail should still be node1")
	}

	// Mix enlistBefore and enlistTail
	delist(&list, &node1.member)
	delist(&list, &node2.member)
	delist(&list, &node3.member)
	list.head = nil
	list.tail = nil
	enlistTail(&list, &node2.member)
	enlistTail(&list, &node3.member) // Order: node2 -> node3
	enlistBefore(&list, list.head, &node1.member) // Order: node1 -> node2 -> node3
	if list.head != &node1.member {
		t.Error("head should be node1")
	}
	if list.tail != &node3.member {
		t.Error("tail should be node3")
	}
	if node1.member.next != &node2.member {
		t.Error("node1.next should be node2")
	}
	if node3.member.prev != &node2.member {
		t.Error("node3.prev should be node2")
	}
}

func TestListEnlistAfterBefore(t *testing.T) {
	list := cavlList{head: nil, tail: nil}
	a := testNode{value: 1, member: cavlListed{next: nil, prev: nil, owner: nil}}
	b := testNode{value: 2, member: cavlListed{next: nil, prev: nil, owner: nil}}
	c := testNode{value: 3, member: cavlListed{next: nil, prev: nil, owner: nil}}
	d := testNode{value: 4, member: cavlListed{next: nil, prev: nil, owner: nil}}
	e := testNode{value: 5, member: cavlListed{next: nil, prev: nil, owner: nil}}
	fNode := testNode{value: 6, member: cavlListed{next: nil, prev: nil, owner: nil}}
	// Set the owner pointers
	a.member.owner = &a
	b.member.owner = &b
	c.member.owner = &c
	d.member.owner = &d
	e.member.owner = &e
	fNode.member.owner = &fNode

	// Build a -> b and insert c after a (anchor->next != NULL)
	enlistTail(&list, &a.member)
	enlistTail(&list, &b.member)
	// enlist_after(list, anchor, member) is equivalent to enlistBefore(list, anchor.next, member)
	enlistBefore(&list, a.member.next, &c.member)
	if list.head != &a.member {
		t.Error("head should be a")
	}
	if list.tail != &b.member {
		t.Error("tail should be b")
	}
	if a.member.next != &c.member {
		t.Error("a.next should be c")
	}
	if b.member.prev != &c.member {
		t.Error("b.prev should be c")
	}

	// Insert d after b (anchor->next == NULL)
	// enlistBefore(list, b.member.next, &d.member) where b.member.next is nil
	enlistBefore(&list, b.member.next, &d.member)
	if list.tail != &d.member {
		t.Error("tail should be d")
	}
	if d.member.prev != &b.member {
		t.Error("d.prev should be b")
	}

	// Reset for next tests
	list.head = nil
	list.tail = nil
	for _, n := range []*testNode{&a, &b, &c, &d, &e, &fNode} {
		n.member.next = nil
		n.member.prev = nil
	}

	// Build a -> b and insert c after a (anchor->next != NULL)
	enlistTail(&list, &a.member)
	enlistTail(&list, &b.member)
	// enlist_after(list, anchor, member) is equivalent to enlistBefore(list, anchor.next, member)
	enlistBefore(&list, a.member.next, &c.member)
	if list.head != &a.member {
		t.Error("head should be a")
	}
	if list.tail != &b.member {
		t.Error("tail should be b")
	}
	if a.member.next != &c.member {
		t.Error("a.next should be c")
	}
	if b.member.prev != &c.member {
		t.Error("b.prev should be c")
	}

	// Insert d after b (anchor->next == NULL)
	// enlistBefore(list, b.member.next, &d.member) where b.member.next is nil
	enlistBefore(&list, b.member.next, &d.member)
	if list.tail != &d.member {
		t.Error("tail should be d")
	}
	if d.member.prev != &b.member {
		t.Error("d.prev should be b")
	}

	// Insert e before c (anchor->prev != NULL)
	enlistBefore(&list, &c.member, &e.member)
	if a.member.next != &e.member {
		t.Error("a.next should be e")
	}
	if e.member.prev != &a.member {
		t.Error("e.prev should be a")
	}
	if e.member.next != &c.member {
		t.Error("e.next should be c")
	}
	if c.member.prev != &e.member {
		t.Error("c.prev should be e")
	}

	// Reset for next test
	list.head = nil
	list.tail = nil
	for _, n := range []*testNode{&a, &b, &c, &d, &e, &fNode} {
		n.member.next = nil
		n.member.prev = nil
	}

	// Insert f before head (anchor->prev == NULL)
	enlistTail(&list, &a.member)
	enlistTail(&list, &b.member)
	// enlistBefore(list, &a.member, &f.member) inserts f before a
	enlistBefore(&list, &a.member, &fNode.member)
	if list.head != &fNode.member {
		t.Error("head should be f")
	}
	if fNode.member.next != &a.member {
		t.Error("f.next should be a")
	}
	if a.member.prev != &fNode.member {
		t.Error("a.prev should be f")
	}
}

// =============================================================================
// Refcount Tests
// =============================================================================

func TestRefcountInc(t *testing.T) {
	c := &Canard{}
	c.Mem = NewDefaultMemSet()

	// Create a frame and bump the refcount
	frame := txFrameNew(c, 1)
	if frame == nil {
		t.Fatal("frame should not be nil")
	}
	view := txFrameView(frame)
	RefCountInc(view)
	if frame.refcount != 2 {
		t.Errorf("refcount = %d, want 2", frame.refcount)
	}

	// Drop the references
	c.RefCountDec(view)
	c.RefCountDec(view)
	if c.tx.queueSize != 0 {
		t.Errorf("queueSize = %d, want 0", c.tx.queueSize)
	}
}

// =============================================================================
// Bitmap Tests
// =============================================================================

func TestBitmapBoundaries(t *testing.T) {
	// Test the limb boundary bits: 0, 63 (MSB of limb 0), 64 (LSB of limb 1), 127 (MSB of limb 1)
	positions := []int{0, 63, 64, 127}
	for _, pos := range positions {
		var b [2]uint64
		bitmapSet(&b, pos)
		if !bitmapTest(&b, pos) {
			t.Errorf("bitmapTest(%d) should be true", pos)
		}
		// Verify neighbors are unaffected
		for i := 0; i < 128; i++ {
			if i != pos && bitmapTest(&b, i) {
				t.Errorf("bitmapTest(%d) should be false when only %d is set", i, pos)
			}
		}
	}
}

func TestBitmapAllBitsRoundTrip(t *testing.T) {
	// For each bit position 0..127: zero bitmap, set one bit, verify only that bit reads true
	for i := 0; i < 128; i++ {
		var b [2]uint64
		bitmapSet(&b, i)
		for j := 0; j < 128; j++ {
			if j == i {
				if !bitmapTest(&b, j) {
					t.Errorf("bitmapTest(%d) should be true", j)
				}
			} else {
				if bitmapTest(&b, j) {
					t.Errorf("bitmapTest(%d) should be false when only %d is set", j, i)
				}
			}
		}
	}
}

func TestBitmapSetIdempotent(t *testing.T) {
	var b [2]uint64
	bitmapSet(&b, 42)
	if popcount(b[0])+popcount(b[1]) != 1 {
		t.Errorf("popcount = %d, want 1", popcount(b[0])+popcount(b[1]))
	}
	bitmapSet(&b, 42) // no-op
	if popcount(b[0])+popcount(b[1]) != 1 {
		t.Errorf("popcount after second set = %d, want 1", popcount(b[0])+popcount(b[1]))
	}
	if !bitmapTest(&b, 42) {
		t.Error("bitmapTest(42) should be true")
	}
}

func TestBitmapAccumulation(t *testing.T) {
	var b [2]uint64
	positions := []int{0, 1, 42, 63, 64, 127}
	for k, pos := range positions {
		bitmapSet(&b, pos)
		// After setting k+1 bits, verify all expected bits are set
		for c := 0; c <= k; c++ {
			if !bitmapTest(&b, positions[c]) {
				t.Errorf("bitmapTest(%d) should be true after setting %d bits", positions[c], k+1)
			}
		}
		if popcount(b[0])+popcount(b[1]) != uint8(k+1) {
			t.Errorf("popcount = %d, want %d", popcount(b[0])+popcount(b[1]), k+1)
		}
	}
}

// =============================================================================
// Random Tests
// =============================================================================

func TestRandomBoundZero(t *testing.T) {
	c := &Canard{}
	c.PRNGState = 12345
	prngBefore := c.PRNGState
	// Note: random is a method in Go, not a field
	// We need to use reflection or make it accessible. For now, we'll test through a helper
	// that we'll add. But since we can't access unexported methods directly,
	// we'll test the PRNG state change indirectly through chance
	// Actually, let's use the internal random function directly
	if random(c, 0) != 0 {
		t.Errorf("random(0) = %d, want 0", random(c, 0))
	}
	// prng_state must be unchanged because splitmix64 is NOT called when bound==0
	if c.PRNGState != prngBefore {
		t.Errorf("PRNGState changed: %d, want %d", c.PRNGState, prngBefore)
	}
}

func TestRandomBoundOne(t *testing.T) {
	// random(self, 1) must always return 0 since splitmix64(...) % 1 == 0
	seeds := []uint64{0, 1, 42, ^uint64(0), 0xDEADBEEF}
	for _, seed := range seeds {
		c := &Canard{}
		c.PRNGState = seed
		if random(c, 1) != 0 {
			t.Errorf("random(1) with seed %d = %d, want 0", seed, random(c, 1))
		}
	}
}

func TestRandomRangeExhaustive(t *testing.T) {
	// For several bounds, verify all results are in [0, bound) and all values appear at least once
	bounds := []uint64{2, 3, 127, 128}
	for _, bound := range bounds {
		seen := make([]bool, bound)
		c := &Canard{}
		c.PRNGState = 0
		for i := 0; i < 2000; i++ {
			r := random(c, bound)
			if r >= bound {
				t.Errorf("random(%d) = %d, want < %d", bound, r, bound)
			}
			seen[r] = true
		}
		for v := uint64(0); v < bound; v++ {
			if !seen[v] {
				t.Errorf("value %d not seen in random(%d) outputs", v, bound)
			}
		}
	}
}

// =============================================================================
// Chance Tests
// =============================================================================

func TestChanceDeterministicEdges(t *testing.T) {
	// chance(self, 0): random(self, 0) returns 0, so 0==0 is true
	for i := 0; i < 10; i++ {
		c := &Canard{}
		c.PRNGState = uint64(i)
		if !chance(c, 0) {
			t.Errorf("chance(0) should be true with seed %d", i)
		}
	}
	// chance(self, 1): random(self, 1) returns 0, so 0==0 is true
	for i := 0; i < 10; i++ {
		c := &Canard{}
		c.PRNGState = uint64(i)
		if !chance(c, 1) {
			t.Errorf("chance(1) should be true with seed %d", i)
		}
	}
}

func TestChanceStatistical(t *testing.T) {
	// p_reciprocal=2: expect ~50% true
	{
		c := &Canard{}
		c.PRNGState = 0
		count := 0
		for i := 0; i < 10000; i++ {
			if chance(c, 2) {
				count++
			}
		}
		if count <= 4500 || count >= 5500 {
			t.Errorf("chance(2) count = %d, want between 4500 and 5500", count)
		}
	}
	// p_reciprocal=10: expect ~10% true
	{
		c := &Canard{}
		c.PRNGState = 0
		count := 0
		for i := 0; i < 10000; i++ {
			if chance(c, 10) {
				count++
			}
		}
		if count <= 500 || count >= 1500 {
			t.Errorf("chance(10) count = %d, want between 500 and 1500", count)
		}
	}
}

// =============================================================================
// Additional CRC Tests
// =============================================================================

func TestCrcAddEmpty(t *testing.T) {
	// crc_add with size=0 and NULL pointer must be an identity operation
	if crcAdd(crcInitial, 0, nil) != crcInitial {
		t.Errorf("crcAdd with size=0 and nil = 0x%04X, want 0xFFFF", crcAdd(crcInitial, 0, nil))
	}
	// Same with a non-NULL pointer but size=0
	dummy := byte(0xAA)
	if crcAdd(crcInitial, 0, []byte{dummy}) != crcInitial {
		t.Errorf("crcAdd with size=0 and non-nil = 0x%04X, want 0xFFFF", crcAdd(crcInitial, 0, []byte{dummy}))
	}
	// Also verify identity with a non-initial CRC value
	if crcAdd(0x1234, 0, nil) != 0x1234 {
		t.Errorf("crcAdd(0x1234, 0, nil) = 0x%04X, want 0x1234", crcAdd(0x1234, 0, nil))
	}
	if crcAdd(0x1234, 0, []byte{dummy}) != 0x1234 {
		t.Errorf("crcAdd(0x1234, 0, non-nil) = 0x%04X, want 0x1234", crcAdd(0x1234, 0, []byte{dummy}))
	}
}

func TestCrcResidueProperty(t *testing.T) {
	// Compute CRC over "Hello", then append CRC bytes (big-endian: high byte first, low byte second)
	// CRC over the whole (data + CRC) must equal CRC_RESIDUE
	data := []byte("Hello")
	crc := crcAdd(crcInitial, len(data), data)
	// Append CRC in big-endian order
	augmented := append([]byte(nil), data...)
	augmented = append(augmented, byte(crc>>8), byte(crc&0xFF))
	residue := crcAdd(crcInitial, len(augmented), augmented)
	if residue != crcResidue {
		t.Errorf("crc residue = 0x%04X, want 0x%04X", residue, crcResidue)
	}

	// Repeat with the standard test vector "123456789"
	vec := []byte("123456789")
	crc2 := crcAdd(crcInitial, len(vec), vec)
	if crc2 != 0x29B1 {
		t.Fatalf("crc2 = 0x%04X, want 0x29B1", crc2)
	}
	aug2 := append([]byte(nil), vec...)
	aug2 = append(aug2, byte(crc2>>8), byte(crc2&0xFF))
	if crcAdd(crcInitial, len(aug2), aug2) != crcResidue {
		t.Error("crc residue for '123456789' should be CRC_RESIDUE")
	}
}

func TestCrcAddChainEmptyFragments(t *testing.T) {
	// In Go, crcAddChain just wraps crcAdd, so we test with concatenated data
	// Simulating: [3 bytes "ABC"] -> [0 bytes] -> [2 bytes "DE"]
	// This is equivalent to "ABCDE"
	d1 := []byte("ABC")
	d3 := []byte("DE")
	combined := append(d1, d3...)
	chainCrc := crcAddChain(crcInitial, combined)
	// Must match the flat computation over "ABCDE"
	flat := []byte("ABCDE")
	flatCrc := crcAdd(crcInitial, len(flat), flat)
	if chainCrc != flatCrc {
		t.Errorf("crcAddChain with empty fragments = 0x%04X, want 0x%04X", chainCrc, flatCrc)
	}
}


