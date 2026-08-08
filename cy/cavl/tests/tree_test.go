package tests

import (
	"testing"

	"github.com/opencyphal/cy-go/cavl"
)

// uint64Compare is a comparison function for uint64.
func uint64Compare(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// stringCompare is a comparison function for strings.
func stringCompare(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// TestTreeInsertAndFind tests basic insertion and lookup.
func TestTreeInsertAndFind(t *testing.T) {
	// Create a tree with uint64 keys and uint64 values
	tree := cavl.New[uint64, uint64](uint64Compare)
	
	// Insert some values
	values := []uint64{5, 3, 7, 2, 4, 6, 8, 1, 9}
	for _, v := range values {
		tree.Insert(v, v)
	}
	
	// Verify all values can be found
	for _, v := range values {
		if !tree.Contains(v) {
			t.Errorf("Failed to find value %d after insertion", v)
		}
	}
	
	// Verify non-existent values are not found
	if tree.Contains(0) {
		t.Error("Found non-existent value 0")
	}
	if tree.Contains(10) {
		t.Error("Found non-existent value 10")
	}
}

// TestTreeInsertDuplicate tests that duplicates are not inserted.
func TestTreeInsertDuplicate(t *testing.T) {
	// Create a tree with uint64 keys and string values
	tree := cavl.New[uint64, string](uint64Compare)
	
	// Insert a value
	tree.Insert(uint64(42), "first")
	
	// Insert it again with different value
	oldSize := tree.Len()
	tree.Insert(uint64(42), "second")
	
	// Size should not have changed
	if tree.Len() != oldSize {
		t.Error("Tree size changed after inserting duplicate")
	}
	
	// But the value should be updated
	val, ok := tree.Get(uint64(42))
	if !ok {
		t.Error("Failed to get value")
	}
	if val != "second" {
		t.Errorf("Expected value 'second', got %v", val)
	}
}

// TestTreeRemove tests removal of nodes.
func TestTreeRemove(t *testing.T) {
	tree := cavl.New[uint64, uint64](uint64Compare)
	
	// Insert values
	values := []uint64{5, 3, 7, 2, 4, 6, 8}
	for _, v := range values {
		tree.Insert(v, v)
	}
	
	// Remove a leaf node
	tree.Delete(2)
	if tree.Contains(2) {
		t.Error("Found removed value 2")
	}
	
	// Remove a node with one child
	tree.Delete(3)
	if tree.Contains(3) {
		t.Error("Found removed value 3")
	}
	
	// Remove a node with two children
	tree.Delete(5)
	if tree.Contains(5) {
		t.Error("Found removed value 5")
	}
	
	// Verify remaining values
	remaining := []uint64{4, 6, 8}
	for _, v := range remaining {
		if !tree.Contains(v) {
			t.Errorf("Lost value %d after removals", v)
		}
	}
}

// TestTreeRemoveNonExistent tests removing a non-existent value.
func TestTreeRemoveNonExistent(t *testing.T) {
	tree := cavl.New[uint64, uint64](uint64Compare)
	
	// Insert some values
	tree.Insert(1, 1)
	tree.Insert(2, 2)
	tree.Insert(3, 3)
	
	// Try to remove a non-existent value
	oldSize := tree.Len()
	tree.Delete(42)
	
	// Size should not have changed
	if tree.Len() != oldSize {
		t.Error("Tree size changed after removing non-existent value")
	}
}

// TestTreeSize tests the size tracking.
func TestTreeSize(t *testing.T) {
	tree := cavl.New[uint64, uint64](uint64Compare)
	
	if tree.Len() != 0 {
		t.Errorf("New tree should have size 0, got %d", tree.Len())
	}
	
	// Insert 10 values
	for i := uint64(0); i < 10; i++ {
		tree.Insert(i, i)
	}
	
	if tree.Len() != 10 {
		t.Errorf("Expected size 10, got %d", tree.Len())
	}
	
	// Remove 5 values
	for i := uint64(0); i < 5; i++ {
		tree.Delete(i)
	}
	
	if tree.Len() != 5 {
		t.Errorf("Expected size 5 after removals, got %d", tree.Len())
	}
}

// TestTreeEmpty tests the Empty method.
func TestTreeEmpty(t *testing.T) {
	tree := cavl.New[uint64, uint64](uint64Compare)
	
	if !tree.Empty() {
		t.Error("New tree should be empty")
	}
	
	tree.Insert(1, 1)
	if tree.Empty() {
		t.Error("Tree with one element should not be empty")
	}
	
	tree.Delete(1)
	if !tree.Empty() {
		t.Error("Tree after removing last element should be empty")
	}
}

// TestTreeGet tests the Get method.
func TestTreeGet(t *testing.T) {
	tree := cavl.New[uint64, string](uint64Compare)
	
	// Insert values
	tree.Insert(1, "one")
	tree.Insert(2, "two")
	tree.Insert(3, "three")
	
	// Get existing values
	val, ok := tree.Get(1)
	if !ok {
		t.Error("Failed to get value for key 1")
	}
	if val != "one" {
		t.Errorf("Expected 'one', got %v", val)
	}
	
	// Get non-existent value
	_, ok = tree.Get(42)
	if ok {
		t.Error("Should not find non-existent key")
	}
}

// TestTreeIterate tests in-order iteration.
func TestTreeIterate(t *testing.T) {
	tree := cavl.New[uint64, uint64](uint64Compare)
	
	// Insert values in random order
	values := []uint64{5, 2, 8, 1, 3, 7, 9, 4, 6}
	for _, v := range values {
		tree.Insert(v, v)
	}
	
	// Iterate and collect
	var result []uint64
	tree.Iterate(func(key uint64, value uint64) {
		result = append(result, key)
	})
	
	// Should be sorted
	expected := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	if len(result) != len(expected) {
		t.Errorf("Expected %d values, got %d", len(expected), len(result))
		return
	}
	
	for i := range expected {
		if result[i] != expected[i] {
			t.Errorf("Expected value %d at index %d, got %d", expected[i], i, result[i])
		}
	}
}

// TestTreeMinMax tests finding minimum and maximum values.
func TestTreeMinMax(t *testing.T) {
	tree := cavl.New[uint64, uint64](uint64Compare)
	
	// Empty tree - Keys() should return empty slice
	keys := tree.Keys()
	if len(keys) != 0 {
		t.Error("Empty tree should have no keys")
	}
	
	// Insert values
	values := []uint64{5, 2, 8, 1, 9, 3}
	for _, v := range values {
		tree.Insert(v, v)
	}
	
	// Get all keys and find min/max
	keys = tree.Keys()
	if len(keys) != len(values) {
		t.Errorf("Expected %d keys, got %d", len(values), len(keys))
	}
	
	// Keys should be sorted
	for i := 1; i < len(keys); i++ {
		if keys[i-1] > keys[i] {
			t.Errorf("Keys are not sorted")
		}
	}
	
	// First key is min
	if keys[0] != 1 {
		t.Errorf("Expected min 1, got %d", keys[0])
	}
	
	// Last key is max
	if keys[len(keys)-1] != 9 {
		t.Errorf("Expected max 9, got %d", keys[len(keys)-1])
	}
	
	// Remove min
	tree.Delete(1)
	keys = tree.Keys()
	if keys[0] != 2 {
		t.Errorf("Expected min 2 after removing 1, got %d", keys[0])
	}
	
	// Remove max
	tree.Delete(9)
	keys = tree.Keys()
	if keys[len(keys)-1] != 8 {
		t.Errorf("Expected max 8 after removing 9, got %d", keys[len(keys)-1])
	}
}

// TestTreeClear tests clearing the tree.
func TestTreeClear(t *testing.T) {
	tree := cavl.New[uint64, uint64](uint64Compare)
	
	// Insert values
	for i := uint64(0); i < 100; i++ {
		tree.Insert(i, i)
	}
	
	if tree.Len() != 100 {
		t.Errorf("Expected size 100, got %d", tree.Len())
	}
	
	// Clear
	tree.Clear()
	
	if tree.Len() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", tree.Len())
	}
	
	if !tree.Empty() {
		t.Error("Tree should be empty after clear")
	}
	
	if tree.Contains(50) {
		t.Error("Found value after clear")
	}
}

// TestTreeHeight tests that the tree maintains balanced height.
func TestTreeHeight(t *testing.T) {
	tree := cavl.New[uint64, uint64](uint64Compare)
	
	// Insert many values in order (worst case for unbalanced tree)
	for i := uint64(0); i < 1000; i++ {
		tree.Insert(i, i)
	}
	
	// The height of an AVL tree with n nodes should be O(log n)
	// For n=1000, max height should be around 20-30
	height := tree.Height()
	if height > 50 {
		t.Errorf("Tree height %d is too large for %d nodes (should be O(log n))", height, tree.Len())
	}
}

// TestTreeWithDuplicates tests handling of duplicate insertions.
func TestTreeWithDuplicates(t *testing.T) {
	tree := cavl.New[uint64, int](uint64Compare)
	
	// Insert the same value many times with different data
	for i := 0; i < 100; i++ {
		tree.Insert(uint64(42), i)
	}
	
	// Should only have one instance
	if tree.Len() != 1 {
		t.Errorf("Expected size 1 with duplicates, got %d", tree.Len())
	}
	
	// Value should be the last one inserted
	val, ok := tree.Get(uint64(42))
	if !ok {
		t.Error("Failed to find value 42")
	}
	if val != 99 {
		t.Errorf("Expected value 99, got %v", val)
	}
}

// TestTreeStringKeys tests with string keys.
func TestTreeStringKeys(t *testing.T) {
	// Create a tree with string keys and string values
	tree := cavl.New[string, string](stringCompare)
	
	// Insert string keys
	keys := []string{"banana", "apple", "cherry", "date"}
	for _, k := range keys {
		tree.Insert(k, k)
	}
	
	// Verify all exist
	for _, k := range keys {
		if !tree.Contains(k) {
			t.Errorf("Failed to find key %s", k)
		}
	}
	
	// Check ordering via Keys()
	allKeys := tree.Keys()
	
	expected := []string{"apple", "banana", "cherry", "date"}
	if len(allKeys) != len(expected) {
		t.Errorf("Expected %d keys, got %d", len(expected), len(allKeys))
		return
	}
	
	for i := range expected {
		if allKeys[i] != expected[i] {
			t.Errorf("Expected key %s at index %d, got %s", expected[i], i, allKeys[i])
		}
	}
}
