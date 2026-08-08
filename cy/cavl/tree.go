// Package cavl provides an AVL tree implementation.
// This is a Go port of the cavl2.h C library used by the cy library.
//
// AVL trees are self-balancing binary search trees where the difference between
// heights of left and right subtrees cannot be more than one for all nodes.
// This ensures O(log n) time complexity for insert, delete, and search operations.
package cavl

import (
	"fmt"
)

// Node is a single node in the AVL tree.
type Node[K comparable, V any] struct {
	Key    K
	Value  V
	parent *Node[K, V]
	left   *Node[K, V]
	right  *Node[K, V]
	height int8
}

// Tree is an AVL tree.
type Tree[K comparable, V any] struct {
	root    *Node[K, V]
	count   int
	compare func(a, b K) int
}

// New creates a new AVL tree with a custom comparison function.
// The comparison function should return:
//   - a negative number if a < b
//   - zero if a == b
//   - a positive number if a > b
func New[K comparable, V any](compare func(a, b K) int) *Tree[K, V] {
	return &Tree[K, V]{
		compare: compare,
	}
}

// Len returns the number of nodes in the tree.
func (t *Tree[K, V]) Len() int {
	return t.count
}

// Empty returns true if the tree is empty.
func (t *Tree[K, V]) Empty() bool {
	return t.root == nil
}

// Height returns the height of the tree.
func (t *Tree[K, V]) Height() int {
	return nodeHeight(t.root)
}

// nodeHeight returns the height of a node, or -1 for nil.
func nodeHeight[K comparable, V any](n *Node[K, V]) int {
	if n == nil {
		return -1
	}
	return int(n.height)
}

// updateHeight updates the height of a node based on its children.
func updateHeight[K comparable, V any](n *Node[K, V]) {
	leftHeight := nodeHeight(n.left)
	rightHeight := nodeHeight(n.right)
	if leftHeight > rightHeight {
		n.height = int8(leftHeight + 1)
	} else {
		n.height = int8(rightHeight + 1)
	}
}

// balanceFactor returns the balance factor of a node.
// Balance factor = height(left) - height(right)
func balanceFactor[K comparable, V any](n *Node[K, V]) int {
	return nodeHeight(n.left) - nodeHeight(n.right)
}

// rotateRight performs a right rotation.
func rotateRight[K comparable, V any](t *Tree[K, V], x *Node[K, V]) *Node[K, V] {
	y := x.left
	if y == nil {
		return x
	}

	// Perform rotation
	x.left = y.right
	if y.right != nil {
		y.right.parent = x
	}
	y.parent = x.parent

	if x.parent == nil {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}

	y.right = x
	x.parent = y

	// Update heights
	updateHeight(x)
	updateHeight(y)

	return y
}

// rotateLeft performs a left rotation.
func rotateLeft[K comparable, V any](t *Tree[K, V], x *Node[K, V]) *Node[K, V] {
	y := x.right
	if y == nil {
		return x
	}

	// Perform rotation
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent

	if x.parent == nil {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}

	y.left = x
	x.parent = y

	// Update heights
	updateHeight(x)
	updateHeight(y)

	return y
}

// balance balances the tree at the given node.
func balance[K comparable, V any](t *Tree[K, V], n *Node[K, V]) *Node[K, V] {
	updateHeight(n)
	bf := balanceFactor(n)

	// Left heavy
	if bf > 1 {
		if balanceFactor(n.left) < 0 {
			// Left-Right case
			n.left = rotateLeft(t, n.left)
		}
		// Left-Left case
		return rotateRight(t, n)
	}

	// Right heavy
	if bf < -1 {
		if balanceFactor(n.right) > 0 {
			// Right-Left case
			n.right = rotateRight(t, n.right)
		}
		// Right-Right case
		return rotateLeft(t, n)
	}

	return n
}

// Insert inserts a key-value pair into the tree.
// If the key already exists, the value is updated and the old node is returned.
// Otherwise, the new node is returned.
func (t *Tree[K, V]) Insert(key K, value V) *Node[K, V] {
	var inserted *Node[K, V]
	t.root, inserted = t.insertRecursive(t.root, nil, key, value)

	if inserted != nil {
		t.count++
	}

	return inserted
}

// insertRecursive performs the actual insertion.
func (t *Tree[K, V]) insertRecursive(n *Node[K, V], parent *Node[K, V], key K, value V) (*Node[K, V], *Node[K, V]) {
	if n == nil {
		newNode := &Node[K, V]{
			Key:    key,
			Value:  value,
			parent: parent,
			height: 0,
		}
		return newNode, newNode
	}

	cmp := t.compare(key, n.Key)
	if cmp < 0 {
		left, inserted := t.insertRecursive(n.left, n, key, value)
		n.left = left
		if inserted != nil {
			return balance(t, n), inserted
		}
		return n, nil
	} else if cmp > 0 {
		right, inserted := t.insertRecursive(n.right, n, key, value)
		n.right = right
		if inserted != nil {
			return balance(t, n), inserted
		}
		return n, nil
	} else {
		// Key already exists, update value but don't count as new insertion
		n.Value = value
		return n, nil
	}
}

// Get retrieves the value associated with a key.
// Returns the value and true if found, or zero value and false if not found.
func (t *Tree[K, V]) Get(key K) (V, bool) {
	n := t.Find(key)
	if n == nil {
		var zero V
		return zero, false
	}
	return n.Value, true
}

// Find returns the node with the given key, or nil if not found.
func (t *Tree[K, V]) Find(key K) *Node[K, V] {
	return t.findRecursive(t.root, key)
}

// findRecursive performs the actual find.
func (t *Tree[K, V]) findRecursive(n *Node[K, V], key K) *Node[K, V] {
	if n == nil {
		return nil
	}

	cmp := t.compare(key, n.Key)
	if cmp < 0 {
		return t.findRecursive(n.left, key)
	} else if cmp > 0 {
		return t.findRecursive(n.right, key)
	} else {
		return n
	}
}

// Contains returns true if the tree contains the given key.
func (t *Tree[K, V]) Contains(key K) bool {
	return t.Find(key) != nil
}

// Delete removes a node with the given key from the tree.
// Returns the removed node, or nil if not found.
func (t *Tree[K, V]) Delete(key K) *Node[K, V] {
	var removed *Node[K, V]
	t.root, removed = t.deleteRecursive(t.root, key)
	if removed != nil {
		t.count--
	}
	return removed
}

// deleteRecursive performs the actual deletion.
func (t *Tree[K, V]) deleteRecursive(n *Node[K, V], key K) (*Node[K, V], *Node[K, V]) {
	if n == nil {
		return nil, nil
	}

	cmp := t.compare(key, n.Key)
	var removed *Node[K, V]

	if cmp < 0 {
		n.left, removed = t.deleteRecursive(n.left, key)
		if removed != nil {
			return balance(t, n), removed
		}
		return n, nil
	} else if cmp > 0 {
		n.right, removed = t.deleteRecursive(n.right, key)
		if removed != nil {
			return balance(t, n), removed
		}
		return n, nil
	} else {
		// Found the node to delete
		removed = n

		// Node with only one child or no child
		if n.left == nil {
			return n.right, removed
		} else if n.right == nil {
			return n.left, removed
		}

		// Node with two children: get inorder successor (smallest in right subtree)
		successor := t.findMin(n.right)
		n.Key = successor.Key
		n.Value = successor.Value
		// Don't decrement count here - it will be decremented by the recursive call
		n.right, _ = t.deleteRecursive(n.right, successor.Key)
		return balance(t, n), removed
	}
}

// findMin finds the node with the minimum key in a subtree.
func (t *Tree[K, V]) findMin(n *Node[K, V]) *Node[K, V] {
	for n.left != nil {
		n = n.left
	}
	return n
}

// DeleteNode removes a specific node from the tree.
func (t *Tree[K, V]) DeleteNode(n *Node[K, V]) *Node[K, V] {
	if n == nil {
		return nil
	}

	removed, newRoot := t.deleteNodeRecursive(t.root, n)
	if removed != nil {
		t.count--
		t.root = newRoot
	}
	return removed
}

// deleteNodeRecursive performs the actual deletion of a specific node.
func (t *Tree[K, V]) deleteNodeRecursive(n, toDelete *Node[K, V]) (*Node[K, V], *Node[K, V]) {
	if n == nil {
		return nil, nil
	}

	cmp := t.compare(toDelete.Key, n.Key)

	if cmp < 0 {
		left, removed := t.deleteNodeRecursive(n.left, toDelete)
		n.left = left
		if removed != nil {
			return balance(t, n), removed
		}
		return n, nil
	} else if cmp > 0 {
		right, removed := t.deleteNodeRecursive(n.right, toDelete)
		n.right = right
		if removed != nil {
			return balance(t, n), removed
		}
		return n, nil
	} else {
		// Found the node to delete
		// Node with only one child or no child
		if n.left == nil {
			return n.right, n
		} else if n.right == nil {
			return n.left, n
		}

		// Node with two children: get inorder successor
		successor := t.findMin(n.right)
		n.Key = successor.Key
		n.Value = successor.Value
		n.right, _ = t.deleteNodeRecursive(n.right, successor)
		return balance(t, n), toDelete
	}
}

// Clear removes all nodes from the tree.
func (t *Tree[K, V]) Clear() {
	t.root = nil
	t.count = 0
}

// Keys returns all keys in the tree in sorted order.
func (t *Tree[K, V]) Keys() []K {
	keys := make([]K, 0, t.count)
	t.inOrder(t.root, func(n *Node[K, V]) {
		keys = append(keys, n.Key)
	})
	return keys
}

// Values returns all values in the tree in key-sorted order.
func (t *Tree[K, V]) Values() []V {
	values := make([]V, 0, t.count)
	t.inOrder(t.root, func(n *Node[K, V]) {
		values = append(values, n.Value)
	})
	return values
}

// inOrder performs an in-order traversal.
func (t *Tree[K, V]) inOrder(n *Node[K, V], visit func(*Node[K, V])) {
	if n == nil {
		return
	}
	t.inOrder(n.left, visit)
	visit(n)
	t.inOrder(n.right, visit)
}

// Iterate calls the function for each node in the tree in sorted order.
func (t *Tree[K, V]) Iterate(fn func(key K, value V)) {
	t.inOrder(t.root, func(n *Node[K, V]) {
		fn(n.Key, n.Value)
	})
}

// Validate checks if the tree is a valid AVL tree.
func (t *Tree[K, V]) Validate() error {
	if t.root == nil {
		if t.count != 0 {
			return fmt.Errorf("tree has count %d but root is nil", t.count)
		}
		return nil
	}

	actualCount := 0
	if err := t.validateRecursive(t.root, nil, &actualCount); err != nil {
		return err
	}

	if actualCount != t.count {
		return fmt.Errorf("tree count mismatch: expected %d, got %d", t.count, actualCount)
	}

	return nil
}

// validateRecursive validates a subtree.
func (t *Tree[K, V]) validateRecursive(n *Node[K, V], parent *Node[K, V], count *int) error {
	*count++

	// Check parent pointer
	if n.parent != parent {
		return fmt.Errorf("parent pointer mismatch")
	}

	// Check height
	expectedHeight := int8(1 + max(nodeHeight(n.left), nodeHeight(n.right)))
	if n.height != expectedHeight {
		return fmt.Errorf("height mismatch: expected %d, got %d", expectedHeight, n.height)
	}

	// Check balance factor
	bf := balanceFactor(n)
	if bf < -1 || bf > 1 {
		return fmt.Errorf("balance factor out of range: %d", bf)
	}

	// Check ordering
	if n.left != nil {
		if t.compare(n.left.Key, n.Key) >= 0 {
			return fmt.Errorf("left child key >= parent key")
		}
		if err := t.validateRecursive(n.left, n, count); err != nil {
			return err
		}
	}

	if n.right != nil {
		if t.compare(n.right.Key, n.Key) <= 0 {
			return fmt.Errorf("right child key <= parent key")
		}
		if err := t.validateRecursive(n.right, n, count); err != nil {
			return err
		}
	}

	return nil
}

// First returns the node with the smallest key.
func (t *Tree[K, V]) First() *Node[K, V] {
	return t.findMin(t.root)
}

// Last returns the node with the largest key.
func (t *Tree[K, V]) Last() *Node[K, V] {
	return t.findMax(t.root)
}

// findMax finds the node with the maximum key in a subtree.
func (t *Tree[K, V]) findMax(n *Node[K, V]) *Node[K, V] {
	for n.right != nil {
		n = n.right
	}
	return n
}

// Next returns the next node in sorted order, or nil if there is no next node.
func Next[K comparable, V any](n *Node[K, V]) *Node[K, V] {
	if n == nil {
		return nil
	}

	// If we have a right subtree, the next node is the minimum in that subtree
	if n.right != nil {
		return findMinInSubtree(n.right)
	}

	// Otherwise, we need to go up until we find a parent where we're the left child
	for n.parent != nil && n == n.parent.right {
		n = n.parent
	}

	return n.parent
}

// findMinInSubtree finds the minimum node in a subtree.
func findMinInSubtree[K comparable, V any](n *Node[K, V]) *Node[K, V] {
	for n.left != nil {
		n = n.left
	}
	return n
}

// Prev returns the previous node in sorted order, or nil if there is no previous node.
func Prev[K comparable, V any](n *Node[K, V]) *Node[K, V] {
	if n == nil {
		return nil
	}

	// If we have a left subtree, the previous node is the maximum in that subtree
	if n.left != nil {
		return findMaxInSubtree(n.left)
	}

	// Otherwise, we need to go up until we find a parent where we're the right child
	for n.parent != nil && n == n.parent.left {
		n = n.parent
	}

	return n.parent
}

// findMaxInSubtree finds the maximum node in a subtree.
func findMaxInSubtree[K comparable, V any](n *Node[K, V]) *Node[K, V] {
	for n.right != nil {
		n = n.right
	}
	return n
}

// max returns the maximum of two integers.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
