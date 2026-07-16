package libcanard

// This file implements an intrusive AVL tree compatible with the CAVL2 abstraction used by the original libcanard.
// The root node's up pointer points to itself; a node is "inserted" iff its up pointer is non-nil.
func branch(c int32) int {
	if c < 0 {
		return 0
	}
	return 1
}

func cavlIsInserted(n *cavlNode) bool {
	if n == nil {
		return false
	}
	return n.up != nil
}

func cavlMin(n *cavlNode) *cavlNode {
	if n == nil {
		return nil
	}
	for n.lr[0] != nil {
		n = n.lr[0]
	}
	return n
}

func cavlMax(n *cavlNode) *cavlNode {
	if n == nil {
		return nil
	}
	for n.lr[1] != nil {
		n = n.lr[1]
	}
	return n
}

func cavlNextGreater(n *cavlNode) *cavlNode {
	if n == nil {
		return nil
	}
	if n.lr[1] != nil {
		return cavlMin(n.lr[1])
	}
	p := n.up
	// Handle root self-loop: if p == n, we've reached the end
	if p == n {
		return nil
	}
	for p != nil && n == p.lr[1] {
		n = p
		p = p.up
		// Handle root self-loop during traversal
		if p == n {
			return nil
		}
	}
	return p
}

func cavlHeight(n *cavlNode) int {
	if n == nil {
		return 0
	}
	return int(n.height)
}

func updateHeight(n *cavlNode) {
	h0 := cavlHeight(n.lr[0])
	h1 := cavlHeight(n.lr[1])
	oldHeight := n.height
	oldBf := n.bf
	if h0 > h1 {
		n.height = int8(h0 + 1)
	} else {
		n.height = int8(h1 + 1)
	}
	n.bf = int8(h1 - h0)
	if oldHeight != n.height || oldBf != n.bf {
	}
}

func cavlRecomputeBF(n *cavlNode) {
	updateHeight(n)
}

func cavlUpdateBF(n *cavlNode) {
	for n != nil {
		cavlRecomputeBF(n)
		if n.up == n {
			break
		}
		n = n.up
	}
}

func cavlRotateLeft(t **cavlNode, x *cavlNode) {
	y := x.lr[1]
	if y == nil {
		return
	}
	x.lr[1] = y.lr[0]
	if y.lr[0] != nil {
		y.lr[0].up = x
	}
	y.up = x.up
	if x.up == x {
		*t = y
		y.up = y
	} else {
		if x.up.lr[0] == x {
			x.up.lr[0] = y
		} else {
			x.up.lr[1] = y
		}
	}
	x.up = y
	y.lr[0] = x
	updateHeight(x)
	updateHeight(y)
}

func cavlRotateRight(t **cavlNode, x *cavlNode) {
	y := x.lr[0]
	if y == nil {
		return
	}
	x.lr[0] = y.lr[1]
	if y.lr[1] != nil {
		y.lr[1].up = x
	}
	y.up = x.up
	if x.up == x {
		*t = y
		y.up = y
	} else {
		if x.up.lr[0] == x {
			x.up.lr[0] = y
		} else {
			x.up.lr[1] = y
		}
	}
	x.up = y
	y.lr[1] = x
	updateHeight(x)
	updateHeight(y)
}

func cavlRebalanceInsert(t **cavlNode, n *cavlNode) {
	for n != nil {
		cavlRecomputeBF(n)
		if n.bf > 1 {
			// Right-heavy
			if n.lr[1] != nil && n.lr[1].bf < 0 {
				// Right-left case: rotate right at right child, then left at n
				cavlRotateRight(t, n.lr[1])
			}
			cavlRotateLeft(t, n)
			break
		} else if n.bf < -1 {
			// Left-heavy
			if n.lr[0] != nil && n.lr[0].bf > 0 {
				// Left-right case: rotate left at left child, then right at n
				cavlRotateLeft(t, n.lr[0])
			}
			cavlRotateRight(t, n)
			break
		}
		if n.bf == 0 {
			break
		}
		if n.up == n {
			break
		}
		n = n.up
	}
}

func cavlRebalanceDelete(t **cavlNode, n *cavlNode) {
	for n != nil {
		cavlRecomputeBF(n)
		if n.lr[0] == nil && n.lr[1] == nil && n != *t {
		}
		if n.bf > 1 {
			// Right-heavy
			if n.lr[1] != nil && n.lr[1].bf < 0 {
				// Right-left case
				cavlRotateRight(t, n.lr[1])
			}
			cavlRotateLeft(t, n)
			// After rotation, continue up because height may have changed
			n = n.up
			if n == nil || n.up == n {
				break
			}
			continue
		} else if n.bf < -1 {
			// Left-heavy
			if n.lr[0] != nil && n.lr[0].bf > 0 {
				// Left-right case
				cavlRotateLeft(t, n.lr[0])
			}
			cavlRotateRight(t, n)
			// After rotation, continue up
			n = n.up
			if n == nil || n.up == n {
				break
			}
			continue
		} else if n.bf == 0 {
			// Height decreased, continue up
			n = n.up
			if n == nil || n.up == n {
				break
			}
			continue
		}
		// |bf| == 1: height unchanged, stop
		break
	}
}

func cavlFind(root **cavlNode, key *cavlNode, cmp func(*cavlNode, *cavlNode) int32) *cavlNode {
	p := *root
	for p != nil {
		c := cmp(key, p)
		if c == 0 {
			return p
		}
		dir := branch(c)
		if p.lr[dir] == nil {
			break
		}
		p = p.lr[dir]
	}
	return p
}

func cavlFindOrInsert(t **cavlNode, key *cavlNode, cmp func(*cavlNode, *cavlNode) int32, factory func() *cavlNode) *cavlNode {
	if *t == nil {
		node := factory()
		if node == nil {
			return nil
		}
		node.up = node
		node.height = 1
		*t = node
		return node
	}
	var parent *cavlNode
	p := *t
	for p != nil {
		parent = p
		c := cmp(key, p)
		if c == 0 {
			return p
		}
		dir := branch(c)
		if p.lr[dir] == nil {
			break
		}
		p = p.lr[dir]
	}
	if cmp(key, parent) == 0 {
		return parent
	}
	node := factory()
	if node == nil {
		return nil
	}
	node.up = parent
	node.height = 1
	dir := branch(cmp(key, parent))
	parent.lr[dir] = node
	if parent.lr[dir] != node {
	}
	cavlRebalanceInsert(t, parent)
	return node
}

func cavlTransplant(t **cavlNode, u, v *cavlNode) {
	if u.up == u {
		*t = v
		if v != nil {
			v.up = v
		}
		return
	}
	p := u.up
	if p.lr[0] == u {
		p.lr[0] = v
	} else {
		p.lr[1] = v
	}
	if v != nil {
		v.up = p
	}
}

func clearNode(n *cavlNode) {
	n.up = nil
	n.lr[0] = nil
	n.lr[1] = nil
	n.bf = 0
}

// cavlRemove removes node z from the tree rooted at *t.
func cavlRemove(t **cavlNode, z *cavlNode) {
	if z.lr[0] == nil {
		child := z.lr[1]
		p := z.up
		cavlTransplant(t, z, child)
		var start *cavlNode
		if p == z {
			start = child
		} else {
			start = p
		}
		cavlRebalanceDelete(t, start)
		clearNode(z)
		return
	}
	if z.lr[1] == nil {
		child := z.lr[0]
		p := z.up
		cavlTransplant(t, z, child)
		var start *cavlNode
		if p == z {
			start = child
		} else {
			start = p
		}
		cavlRebalanceDelete(t, start)
		clearNode(z)
		return
	}
	// Two children: y is the in-order successor (minimum of the right subtree).
	y := cavlMin(z.lr[1])
	if y.up != z {
		// y is not the direct right child of z
		yp := y.up
		child := y.lr[1]
		cavlTransplant(t, y, child)
		start := yp
		y.lr[0] = z.lr[0]
		if y.lr[0] != nil {
			y.lr[0].up = y
		}
		y.lr[1] = z.lr[1]
		updateHeight(y)
		if y.lr[1] != nil {
			y.lr[1].up = y
		}
		cavlTransplant(t, z, y)
		cavlRebalanceDelete(t, start)
		clearNode(z)
		return
	}
	// y.up == z, so y is the direct right child
	gp := z.up
	child := y.lr[1]
	cavlTransplant(t, y, child)
	y.lr[0] = z.lr[0]
	if y.lr[0] != nil {
		y.lr[0].up = y
	}
	cavlTransplant(t, z, y)
	var start *cavlNode
	if gp == z {
		start = *t
	} else {
		start = gp
	}
	cavlRebalanceDelete(t, start)
	clearNode(z)
}

// cavlRemoveIf removes node if it is currently inserted. Returns true if it was removed.
func cavlRemoveIf(t **cavlNode, n *cavlNode) bool {
	if !cavlIsInserted(*t) || !cavlIsInserted(n) {
		return false
	}
	cavlRemove(t, n)
	return true
}

// cavlIterate invokes fn for every node in in-order sequence.
func cavlIterate(root *cavlNode, fn func(*cavlNode)) {
	n := cavlMin(root)
	for n != nil {
		fn(n)
		n = cavlNextGreater(n)
	}
}
