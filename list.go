package libcanard

// delist removes member from list. No effect if not in the list.
func delist(list *cavlList, member *cavlListed) {
	if member.next != nil {
		member.next.prev = member.prev
	}
	if member.prev != nil {
		member.prev.next = member.next
	}
	if list.head == member {
		list.head = member.next
	}
	if list.tail == member {
		list.tail = member.prev
	}
	member.next = nil
	member.prev = nil
}

// enlistBefore inserts addendum before anchor. If anchor is nil, inserts at the tail. If the item is already in the
// list it is delisted first (so this can be used to move it to the specified position).
func enlistBefore(list *cavlList, anchor *cavlListed, addendum *cavlListed) {
	delist(list, addendum)
	if anchor == nil {
		addendum.prev = list.tail
		if list.tail != nil {
			list.tail.next = addendum
		}
		list.tail = addendum
		if list.head == nil {
			list.head = addendum
		}
	} else {
		addendum.next = anchor
		addendum.prev = anchor.prev
		if anchor.prev != nil {
			anchor.prev.next = addendum
		} else {
			list.head = addendum
		}
		anchor.prev = addendum
	}
}

// enlistTail inserts member at the tail of the list.
func enlistTail(list *cavlList, member *cavlListed) {
	enlistBefore(list, nil, member)
}

// listHead returns the owner of the head node, or nil if the list is empty.
func listHead[T any](list *cavlList) *T {
	if list.head == nil {
		return nil
	}
	return list.head.owner.(*T)
}

// listNext returns the owner of the next node, or nil if there is none.
func listNext[T any](member *cavlListed) *T {
	if member.next == nil {
		return nil
	}
	return member.next.owner.(*T)
}
