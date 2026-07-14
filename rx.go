package libcanard

import "unsafe"

const rxSessionTimeout = 30 * 1000 * 1000 // 30 seconds, in microseconds.

// rxFrameParsed is a single CAN frame parsed into its transfer parameters.
type rxFrameParsed struct {
	priority    Prio
	kind        Kind
	portID      uint16
	dst         uint8
	src         uint8
	transferID  uint8
	start       bool
	end         bool
	toggle      bool
	payload     []byte
}

// rxParse parses a raw CAN frame into the v0 and/or v1 representations. It returns a bitmask: bit0 set if the frame
// parses as v0, bit1 if it parses as v1.
func rxParse(canID uint32, payloadRaw []byte) (byte, *rxFrameParsed, *rxFrameParsed) {
	outV0 := &rxFrameParsed{}
	outV1 := &rxFrameParsed{}
	if len(payloadRaw) < 1 {
		return 0, outV0, outV1
	}
	tail := payloadRaw[len(payloadRaw)-1]
	start := (tail & tailSOT) != 0
	end := (tail & tailEOT) != 0
	toggle := (tail & tailToggle) != 0
	transferID := tail & transferIDMax
	priority := Prio(canID >> prioShift)
	src := uint8(canID & NodeIDMax)
	payload := payloadRaw[:len(payloadRaw)-1]
	payloadOK := (end || len(payloadRaw) >= MTUClassic) && ((start && end) || len(payload) > 0)

	isV1 := !(start && !toggle) && payloadOK
	isV0 := !(start && toggle) && payloadOK

	if isV1 {
		outV1.priority = priority
		outV1.src = src
		outV1.transferID = transferID
		outV1.start = start
		outV1.end = end
		outV1.toggle = toggle
		outV1.payload = payload
		svc := (canID & (1 << 25)) != 0
		bit23 := (canID & (1 << 23)) != 0
		if svc {
			outV1.dst = uint8((canID >> 7) & NodeIDMax)
			outV1.portID = uint16((canID >> 14) & ServiceIDMax)
			req := (canID & (1 << 24)) != 0
			if req {
				outV1.kind = KindRequest
			} else {
				outV1.kind = KindResponse
			}
			isV1 = isV1 && !bit23 && (outV1.src != outV1.dst)
		} else {
			outV1.dst = NodeIDAnonymous
			is11 := (canID & (1 << 7)) != 0
			if is11 {
				bit24 := (canID & (1 << 24)) != 0
				isV1 = isV1 && !bit24
				outV1.portID = uint16((canID >> 8) & SubjectIDMax)
				outV1.kind = KindMessage16b
			} else {
				isV1 = isV1 && !bit23
				outV1.portID = uint16((canID >> 8) & SubjectIDMax13b)
				outV1.kind = KindMessage13b
				if (canID & (1 << 24)) != 0 {
					outV1.src = NodeIDAnonymous
					isV1 = isV1 && start && end
				}
			}
		}
	}
	if isV0 {
		outV0.priority = priority
		outV0.src = src
		outV0.transferID = transferID
		outV0.start = start
		outV0.end = end
		outV0.toggle = toggle
		outV0.payload = payload
		svc := (canID & (1 << 7)) != 0
		if svc {
			dst := uint8((canID >> 8) & NodeIDMax)
			outV0.dst = dst
			outV0.portID = uint16((canID >> 16) & 0xFF)
			req := (canID & (1 << 15)) != 0
			if req {
				outV0.kind = KindV0Request
			} else {
				outV0.kind = KindV0Response
			}
			isV0 = isV0 && (dst != 0) && (src != 0) && (src != dst)
		} else {
			outV0.dst = NodeIDAnonymous
			outV0.kind = KindV0Message
			if src == 0 {
				outV0.src = NodeIDAnonymous
				outV0.portID = uint16((canID >> 8) & 0x3)
				isV0 = isV0 && start && end
			} else {
				outV0.portID = uint16((canID >> 8) & 0xFFFF)
			}
		}
	}
	mask := byte(0)
	if isV0 {
		mask |= 1
	}
	if isV1 {
		mask |= 2
	}
	return mask, outV0, outV1
}

// rxSlot is the reassembly state for one priority level of one remote session.
type rxSlot struct {
	startTs        int64
	extent         uint64
	totalSize      uint32
	crc            uint16
	transferID     uint8
	ifaceIndex     uint8
	expectedToggle bool
	payload        []byte
}

func rxSlotNew(sub *Subscription, startTs int64, transferID uint8, ifaceIndex uint8) *rxSlot {
	extentFull := sub.Extent
	if kindVersion(sub.Kind) == 0 {
		extentFull += crcBytes
	}
	payload := make([]byte, extentFull)
	return &rxSlot{
		startTs:        startTs,
		extent:         uint64(extentFull),
		crc:            sub.CRCSeed,
		transferID:     transferID & transferIDMax,
		ifaceIndex:     ifaceIndex,
		expectedToggle: kindVersion(sub.Kind) != 0,
		payload:        payload,
	}
}

func rxSlotDestroy(sub *Subscription, slot *rxSlot) {
	// Memory is managed by the Go runtime; nothing to do beyond dropping the reference.
	_ = sub
	_ = slot
}

func rxSlotAdvance(slot *rxSlot, payload []byte) {
	if slot.totalSize < uint32(slot.extent) {
		copySize := smaller(len(payload), int(slot.extent)-int(slot.totalSize))
		copy(slot.payload[slot.totalSize:], payload[:copySize])
	}
	slot.totalSize = uint32(int(slot.totalSize) + len(payload))
	slot.expectedToggle = !slot.expectedToggle
}

// rxSession holds per-remote reassembly state for one subscription, indexed by remote node-ID.
type rxSession struct {
	index           cavlNode
	listAnimation   cavlListed
	lastAdmissionTs int64
	slots           [PrioCount]*rxSlot
	owner           *Subscription
	nodeID          uint8
	lastAdmittedTransferID uint8
	lastAdmittedPriority    uint8
	ifaceIndex       uint8
}

type rxSessionFactoryCtx struct {
	owner      *Subscription
	ifaceIndex uint8
	nodeID     uint8
}

func rxSessionCompare(key *cavlNode, node *cavlNode) int32 {
	k := key.owner.(uint8)
	n := node.owner.(*rxSession).nodeID
	return int32(k) - int32(n)
}

func rxSessionFactory(ctx *rxSessionFactoryCtx) *cavlNode {
	ses := &rxSession{
		owner:          ctx.owner,
		ifaceIndex:     ctx.ifaceIndex,
		nodeID:         ctx.nodeID,
		lastAdmissionTs: bigBang,
	}
	ses.index.owner = ses
	for i := 0; i < PrioCount; i++ {
		ses.slots[i] = nil
	}
	enlistTail(&ctx.owner.Owner.rx.listSessionByAnimation, &ses.listAnimation)
	return &ses.index
}

func rxSessionDestroy(ses *rxSession) {
	sub := ses.owner
	for i := 0; i < PrioCount; i++ {
		rxSlotDestroy(sub, ses.slots[i])
	}
	cavlRemove(&sub.Sessions, &ses.index)
	delist(&sub.Owner.rx.listSessionByAnimation, &ses.listAnimation)
}

func rxSessionCleanup(ses *rxSession, now int64) int {
	deadline := now - later(rxSessionTimeout, ses.owner.TransferIDTimeout)
	n := 0
	for i := 0; i < PrioCount; i++ {
		slot := ses.slots[i]
		if slot == nil {
			continue
		}
		if slot.startTs < deadline {
			rxSlotDestroy(ses.owner, ses.slots[i])
			ses.slots[i] = nil
		} else {
			n++
		}
	}
	return n
}

func bytesFromSlice(b []byte) Bytes {
	if len(b) == 0 {
		return Bytes{Size: 0, Data: nil}
	}
	return Bytes{Size: len(b), Data: unsafe.Pointer(&b[0])}
}

func rxSessionCompleteSingleFrame(sub *Subscription, ts int64, fr *rxFrameParsed) {
	payload := Payload{
		View:   bytesFromSlice(fr.payload),
		Origin: BytesMut{Size: 0, Data: nil},
	}
	sub.VTable.OnMessage(sub, ts, fr.priority, fr.src, fr.transferID, payload)
}

func rxSessionCompleteSlot(ses *rxSession, fr *rxFrameParsed) {
	sub := ses.owner
	slot := ses.slots[fr.priority]
	ses.slots[fr.priority] = nil
	v1 := kindVersion(sub.Kind) == 1
	var crcRef uint16
	if v1 {
		crcRef = crcResidue
	} else {
		crcRef = uint16(slot.payload[0]) | (uint16(slot.payload[1]) << 8)
	}
	if slot.crc == crcRef {
		var size int
		if v1 {
			size = smaller(int(slot.totalSize)-crcBytes, int(slot.extent))
		} else {
			size = smaller(int(slot.totalSize)-crcBytes, int(slot.extent)-crcBytes)
		}
		var viewBytes []byte
		if v1 {
			viewBytes = slot.payload[:size]
		} else {
			viewBytes = slot.payload[crcBytes : crcBytes+size]
		}
		payload := Payload{
			View:   bytesFromSlice(viewBytes),
			Origin: BytesMut{Size: int(slot.extent), Data: unsafe.Pointer(&slot.payload[0])},
		}
		sub.VTable.OnMessage(sub, slot.startTs, fr.priority, fr.src, fr.transferID, payload)
	} else {
		sub.Owner.Err.RXTransfer++
		rxSlotDestroy(ses.owner, slot)
	}
}

func rxSessionAccept(ses *rxSession, ts int64, fr *rxFrameParsed) {
	sub := ses.owner
	slot := ses.slots[fr.priority]
	if slot != nil {
		rxSlotAdvance(slot, fr.payload)
		var crcInput []byte
		if kindVersion(sub.Kind) == 0 && fr.start {
			crcInput = fr.payload[crcBytes:]
		} else {
			crcInput = fr.payload
		}
		slot.crc = crcAdd(slot.crc, len(crcInput), crcInput)
		if fr.end {
			rxSessionCompleteSlot(ses, fr)
		}
	} else {
		rxSessionCompleteSingleFrame(sub, ts, fr)
	}
}

func rxSessionRecordAdmission(ses *rxSession, priority Prio, transferID uint8, ts int64, ifaceIndex uint8) {
	ses.lastAdmissionTs = ts
	ses.lastAdmittedTransferID = transferID & transferIDMax
	ses.lastAdmittedPriority = uint8(priority) & ((1 << PrioBits) - 1)
	ses.ifaceIndex = ifaceIndex
}

func rxSessionSolveAdmission(ses *rxSession, ts int64, priority Prio, start bool, toggle bool, transferID uint8, ifaceIndex uint8) bool {
	if !start {
		slot := ses.slots[priority]
		return slot != nil && slot.transferID == transferID && slot.ifaceIndex == ifaceIndex && slot.expectedToggle == toggle
	}
	fresh := (transferID != ses.lastAdmittedTransferID) || (uint8(priority) != ses.lastAdmittedPriority)
	affine := ses.ifaceIndex == ifaceIndex
	stale := (ts - ses.owner.TransferIDTimeout) > ses.lastAdmissionTs
	return (fresh && affine) || (affine && stale) || (stale && fresh)
}

func rxSessionUpdate(sub *Subscription, ts int64, frame *rxFrameParsed, ifaceIndex uint8) {
	if frame.src == NodeIDAnonymous {
		rxSessionCompleteSingleFrame(sub, ts, frame)
		return
	}
	var factoryCtx rxSessionFactoryCtx
	factoryCtx.owner = sub
	factoryCtx.ifaceIndex = ifaceIndex
	factoryCtx.nodeID = frame.src
	var searchKey cavlNode
	searchKey.owner = frame.src
	var ses *rxSession
	if frame.start {
		node := cavlFindOrInsert(&sub.Sessions, &searchKey, rxSessionCompare, func() *cavlNode {
			return rxSessionFactory(&factoryCtx)
		})
		ses = node.owner.(*rxSession)
	} else {
		node := cavlFind(&sub.Sessions, &searchKey, rxSessionCompare)
		if node == nil {
			return
		}
		ses = node.owner.(*rxSession)
	}
	if ses == nil {
		if frame.start {
			sub.Owner.Err.OOM++
		}
		return
	}
	admit := rxSessionSolveAdmission(ses, ts, frame.priority, frame.start, frame.toggle, frame.transferID, ifaceIndex)
	if !admit {
		return
	}
	if frame.start {
		enlistTail(&sub.Owner.rx.listSessionByAnimation, &ses.listAnimation)
		if ses.slots[frame.priority] != nil {
			rxSlotDestroy(sub, ses.slots[frame.priority])
			ses.slots[frame.priority] = nil
		}
		if !frame.end {
			rxSessionCleanup(ses, ts)
			ses.slots[frame.priority] = rxSlotNew(sub, ts, frame.transferID, ifaceIndex)
			if ses.slots[frame.priority] == nil {
				sub.Owner.Err.OOM++
				return
			}
		}
		rxSessionRecordAdmission(ses, frame.priority, frame.transferID, ts, ifaceIndex)
	}
	rxSessionAccept(ses, ts, frame)
}

func rxSubscriptionCompare(key *cavlNode, node *cavlNode) int32 {
	k := key.owner.(uint16)
	n := node.owner.(*Subscription).PortID
	return int32(k) - int32(n)
}

func rxFindSubscription(self *Canard, kind Kind, portID uint16) *Subscription {
	node := cavlFind(&self.rx.subscriptions[kind], &cavlNode{owner: portID}, rxSubscriptionCompare)
	if node == nil {
		return nil
	}
	return node.owner.(*Subscription)
}

func rxRoute(self *Canard, fr *rxFrameParsed) *Subscription {
	if (fr.dst != NodeIDAnonymous) && (fr.dst != self.NodeID) {
		return nil
	}
	return rxFindSubscription(self, fr.kind, fr.portID)
}

func rxFilterForSubscription(self *Canard, kind Kind, portID uint16) Filter {
	f := Filter{}
	id := self.NodeID & NodeIDMax
	switch kind {
	case KindMessage16b:
		f.ExtendedCANID = (uint32(portID) << 8) | (1 << 7)
		f.ExtendedMask = 0x03FFFF80
	case KindMessage13b:
		f.ExtendedCANID = uint32(portID) << 8
		f.ExtendedMask = 0x029FFF80
	case KindResponse, KindRequest:
		rnr := uint32(0)
		if kind == KindRequest {
			rnr = 1 << 24
		}
		f.ExtendedCANID = (1 << 25) | rnr | (uint32(portID) << 14) | (uint32(id) << 7)
		f.ExtendedMask = 0x03FFFF80
	case KindV0Message:
		f.ExtendedCANID = uint32(portID) << 8
		if portID <= 3 {
			f.ExtendedMask = 0x0000FF80
		} else {
			f.ExtendedMask = 0x00FFFF80
		}
	case KindV0Response, KindV0Request:
		rnr := uint32(0)
		if kind == KindV0Request {
			rnr = 1 << 15
		}
		f.ExtendedCANID = (uint32(portID&0xFF) << 16) | rnr | (uint32(id) << 8) | (1 << 7)
		f.ExtendedMask = 0x00FFFF80
	}
	return f
}

func rxFilterFuse(a, b Filter) Filter {
	mask := a.ExtendedMask & b.ExtendedMask & ^(a.ExtendedCANID ^ b.ExtendedCANID)
	return Filter{ExtendedCANID: a.ExtendedCANID & mask, ExtendedMask: mask}
}

func rxFilterRank(a Filter) uint8 {
	return popcount(uint64(a.ExtendedMask))
}

func rxFilterCovered(count int, filters []Filter, inner Filter) bool {
	for i := 0; i < count; i++ {
		if ((filters[i].ExtendedMask & ^inner.ExtendedMask) == 0) &&
			((inner.ExtendedCANID & filters[i].ExtendedMask) == filters[i].ExtendedCANID) {
			return true
		}
	}
	return false
}

func rxFilterCoalesceInto(count int, into []Filter, newF Filter) {
	initialized := false
	bestI := 0
	bestJ := count
	var bestRank uint8
	var bestFuse Filter
	for i := 0; i < count; i++ {
		for j := i + 1; j <= count; j++ {
			var f Filter
			if j < count {
				f = rxFilterFuse(into[i], into[j])
			} else {
				f = rxFilterFuse(into[i], newF)
			}
			r := rxFilterRank(f)
			if !initialized || r >= bestRank {
				initialized = true
				bestI = i
				bestJ = j
				bestRank = r
				bestFuse = f
			}
		}
	}
	into[bestI] = bestFuse
	if bestJ < count {
		into[bestJ] = newF
	}
}

func rxFilterAppend(into []Filter, n *int, capacity int, f Filter) {
	if *n < capacity {
		into[*n] = f
		*n++
	} else {
		rxFilterCoalesceInto(*n, into, f)
	}
}

func (self *Canard) rxFilterConfigure() bool {
	if self.rx.filterCount == 0 {
		return true
	}
	capacity := self.rx.filterCount
	filters := make([]Filter, capacity)
	n := 0
	for kind := Kind(0); kind < KindCount; kind++ {
		cavlIterate(self.rx.subscriptions[kind], func(node *cavlNode) {
			sub := node.owner.(*Subscription)
			rxFilterAppend(filters, &n, capacity, rxFilterForSubscription(self, kind, sub.PortID))
		})
	}
	forced := []struct {
		kind   Kind
		portID uint16
	}{
		{KindMessage13b, 7509}, // Cyphal v1.0 Heartbeat
		{KindV0Message, 341},   // DroneCAN NodeStatus
	}
	for i := 0; i < len(forced); i++ {
		g := rxFilterForSubscription(self, forced[i].kind, forced[i].portID)
		if !rxFilterCovered(n, filters, g) {
			rxFilterAppend(filters, &n, capacity, g)
		}
	}
	ok := self.VTable.Filter(self, n, filters[:n])
	return ok
}

func rxSubscribe(self *Canard, sub *Subscription, kind Kind, portID uint16, crcSeed uint16, extent int, transferIDTimeout int64, vtable *SubscriptionVTable) *Subscription {
	var out *Subscription
	if self != nil && sub != nil && vtable != nil && vtable.OnMessage != nil && transferIDTimeout >= 0 {
		sub.TransferIDTimeout = transferIDTimeout
		sub.Extent = extent
		sub.PortID = portID
		sub.CRCSeed = crcSeed
		sub.Kind = kind
		sub.Owner = self
		sub.Sessions = nil
		sub.VTable = vtable
		sub.UserContext = nil
		var searchKey cavlNode
		searchKey.owner = portID
		node := cavlFindOrInsert(&self.rx.subscriptions[kind], &searchKey, rxSubscriptionCompare, func() *cavlNode {
			sub.indexPortID.owner = sub
			return &sub.indexPortID
		})
		out = node.owner.(*Subscription)
		self.rx.filtersDirty = self.rx.filtersDirty || (node == &sub.indexPortID)
	}
	return out
}

func (self *Canard) Unsubscribe(sub *Subscription) {
	for sub.Sessions != nil {
		node := cavlMin(sub.Sessions)
		rxSessionDestroy(node.owner.(*rxSession))
	}
	cavlRemove(&self.rx.subscriptions[sub.Kind], &sub.indexPortID)
	self.rx.filtersDirty = true
}
