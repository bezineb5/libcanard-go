package libcanard

import "unsafe"

func bytesChainValid(b []byte) bool {
	return len(b) == 0 || unsafe.SliceData(b) != nil
}

func boolToUint32(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

// New allocates and initializes a new Canard instance. It returns (instance, true) on success.
func New(platform Platform, memory MemSet, ifaceBitmap uint8, txQueueCapacity int, prngSeed uint64, filterCount int) (*Canard, bool) {
	self := &Canard{}
	ok := self.Init(platform, memory, ifaceBitmap, txQueueCapacity, prngSeed, filterCount)
	if !ok {
		return nil, false
	}
	return self, true
}

// Init initializes the instance in place (equivalent to canard_new). It returns true on success, false if any of the
// parameters are invalid.
func (self *Canard) Init(platform Platform, memory MemSet, ifaceBitmap uint8, txQueueCapacity int, prngSeed uint64, filterCount int) bool {
	filterOK := (filterCount == 0) || memValid(memory.RXFilters)
	ifaceOK := (ifaceBitmap & IfaceBitmapAll) == ifaceBitmap
	ok := self != nil && platform != nil &&
		memValid(memory.TXTransfer) && memValid(memory.TXFrame) && memValid(memory.RXSession) && memValid(memory.RXPayload) &&
		filterOK && ifaceOK
	if ok {
		*self = Canard{}
		self.tx.FD = true
		self.tx.queueCapacity = txQueueCapacity
		self.IfaceBitmap = ifaceBitmap
		self.rx.filterCount = filterCount
		self.rx.filtersDirty = filterCount > 0
		self.Mem = memory
		self.PRNGState = prngSeed ^ uint64(uintptr(unsafe.Pointer(self)))
		self.Platform = platform
		self.NodeID = uint8(random(self, NodeIDMax) + 1) // [1, 127]
		nodeIDOccupancyReset(self)
	}
	return ok
}

// Destroy purges the TX queue. The application MUST destroy all subscriptions before invoking this.
func (self *Canard) Destroy() {
	for self.tx.agewise.head != nil {
		tr := listHead[txTransfer](&self.tx.agewise)
		self.txRetire(tr)
	}
	*self = Canard{}
}

// SetNodeID manually assigns the desired node-ID. Returns false if the argument is invalid.
func (self *Canard) SetNodeID(nodeID uint8) bool {
	ok := self != nil && nodeID <= NodeIDMax
	if ok && nodeID != self.NodeID {
		self.NodeID = nodeID
		self.txPurgeContinuations()
		nodeIDOccupancyReset(self)
		self.rx.filtersDirty = true
	}
	return ok
}

// SetClassicCAN enables or disables Classic CAN (vs CAN FD) framing for this instance.
//
// By default a Canard instance uses CAN FD (tx.FD = true), which permits up to 64-byte frames.
// Many embedded CAN controllers (e.g. the MCP2515 behind an I2C-CAN bridge) only support
// Classic CAN with an 8-byte MTU. Call SetClassicCAN(true) before publishing to force every
// subsequent transfer to use the Classic CAN MTU, matching such controllers. This is purely a
// per-instance MTU switch and produces wire frames identical to OpenCyphal/libcanard running in
// Classic CAN mode, so it is compatible with any compliant Cyphal/CAN v1.x receiver.
//
// It affects Publish16b, Publish13b, and the service publish paths (all of which consult tx.FD);
// V0Publish already uses Classic CAN unconditionally. Safe to call at any time, but avoid changing
// it while transfers are pending, as in-flight multi-frame transfers would mix MTUs.
func (self *Canard) SetClassicCAN(enabled bool) {
	if self == nil {
		return
	}
	self.tx.FD = !enabled
}

// Poll drives the TX pipeline and reconfigures RX filters if dirty. It must be called periodically and whenever any
// interface with pending transmissions becomes writable.
func (self *Canard) Poll(txReadyIfaceBitmap uint8) {
	if self == nil {
		return
	}
	self.rx.filtersDirty = self.rx.filtersDirty && !self.rxFilterConfigure()
	ses := listHead[rxSession](&self.rx.listSessionByAnimation)
	if ses != nil {
		now := self.Platform.Now(self)
		inProgress := rxSessionCleanup(ses, now)
		if inProgress == 0 && ses.lastAdmissionTs < (now-ses.owner.TransferIDTimeout) {
			rxSessionDestroy(ses)
		}
	}
	self.txExpire(self.Platform.Now(self))
	for i := range IfaceCount {
		if (txReadyIfaceBitmap & (1 << uint(i))) != 0 {
			self.txEjectPending(uint8(i))
		}
	}
}

// IngestFrame processes a received CAN frame. Returns true if the arguments were valid.
func (self *Canard) IngestFrame(timestamp int64, ifaceIndex uint8, extendedCANID uint32, canData []byte) bool {
	ok := self != nil && timestamp >= 0 && ifaceIndex < IfaceCount && extendedCANID <= canExtIDMask &&
		(len(canData) == 0 || unsafe.SliceData(canData) != nil)
	if ok {
		mask, v0, v1 := rxParse(extendedCANID, canData)
		if mask == 0 {
			self.Err.RXFrame++
		}
		if mask&1 != 0 {
			ingestFrame(self, timestamp, ifaceIndex, v0)
		}
		if mask&2 != 0 {
			ingestFrame(self, timestamp, ifaceIndex, v1)
		}
	}
	return ok
}

func ingestFrame(self *Canard, timestamp int64, ifaceIndex uint8, frame *rxFrameParsed) {
	if frame.start {
		nodeIDOccupancyUpdate(self, frame.src)
	}
	sub := rxRoute(self, frame)
	if sub != nil {
		rxSessionUpdate(sub, timestamp, frame, ifaceIndex)
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// TX: publish / request / respond.
// ---------------------------------------------------------------------------------------------------------------------

// Publish16b enqueues a 16-bit subject-ID message transfer.
func (self *Canard) Publish16b(deadline int64, ifaceBitmap uint8, priority Prio, subjectID uint16, transferID uint8, payload []byte, userContext any) bool {
	ok := self != nil && int(priority) < PrioCount && bytesChainValid(payload) &&
		((ifaceBitmap & IfaceBitmapAll) != 0) && ((ifaceBitmap & IfaceBitmapAll) == ifaceBitmap)
	if ok {
		canID := (uint32(priority) << prioShift) | (uint32(subjectID) << 8) | (1 << 7)
		tr := txTransferNew(self, deadline, canID, self.tx.FD, userContext)
		ok = tr != nil && self.txPush(tr, false, ifaceBitmap, transferID, payload, crcInitial)
	}
	return ok
}

// Publish13b enqueues a 13-bit subject-ID message transfer.
func (self *Canard) Publish13b(deadline int64, ifaceBitmap uint8, priority Prio, subjectID uint16, transferID uint8, payload []byte, userContext any) bool {
	ok := self != nil && int(priority) < PrioCount && bytesChainValid(payload) &&
		((ifaceBitmap & IfaceBitmapAll) != 0) && ((ifaceBitmap & IfaceBitmapAll) == ifaceBitmap) &&
		(subjectID <= SubjectIDMax13b)
	if ok {
		canID := (uint32(priority) << prioShift) | (3 << 21) | (uint32(subjectID) << 8)
		tr := txTransferNew(self, deadline, canID, self.tx.FD, userContext)
		ok = tr != nil && self.txPush(tr, false, ifaceBitmap, transferID, payload, crcInitial)
	}
	return ok
}

func (self *Canard) tx1v0Service(deadline int64, priority Prio, serviceID uint16, destinationNodeID uint8, requestNotResponse bool, transferID uint8, payload []byte, userContext any) bool {
	ok := self != nil && int(priority) < PrioCount && bytesChainValid(payload) &&
		(serviceID <= ServiceIDMax) && (destinationNodeID <= NodeIDMax)
	if ok {
		canID := (uint32(priority) << prioShift) | (1 << 25) |
			(boolToUint32(requestNotResponse) << 24) | (uint32(serviceID) << 14) | (uint32(destinationNodeID) << 7)
		tr := txTransferNew(self, deadline, canID, self.tx.FD, userContext)
		ok = tr != nil && self.txPush(tr, false, IfaceBitmapAll, transferID, payload, crcInitial)
	}
	return ok
}

// Request enqueues a service request on all interfaces.
func (self *Canard) Request(deadline int64, priority Prio, serviceID uint16, serverNodeID uint8, transferID uint8, payload []byte, userContext any) bool {
	return self.tx1v0Service(deadline, priority, serviceID, serverNodeID, true, transferID, payload, userContext)
}

// Respond enqueues a service response on all interfaces.
func (self *Canard) Respond(deadline int64, priority Prio, serviceID uint16, clientNodeID uint8, transferID uint8, payload []byte, userContext any) bool {
	return self.tx1v0Service(deadline, priority, serviceID, clientNodeID, false, transferID, payload, userContext)
}

// V0Publish enqueues a legacy v0 message transfer (always Classic CAN).
func (self *Canard) V0Publish(deadline int64, ifaceBitmap uint8, priority Prio, dataTypeID uint16, crcSeed uint16, transferID uint8, payload []byte, userContext any) bool {
	ok := self != nil && int(priority) < PrioCount && bytesChainValid(payload) &&
		((ifaceBitmap & IfaceBitmapAll) != 0) && ((ifaceBitmap & IfaceBitmapAll) == ifaceBitmap) && (self.NodeID != 0)
	if ok {
		canID := (uint32(priority) << prioShift) | (uint32(dataTypeID) << 8)
		tr := txTransferNew(self, deadline, canID, false, userContext)
		ok = tr != nil && self.txPush(tr, true, ifaceBitmap, transferID, payload, crcSeed)
	}
	return ok
}

func (self *Canard) txV0Service(deadline int64, priority Prio, dataTypeID uint8, crcSeed uint16, destinationNodeID uint8, requestNotResponse bool, transferID uint8, payload []byte, userContext any) bool {
	ok := self != nil && int(priority) < PrioCount && bytesChainValid(payload) && (self.NodeID != 0) &&
		(destinationNodeID > 0) && (destinationNodeID <= NodeIDMax)
	if ok {
		canID := (uint32(priority) << prioShift) | (uint32(dataTypeID) << 16) |
			(boolToUint32(requestNotResponse) << 15) | (uint32(destinationNodeID) << 8) | (1 << 7)
		tr := txTransferNew(self, deadline, canID, false, userContext)
		ok = tr != nil && self.txPush(tr, true, IfaceBitmapAll, transferID, payload, crcSeed)
	}
	return ok
}

// V0Request enqueues a legacy v0 service request on all interfaces.
func (self *Canard) V0Request(deadline int64, priority Prio, dataTypeID uint8, crcSeed uint16, serverNodeID uint8, transferID uint8, payload []byte, userContext any) bool {
	return self.txV0Service(deadline, priority, dataTypeID, crcSeed, serverNodeID, true, transferID, payload, userContext)
}

// V0Respond enqueues a legacy v0 service response on all interfaces.
func (self *Canard) V0Respond(deadline int64, priority Prio, dataTypeID uint8, crcSeed uint16, clientNodeID uint8, transferID uint8, payload []byte, userContext any) bool {
	return self.txV0Service(deadline, priority, dataTypeID, crcSeed, clientNodeID, false, transferID, payload, userContext)
}

// ---------------------------------------------------------------------------------------------------------------------
// RX: subscribe / find / unsubscribe.
// ---------------------------------------------------------------------------------------------------------------------

// Subscribe16b registers a subscription for a 16-bit subject-ID message.
func (self *Canard) Subscribe16b(sub *Subscription, subjectID uint16, extent int, transferIDTimeout int64, vtable *SubscriptionVTable) *Subscription {
	return rxSubscribe(self, sub, KindMessage16b, subjectID, crcInitial, extent, transferIDTimeout, vtable)
}

// Subscribe13b registers a subscription for a 13-bit subject-ID message.
func (self *Canard) Subscribe13b(sub *Subscription, subjectID uint16, extent int, transferIDTimeout int64, vtable *SubscriptionVTable) *Subscription {
	if subjectID <= SubjectIDMax13b {
		return rxSubscribe(self, sub, KindMessage13b, subjectID, crcInitial, extent, transferIDTimeout, vtable)
	}
	return nil
}

// SubscribeRequest registers a subscription for service requests.
func (self *Canard) SubscribeRequest(sub *Subscription, serviceID uint16, extent int, transferIDTimeout int64, vtable *SubscriptionVTable) *Subscription {
	if serviceID <= ServiceIDMax {
		return rxSubscribe(self, sub, KindRequest, serviceID, crcInitial, extent, transferIDTimeout, vtable)
	}
	return nil
}

// SubscribeResponse registers a subscription for service responses (zero transfer-ID timeout).
func (self *Canard) SubscribeResponse(sub *Subscription, serviceID uint16, extent int, vtable *SubscriptionVTable) *Subscription {
	if serviceID <= ServiceIDMax {
		return rxSubscribe(self, sub, KindResponse, serviceID, crcInitial, extent, 0, vtable)
	}
	return nil
}

// FindSubscription returns the installed subscription for the given kind and port-ID, or nil.
func (self *Canard) FindSubscription(kind Kind, portID uint16) *Subscription {
	if self == nil || int(kind) >= KindCount {
		return nil
	}
	return rxFindSubscription(self, kind, portID)
}

// V0Subscribe registers a legacy v0 message subscription.
func (self *Canard) V0Subscribe(sub *Subscription, dataTypeID uint16, crcSeed uint16, extent int, transferIDTimeout int64, vtable *SubscriptionVTable) *Subscription {
	return rxSubscribe(self, sub, KindV0Message, dataTypeID, crcSeed, extent, transferIDTimeout, vtable)
}

// V0SubscribeRequest registers a legacy v0 service request subscription.
func (self *Canard) V0SubscribeRequest(sub *Subscription, dataTypeID uint8, crcSeed uint16, extent int, transferIDTimeout int64, vtable *SubscriptionVTable) *Subscription {
	return rxSubscribe(self, sub, KindV0Request, uint16(dataTypeID), crcSeed, extent, transferIDTimeout, vtable)
}

// V0SubscribeResponse registers a legacy v0 service response subscription (zero transfer-ID timeout).
func (self *Canard) V0SubscribeResponse(sub *Subscription, dataTypeID uint8, crcSeed uint16, extent int, vtable *SubscriptionVTable) *Subscription {
	return rxSubscribe(self, sub, KindV0Response, uint16(dataTypeID), crcSeed, extent, 0, vtable)
}

// ---------------------------------------------------------------------------------------------------------------------
// Node-ID occupancy / collision handling.
// ---------------------------------------------------------------------------------------------------------------------

func nodeIDOccupancyReset(self *Canard) {
	self.NodeIDOccupancyBitmap[0] = 1 // Reserve 0 for compatibility with v0.
	self.NodeIDOccupancyBitmap[1] = 0
}

func nodeIDOccupancyUpdate(self *Canard, src uint8) {
	if (src == NodeIDAnonymous) ||
		(bitmapTest(&self.NodeIDOccupancyBitmap, int(src)) && (self.NodeID != src)) {
		return
	}
	bitmapSet(&self.NodeIDOccupancyBitmap, int(src))
	cap := uint8(NodeIDCapacity)
	pc := popcount(self.NodeIDOccupancyBitmap[0]) + popcount(self.NodeIDOccupancyBitmap[1])
	zc := cap - pc
	purge := (pc > cap/2) && chance(self, uint64(zc))
	if self.NodeID == src {
		// Uniformly pick a cleared bit from the bitmap as the new node-ID.
		z := random(self, uint64(zc))
		id := 0
		for z > 0 {
			if !bitmapTest(&self.NodeIDOccupancyBitmap, id) {
				z--
			}
			id++
		}
		for bitmapTest(&self.NodeIDOccupancyBitmap, id) {
			id++
		}
		self.NodeID = uint8(id)
		self.txPurgeContinuations()
		self.rx.filtersDirty = true
		self.Err.Collision++
	}
	if purge {
		nodeIDOccupancyReset(self)
		bitmapSet(&self.NodeIDOccupancyBitmap, int(src))
	}
}
