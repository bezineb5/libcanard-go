package libcanard

import "unsafe"

// frameRegistry maps the first byte of a frame's data buffer back to its txFrame, so that reference counting can be
// performed from the byte slice handed to the TX vtable callback.
var frameRegistry = map[unsafe.Pointer]*txFrame{}

// txFrame is a single CAN frame in the TX spool. Frames are chained to form a multi-frame transfer.
type txFrame struct {
	size    int
	next     *txFrame
	refcount uint32
	dlc      uint8
	data     []byte
}

func txFrameView(f *txFrame) []byte {
	if f == nil {
		return nil
	}
	return f.data[:f.size]
}

func txFrameFromView(b []byte) *txFrame {
	if len(b) == 0 {
		return nil
	}
	return frameRegistry[unsafe.Pointer(&b[0])]
}

func txFrameNew(self *Canard, dataSize int) *txFrame {
	if dataSize > MTUFD {
		return nil
	}
	data := make([]byte, dataSize)
	f := &txFrame{
		refcount: 1,
		size:    dataSize,
		dlc:      LenToDlc[dataSize],
		data:     data,
	}
	frameRegistry[unsafe.Pointer(&data[0])] = f
	self.tx.queueSize++
	return f
}

// RefCountInc retains a TX frame view so it may outlive the callback and the TX queue entry.
func RefCountInc(view []byte) {
	if len(view) == 0 {
		return
	}
	if f := txFrameFromView(view); f != nil {
		f.refcount++
	}
}

// RefCountDec releases a TX frame retained earlier.
func (self *Canard) RefCountDec(view []byte) {
	self.refCountDec(view)
}

func (self *Canard) refCountDec(view []byte) {
	if len(view) == 0 {
		return
	}
	f := txFrameFromView(view)
	if f == nil {
		return
	}
	f.refcount--
	if f.refcount == 0 {
		self.tx.queueSize--
		delete(frameRegistry, unsafe.Pointer(&f.data[0]))
	}
}

func txMakeTailByte(sot, eot, tog bool, transferID uint8) uint8 {
	b := uint8(0)
	if sot {
		b |= tailSOT
	}
	if eot {
		b |= tailEOT
	}
	if tog {
		b |= tailToggle
	}
	b |= transferID & transferIDMax
	return b
}

// txCeilFramePayloadSize rounds a frame payload size up to the nearest valid DLC length.
func txCeilFramePayloadSize(x int) int {
	if x >= len(LenToDlc) {
		return int(DlcToLen[LenToDlc[len(LenToDlc)-1]])
	}
	return int(DlcToLen[LenToDlc[x]])
}

type payloadReader struct {
	data []byte
	pos  int
}

func (r *payloadReader) init(p []byte) { r.data = p; r.pos = 0 }

func (r *payloadReader) read(n int, dst []byte) {
	copy(dst, r.data[r.pos:r.pos+n])
	r.pos += n
}

// txSpool builds a chain of txFrame instances for a Cyphal/CAN transfer, or nil on OOM.
func txSpool(self *Canard, crcSeed uint16, mtu int, transferID uint8, size int, payload []byte) *txFrame {
	var reader payloadReader
	reader.init(payload)
	var head *txFrame
	toggle := true // Cyphal transfers start with toggle==1, unlike legacy.
	if size < mtu {
		var frameSize int
		if mtu == MTUClassic {
			frameSize = txCeilFramePayloadSize(size + 1)
		} else {
			frameSize = size + 1
		}
		head = txFrameNew(self, frameSize)
		if head != nil {
			reader.read(size, head.data)
			for i := size; i < frameSize-1; i++ {
				head.data[i] = paddingByteValue
			}
			head.data[frameSize-1] = txMakeTailByte(true, true, toggle, transferID)
		}
		return head
	}
	sizeWithCRC := size + crcBytes
	offset := 0
	crc := crcSeed
	var tail *txFrame
	for offset < sizeWithCRC {
		frameSizeWithTail := mtu
		if (sizeWithCRC - offset) < (mtu - 1) {
			if mtu == MTUClassic {
				frameSizeWithTail = txCeilFramePayloadSize((sizeWithCRC - offset) + 1)
			} else {
				frameSizeWithTail = (sizeWithCRC - offset) + 1
			}
		}
		item := txFrameNew(self, frameSizeWithTail)
		if head == nil {
			head = item
		} else {
			tail.next = item
		}
		tail = item
		if tail == nil {
			for head != nil {
				next := head.next
				self.refCountDec(txFrameView(head))
				head = next
			}
			return nil
		}
		frameSize := frameSizeWithTail - 1
		frameOffset := 0
		if offset < size {
			moveSize := smaller(size-offset, frameSize)
			reader.read(moveSize, tail.data[frameOffset:frameOffset+moveSize])
			crc = crcAdd(crc, moveSize, tail.data[frameOffset:frameOffset+moveSize])
			frameOffset += moveSize
			offset += moveSize
		}
		if offset >= size {
			for (frameOffset + crcBytes) < frameSize {
				tail.data[frameOffset] = paddingByteValue
				frameOffset++
				crc = crcAddByte(crc, paddingByteValue)
			}
			if (frameOffset < frameSize) && (offset == size) {
				tail.data[frameOffset] = byte((crc >> 8) & byteMax)
				frameOffset++
				offset++
			}
			if (frameOffset < frameSize) && (offset > size) {
				tail.data[frameOffset] = byte(crc & byteMax)
				frameOffset++
				offset++
			}
		}
		tail.data[frameOffset] = txMakeTailByte(head == tail, offset >= sizeWithCRC, toggle, transferID)
		toggle = !toggle
	}
	return head
}

// txSpoolV0 is the legacy counterpart for UAVCAN v0 transfers. Always uses Classic CAN MTU.
func txSpoolV0(self *Canard, crcSeed uint16, transferID uint8, size int, payload []byte) *txFrame {
	toggle := false // In v0 the toggle starts with zero; that is how v0/v1 can be distinguished.
	if size < MTUClassic {
		item := txFrameNew(self, size+1)
		if item != nil {
			var reader payloadReader
			reader.init(payload)
			reader.read(size, item.data)
			item.data[size] = txMakeTailByte(true, true, toggle, transferID)
		}
		return item
	}
	crc := crcAddChain(crcSeed, payload)
	crcBytesArr := []byte{byte(crc & 0xFF), byte((crc >> 8) & 0xFF)} // v0 CRC is little-endian.
	sizeTotal := size + crcBytes
	full := append(append([]byte{}, crcBytesArr...), payload...)
	var reader payloadReader
	reader.init(full)
	var head *txFrame
	var tail *txFrame
	offset := 0
	for offset < sizeTotal {
		frameDataSize := smaller(sizeTotal-offset+1, MTUClassic)
		item := txFrameNew(self, frameDataSize)
		if item == nil {
			for head != nil {
				next := head.next
				self.refCountDec(txFrameView(head))
				head = next
			}
			return nil
		}
		if head == nil {
			head = item
		} else {
			tail.next = item
		}
		tail = item
		progress := smaller(sizeTotal-offset, int(DlcToLen[item.dlc])-1)
		reader.read(progress, tail.data[:progress])
		offset += progress
		tail.data[progress] = txMakeTailByte(head == tail, offset == sizeTotal, toggle, transferID)
		toggle = !toggle
	}
	return head
}

func crcAddChain(crc uint16, payload []byte) uint16 {
	return crcAdd(crc, len(payload), payload)
}

// txTransfer is the reassembly/transmission descriptor for a whole transfer (possibly multi-frame).
type txTransfer struct {
	indexPending    [IfaceCount]cavlNode
	indexDeadline   cavlNode
	listAgewise     cavlListed
	userContext     any
	deadline        int64
	seqno           uint64
	canIDMSB        uint32
	fd              bool
	multiFrame      bool
	firstFrameDeparted bool
	cursor          [IfaceCount]*txFrame
}

func txTransferNew(self *Canard, deadline int64, canIDTemplate uint32, fd bool, userContext any) *txTransfer {
	tr := &txTransfer{
		userContext: userContext,
		deadline:    deadline,
		seqno:       self.tx.seqno,
		canIDMSB:    (canIDTemplate >> 7) & ((uint32(1) << canIDMSbBits) - 1),
		fd:          fd,
	}
	self.tx.seqno++
	for i := 0; i < IfaceCount; i++ {
		tr.indexPending[i].owner = tr
	}
	tr.indexDeadline.owner = tr
	tr.listAgewise.owner = tr
	return tr
}

func txComparePendingOrder(a, b *cavlNode) int32 {
	lhs := a.owner.(*txTransfer)
	rhs := b.owner.(*txTransfer)
	if lhs.canIDMSB < rhs.canIDMSB {
		return -1
	}
	if lhs.canIDMSB > rhs.canIDMSB {
		return 1
	}
	if lhs.seqno < rhs.seqno {
		return -1
	}
	return 1
}

func txCompareDeadline(a, b *cavlNode) int32 {
	lhs := a.owner.(*txTransfer)
	rhs := b.owner.(*txTransfer)
	if lhs.deadline < rhs.deadline {
		return -1
	}
	if lhs.deadline > rhs.deadline {
		return 1
	}
	if lhs.seqno < rhs.seqno {
		return -1
	}
	return 1
}

func txIsPending(self *Canard, tr *txTransfer) bool {
	for i := 0; i < IfaceCount; i++ {
		if cavlIsInserted(&tr.indexPending[i]) {
			return true
		}
	}
	return false
}

func (self *Canard) txFreePayload(tr *txTransfer) {
	for i := 0; i < IfaceCount; i++ {
		frame := tr.cursor[i]
		for frame != nil {
			next := frame.next
			self.refCountDec(txFrameView(frame))
			frame = next
		}
		tr.cursor[i] = nil
	}
}

func (self *Canard) txRetire(tr *txTransfer) {
	for i := 0; i < IfaceCount; i++ {
		cavlRemoveIf(&self.tx.pending[i], &tr.indexPending[i])
	}
	cavlRemove(&self.tx.deadline, &tr.indexDeadline)
	delist(&self.tx.agewise, &tr.listAgewise)
	self.txFreePayload(tr)
}

func (self *Canard) txMakePending(tr *txTransfer) {
	for i := 0; i < IfaceCount; i++ {
		if (tr.cursor[i] != nil) && !cavlIsInserted(&tr.indexPending[i]) {
			cavlFindOrInsert(&self.tx.pending[i], &tr.indexPending[i], txComparePendingOrder,
				func() *cavlNode { return &tr.indexPending[i] })
		}
	}
}

func txPredictFrameCount(transferSize, mtu int) int {
	bytesPerFrame := mtu - 1
	if transferSize <= bytesPerFrame {
		return 1
	}
	return ((transferSize + crcBytes + bytesPerFrame) - 1) / bytesPerFrame
}

func (self *Canard) txEnsureQueueSpace(totalFramesNeeded int) bool {
	if totalFramesNeeded > self.tx.queueCapacity {
		return false
	}
	for totalFramesNeeded > (self.tx.queueCapacity - self.tx.queueSize) {
		tr := listHead[txTransfer](&self.tx.agewise)
		if tr == nil {
			break
		}
		self.txRetire(tr)
		self.Err.TXSacrifice++
	}
	return totalFramesNeeded <= (self.tx.queueCapacity - self.tx.queueSize)
}

func (self *Canard) txExpire(now int64) {
	node := cavlMin(self.tx.deadline)
	for node != nil && now > node.owner.(*txTransfer).deadline {
		tr := node.owner.(*txTransfer)
		next := cavlNextGreater(&tr.indexDeadline)
		self.txRetire(tr)
		self.Err.TXExpiration++
		if next == nil {
			break
		}
		node = next
	}
}

func (self *Canard) txPush(tr *txTransfer, v0 bool, ifaceBitmap uint8, transferID uint8, payload []byte, crcSeed uint16) bool {
	effective := ifaceBitmap & self.IfaceBitmap
	if effective == 0 {
		memFree(self.Mem.TXTransfer, 0, unsafe.Pointer(tr))
		return false
	}
	now := self.VTable.Now(self)
	self.txExpire(now)

	mtu := MTUFD
	if !tr.fd {
		mtu = MTUClassic
	}
	size := len(payload)
	nFrames := txPredictFrameCount(size, mtu)
	tr.multiFrame = nFrames > 1
	if !self.txEnsureQueueSpace(nFrames) {
		self.Err.TXCapacity++
		memFree(self.Mem.TXTransfer, 0, unsafe.Pointer(tr))
		return false
	}
	var spool *txFrame
	if v0 {
		spool = txSpoolV0(self, crcSeed, transferID, size, payload)
	} else {
		spool = txSpool(self, crcSeed, mtu, transferID, size, payload)
	}
	if spool == nil {
		self.Err.OOM++
		memFree(self.Mem.TXTransfer, 0, unsafe.Pointer(tr))
		return false
	}
	frameRefcountInc := popcount(uint64(effective)) - 1
	if frameRefcountInc > 0 {
		f := spool
		for f != nil {
			f.refcount += uint32(frameRefcountInc)
			f = f.next
		}
	}
	for i := 0; i < IfaceCount; i++ {
		if (effective & (1 << uint(i))) != 0 {
			tr.cursor[i] = spool
		}
	}
	cavlFindOrInsert(&self.tx.deadline, &tr.indexDeadline, txCompareDeadline,
		func() *cavlNode { return &tr.indexDeadline })
	enlistTail(&self.tx.agewise, &tr.listAgewise)
	self.txMakePending(tr)
	return true
}

func (self *Canard) txEjectPending(ifaceIndex uint8) {
	for {
		pending := cavlMin(self.tx.pending[ifaceIndex])
		if pending == nil {
			break
		}
		tr := pending.owner.(*txTransfer)
		frame := tr.cursor[ifaceIndex]
		frameNext := frame.next
		canID := (uint32(tr.canIDMSB) << 7) | uint32(self.NodeID)
		data := txFrameView(frame)
		if len(data) == 0 {
		}
		ejected := self.VTable.TX(self, tr.userContext, tr.deadline, ifaceIndex, tr.fd, canID, data)
		if !ejected {
			break
		}
		tr.firstFrameDeparted = true
		tr.cursor[ifaceIndex] = frameNext
		self.refCountDec(txFrameView(frame))
		if frameNext == nil {
			cavlRemoveIf(&self.tx.pending[ifaceIndex], &tr.indexPending[ifaceIndex])
			if !txIsPending(self, tr) {
				self.txRetire(tr)
			}
		}
	}
}

// txPurgeContinuations cancels all pending multi-frame transfers where the first frame has already departed via at
// least one of the interfaces, so that a node-ID change on collision detection does not produce frankentransfers.
func (self *Canard) txPurgeContinuations() {
	tr := listHead[txTransfer](&self.tx.agewise)
	for tr != nil {
		next := listNext[txTransfer](&tr.listAgewise)
		if tr.multiFrame && tr.firstFrameDeparted {
			self.txRetire(tr)
		}
		tr = next
	}
}

// PendingIfaces returns a bitmap of interfaces that have pending transmissions.
func (self *Canard) PendingIfaces() uint8 {
	var out uint8
	for i := 0; i < IfaceCount; i++ {
		if self.tx.pending[i] != nil {
			out |= uint8(1) << uint(i)
		}
	}
	return out
}
