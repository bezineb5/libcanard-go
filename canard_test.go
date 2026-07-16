package libcanard

import (
	"testing"
	"unsafe"
)

func unsafeSlice(b Bytes) []byte {
	if b.Size == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(b.Data), b.Size)
}

func checkAVL(t *testing.T, root *cavlNode) {
	var prev int
	inOrder := 0
	cavlIterate(root, func(n *cavlNode) {
		inOrder++
		v := n.owner.(int)
		if inOrder > 1 && v <= prev {
			t.Fatalf("BST order violated: %d after %d (inOrder=%d)", v, prev, inOrder)
		}
		prev = v
		h0 := cavlHeight(n.lr[0])
		h1 := cavlHeight(n.lr[1])
		bf := h1 - h0
		if bf < -1 || bf > 1 {
			t.Fatalf("AVL balance violated at %d: bf=%d, left height=%d, right height=%d", v, bf, h0, h1)
		}
	})
}

func TestAVLInsert(t *testing.T) {
	owners := map[int]*cavlNode{}
	var root *cavlNode
	for i := range 1000 {
		node := &cavlNode{owner: i}
		owners[i] = node
		cavlFindOrInsert(&root, &cavlNode{owner: i}, func(a, b *cavlNode) int32 {
			return int32(a.owner.(int) - b.owner.(int))
		}, func() *cavlNode { return owners[i] })
		checkAVL(t, root)
	}
	count := 0
	cavlIterate(root, func(n *cavlNode) { count++ })
	if count != 1000 {
		t.Fatalf("expected 1000 nodes, got %d", count)
	}
}

type recorderTX struct {
	frames []recordedFrame
}

type recordedFrame struct {
	id   uint32
	data []byte
}

func makeInstance(nodeID uint8) *Canard {
	now := int64(0)
	c := &Canard{}
	ok := c.Init(&VTable{
		Now: func(self *Canard) int64 { return now },
		TX: func(self *Canard, ctx any, deadline int64, iface uint8, fd bool, id uint32, data []byte) bool {
			r := ctx.(*recorderTX)
			buf := make([]byte, len(data))
			copy(buf, data)
			r.frames = append(r.frames, recordedFrame{id: id, data: buf})
			now += 1
			return true
		},
		Filter: func(self *Canard, count int, filters []Filter) bool { return true },
	}, NewDefaultMemSet(), IfaceBitmapAll, 1000, 12345, 0)
	if !ok {
		panic("init failed")
	}
	c.SetNodeID(nodeID)
	return c
}

func TestRoundTripSingleFrame(t *testing.T) {
	sender := makeInstance(42)
	receiver := makeInstance(43)
	rec := &recorderTX{}
	sender.VTable.TX = func(self *Canard, ctx any, dl int64, iface uint8, fd bool, id uint32, data []byte) bool {
		buf := make([]byte, len(data))
		copy(buf, data)
		rec.frames = append(rec.frames, recordedFrame{id: id, data: buf})
		return true
	}

	var got []byte
	sub := &Subscription{}
	receiver.Subscribe16b(sub, 1234, 64, DefaultTransferIDTimeoutUs, &SubscriptionVTable{
		OnMessage: func(s *Subscription, ts int64, p Prio, src uint8, tid uint8, payload Payload) {
			b := make([]byte, payload.View.Size)
			copy(b, unsafeSlice(payload.View))
			got = b
		},
	})

	payload := []byte("hello canard")
	if !sender.Publish16b(1000, IfaceBitmapAll, PrioNominal, 1234, 7, payload, rec) {
		t.Fatal("publish failed")
	}
	sender.Poll(IfaceBitmapAll)

	if len(rec.frames) == 0 {
		t.Fatal("no frames transmitted")
	}
	for _, fr := range rec.frames {
		if !receiver.IngestFrame(1000, 0, fr.id, fr.data) {
			t.Fatalf("ingest failed for id=%x", fr.id)
		}
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %q want %q", got, payload)
	}
}

func TestRoundTripMultiFrameClassic(t *testing.T) {
	sender := makeInstance(42)
	sender.tx.FD = false // force Classic CAN -> multi-frame
	receiver := makeInstance(43)

	rec := &recorderTX{}
	sender.VTable.TX = func(self *Canard, ctx any, dl int64, iface uint8, fd bool, id uint32, data []byte) bool {
		buf := make([]byte, len(data))
		copy(buf, data)
		rec.frames = append(rec.frames, recordedFrame{id: id, data: buf})
		return true
	}

	var got []byte
	sub := &Subscription{}
	receiver.Subscribe13b(sub, 777, 64, DefaultTransferIDTimeoutUs, &SubscriptionVTable{
		OnMessage: func(s *Subscription, ts int64, p Prio, src uint8, tid uint8, payload Payload) {
			b := make([]byte, payload.View.Size)
			copy(b, unsafeSlice(payload.View))
			got = b
		},
	})

	payload := make([]byte, 20)
	for i := range payload {
		payload[i] = byte(i)
	}
	if !sender.Publish13b(1000, IfaceBitmapAll, PrioNominal, 777, 3, payload, rec) {
		t.Fatal("publish failed")
	}
	sender.Poll(IfaceBitmapAll)
	if len(rec.frames) < 2 {
		t.Fatalf("expected multi-frame, got %d", len(rec.frames))
	}
	for _, fr := range rec.frames {
		if !receiver.IngestFrame(1000, 0, fr.id, fr.data) {
			t.Fatalf("ingest failed for id=%x", fr.id)
		}
	}
	if len(got) != len(payload) || string(got) != string(payload) {
		t.Fatalf("payload mismatch: got %v want %v", got, payload)
	}
}
