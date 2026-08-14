package libcanard

import "testing"

// TestSetClassicCAN verifies that the exported Classic CAN switch flips tx.FD (the MTU selector)
// and that the default remains CAN FD, matching the C library's default behaviour.
func TestSetClassicCAN(t *testing.T) {
	inst, ok := New(&VTable{
		Now: func(self *Canard) int64 { return 0 },
		TX:  func(self *Canard, uc any, dl int64, idx uint8, fd bool, id uint32, data []byte) bool { return true },
	}, NewDefaultMemSet(), IfaceBitmapAll, 16, 0, 0)
	if !ok {
		t.Fatal("New returned false")
	}
	if !inst.tx.FD {
		t.Fatal("default tx.FD should be true (CAN FD)")
	}

	inst.SetClassicCAN(true)
	if inst.tx.FD {
		t.Error("SetClassicCAN(true) should set tx.FD = false (Classic CAN)")
	}

	inst.SetClassicCAN(false)
	if !inst.tx.FD {
		t.Error("SetClassicCAN(false) should restore tx.FD = true (CAN FD)")
	}
}
