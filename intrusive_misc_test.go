package libcanard

// This file migrates tests/src/test_intrusive_misc.c (the C "misc" white-box test suite) to Go.
//
// It exercises the node-ID occupancy / collision-reroll / probabilistic-purge logic and the
// TX-queue purge-on-collision behaviour. Like the C suite it is a white-box test living in the
// same package, so it may call the unexported helpers directly.
//
// Differences from the C original:
//   - Canard instances are built manually (set PRNGState / NodeID / bitmap directly) instead of via
//     Init, because Init XORs the PRNG seed with the instance pointer and would defeat the
//     find_seed_* helpers that rely on a specific seed.
//   - The C TX tests count instrumented-allocator fragments. The Go port uses the Go runtime for
//     fixed-size objects, so instead of allocated_fragments we assert on the agewise transfer list
//     (listHead[txTransfer](&self.tx.agewise)) and self.tx.queueSize, which directly reflect
//     whether a transfer survived txPurgeContinuations.

import (
	"testing"
)

// ---------------------------------------------------------------------------------------------------------------------
// Fixtures and helpers
// ---------------------------------------------------------------------------------------------------------------------

// makeCanard builds a Canard with the given PRNG seed and node-ID, with the occupancy bitmap reset.
func makeCanard(prngSeed uint64, nodeID uint8) Canard {
	var self Canard
	self.PRNGState = prngSeed
	self.NodeID = nodeID
	nodeIDOccupancyReset(&self)
	return self
}

// fillBitmapExcept sets every bit, then clears the listed positions. Bit 0 is always set after a
// reset, so callers should not include 0 in except.
func fillBitmapExcept(self *Canard, except []int) {
	self.NodeIDOccupancyBitmap[0] = ^uint64(0)
	self.NodeIDOccupancyBitmap[1] = ^uint64(0)
	for _, e := range except {
		self.NodeIDOccupancyBitmap[e/64] &^= uint64(1) << uint(e%64)
	}
}

// findSeedDense finds a seed where, inside nodeIDOccupancyUpdate for the given zc:
//   - the purge chance (first PRNG step) returns wantPurge
//   - the subsequent reroll random (second PRNG step) returns wantZ
func findSeedDense(zc uint64, wantPurge bool, wantZ uint64) uint64 {
	for seed := range uint64(10000000) {
		var probe Canard
		probe.PRNGState = seed
		ch := chance(&probe, zc) // first PRNG step
		if ch != wantPurge {
			continue
		}
		r := random(&probe, zc) // second PRNG step
		if r == wantZ {
			return seed
		}
	}
	return ^uint64(0)
}

// findSeedChance finds a seed where chance(&self, pReciprocal) returns want (single PRNG step).
func findSeedChance(pReciprocal uint64, want bool) uint64 {
	for seed := range uint64(10000000) {
		var probe Canard
		probe.PRNGState = seed
		if chance(&probe, pReciprocal) == want {
			return seed
		}
	}
	return ^uint64(0)
}

func fatalSeedNotFound(t *testing.T, seed uint64) {
	t.Helper()
	if seed == ^uint64(0) {
		t.Fatal("find_seed: no matching seed found (unexpected)")
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// TX test infrastructure (needed for purge-on-collision tests)
// ---------------------------------------------------------------------------------------------------------------------

// makeTXCanard builds a Canard suitable for TX tests: a vtable whose TX callback never ejects
// (we control frame departure manually), the default memory set, full interface bitmap, classic/ FD
// flags and queue capacity.
func makeTXCanard(nodeID uint8, prngSeed uint64) *Canard {
	self := &Canard{}
	self.VTable = &VTable{
		Now: func(self *Canard) int64 { return 0 },
		TX:  func(self *Canard, _ any, _ int64, _ uint8, _ bool, _ uint32, _ []byte) bool { return false },
	}
	self.IfaceBitmap = IfaceBitmapAll
	self.Mem = NewDefaultMemSet()
	self.tx.queueCapacity = 64
	self.tx.FD = true
	self.PRNGState = prngSeed
	self.NodeID = nodeID
	nodeIDOccupancyReset(self)
	return self
}

// enqueueTransfer enqueues a transfer with a small (single-frame) or large (multi-frame) payload.
func enqueueTransfer(self *Canard, multiFrame bool) *txTransfer {
	dataSmall := []byte{0xAA}
	dataLarge := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13} // >7 bytes => multi-frame classic
	d := dataSmall
	if multiFrame {
		d = dataLarge
	}
	canID := uint32(PrioNominal) << prioShift
	tr := txTransferNew(self, 1000000, canID, false, nil)
	if tr != nil {
		if !self.txPush(tr, false, IfaceBitmapAll, 5, d, crcInitial) {
			tr = nil // txPush already freed tr on failure
		}
	}
	return tr
}

// freeAllTransfers retires every queued transfer.
func freeAllTransfers(self *Canard) {
	for {
		tr := listHead[txTransfer](&self.tx.agewise)
		if tr == nil {
			break
		}
		self.txRetire(tr)
	}
}

func assertNoTransferQueued(t *testing.T, self *Canard) {
	t.Helper()
	if listHead[txTransfer](&self.tx.agewise) != nil {
		t.Fatal("expected no queued transfer")
	}
	if self.tx.queueSize != 0 {
		t.Fatalf("expected tx.queueSize==0, got %d", self.tx.queueSize)
	}
}

func assertTransferQueued(t *testing.T, self *Canard) {
	t.Helper()
	if listHead[txTransfer](&self.tx.agewise) == nil {
		t.Fatal("expected a queued transfer to survive")
	}
	if self.tx.queueSize == 0 {
		t.Fatal("expected tx.queueSize>0 for the surviving transfer")
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// Group 1: nodeIDOccupancyReset
// ---------------------------------------------------------------------------------------------------------------------

func TestMiscResetClearsBitmap(t *testing.T) {
	var self Canard
	self.NodeIDOccupancyBitmap[0] = ^uint64(0)
	self.NodeIDOccupancyBitmap[1] = ^uint64(0)
	nodeIDOccupancyReset(&self)
	if self.NodeIDOccupancyBitmap[0] != 1 {
		t.Fatalf("bitmap[0]=%d, want 1", self.NodeIDOccupancyBitmap[0])
	}
	if self.NodeIDOccupancyBitmap[1] != 0 {
		t.Fatalf("bitmap[1]=%d, want 0", self.NodeIDOccupancyBitmap[1])
	}
}

func TestMiscResetPreservesOtherFields(t *testing.T) {
	var self Canard
	self.NodeID = 42
	self.PRNGState = 123
	self.Err.Collision = 7
	self.NodeIDOccupancyBitmap[0] = ^uint64(0)
	self.NodeIDOccupancyBitmap[1] = ^uint64(0)
	nodeIDOccupancyReset(&self)
	if self.NodeID != 42 {
		t.Fatalf("NodeID=%d, want 42", self.NodeID)
	}
	if self.PRNGState != 123 {
		t.Fatalf("PRNGState=%d, want 123", self.PRNGState)
	}
	if self.Err.Collision != 7 {
		t.Fatalf("Collision=%d, want 7", self.Err.Collision)
	}
	if self.NodeIDOccupancyBitmap[0] != 1 {
		t.Fatalf("bitmap[0]=%d, want 1", self.NodeIDOccupancyBitmap[0])
	}
	if self.NodeIDOccupancyBitmap[1] != 0 {
		t.Fatalf("bitmap[1]=%d, want 0", self.NodeIDOccupancyBitmap[1])
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// Group 2: Early Exit Paths
// ---------------------------------------------------------------------------------------------------------------------

func TestMiscUpdateAnonymousNoop(t *testing.T) {
	self := makeCanard(0, 10)
	bm0, bm1, prng := self.NodeIDOccupancyBitmap[0], self.NodeIDOccupancyBitmap[1], self.PRNGState
	nodeIDOccupancyUpdate(&self, NodeIDAnonymous)
	if self.NodeIDOccupancyBitmap[0] != bm0 || self.NodeIDOccupancyBitmap[1] != bm1 {
		t.Fatal("bitmap changed on anonymous update")
	}
	if self.PRNGState != prng {
		t.Fatal("PRNGState changed on anonymous update")
	}
	if self.Err.Collision != 0 {
		t.Fatalf("Collision=%d, want 0", self.Err.Collision)
	}
}

func TestMiscUpdateKnownNoncollidingNoop(t *testing.T) {
	self := makeCanard(0, 10)
	bitmapSet(&self.NodeIDOccupancyBitmap, 42)
	prng := self.PRNGState
	nodeIDOccupancyUpdate(&self, 42) // bitmap_test(42) && node_id(10)!=42 => early return
	if self.PRNGState != prng {
		t.Fatal("PRNGState changed (early return expected)")
	}
	if self.Err.Collision != 0 {
		t.Fatalf("Collision=%d, want 0", self.Err.Collision)
	}
	if self.NodeID != 10 {
		t.Fatalf("NodeID=%d, want 10", self.NodeID)
	}
}

func TestMiscUpdateNewSrcMarksBit(t *testing.T) {
	self := makeCanard(0, 10)
	if bitmapTest(&self.NodeIDOccupancyBitmap, 42) {
		t.Fatal("bit 42 unexpectedly set")
	}
	nodeIDOccupancyUpdate(&self, 42)
	if !bitmapTest(&self.NodeIDOccupancyBitmap, 42) {
		t.Fatal("bit 42 not marked by update with new src")
	}
	if self.NodeID != 10 {
		t.Fatalf("NodeID=%d, want 10", self.NodeID)
	}
	if self.Err.Collision != 0 {
		t.Fatalf("Collision=%d, want 0", self.Err.Collision)
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// Group 3: Collision Reroll
// ---------------------------------------------------------------------------------------------------------------------

func TestMiscCollisionBasic(t *testing.T) {
	self := makeCanard(0, 42)
	nodeIDOccupancyUpdate(&self, 42)
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	if !self.rx.filtersDirty {
		t.Fatal("filters_dirty not set on collision")
	}
	if self.NodeID == 42 {
		t.Fatal("node-ID not rerolled")
	}
	if self.NodeID == 0 || self.NodeID > NodeIDMax {
		t.Fatalf("node-ID out of range: %d", self.NodeID)
	}
	if bitmapTest(&self.NodeIDOccupancyBitmap, int(self.NodeID)) {
		t.Fatal("new node-ID bit is set (should be free)")
	}
}

func TestMiscCollisionKnownSrcStillTriggers(t *testing.T) {
	self := makeCanard(0, 42)
	bitmapSet(&self.NodeIDOccupancyBitmap, 42) // pre-mark: bitmap_test true but node_id==src
	nodeIDOccupancyUpdate(&self, 42)
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	if !self.rx.filtersDirty {
		t.Fatal("filters_dirty not set on collision")
	}
	if self.NodeID == 42 {
		t.Fatal("node-ID not rerolled")
	}
	if self.NodeID == 0 || self.NodeID > NodeIDMax {
		t.Fatalf("node-ID out of range: %d", self.NodeID)
	}
}

func TestMiscCollisionZcOneMidrange(t *testing.T) {
	self := makeCanard(0, 10)
	fillBitmapExcept(&self, []int{50})
	nodeIDOccupancyUpdate(&self, 10)
	if self.NodeID != 50 {
		t.Fatalf("NodeID=%d, want 50", self.NodeID)
	}
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	if !self.rx.filtersDirty {
		t.Fatal("filters_dirty not set")
	}
	if !bitmapTest(&self.NodeIDOccupancyBitmap, 0) || !bitmapTest(&self.NodeIDOccupancyBitmap, 10) {
		t.Fatal("purge did not reset bitmap to {0, 10}")
	}
	pc := popcount(self.NodeIDOccupancyBitmap[0]) + popcount(self.NodeIDOccupancyBitmap[1])
	if pc != 2 {
		t.Fatalf("popcount=%d, want 2", pc)
	}
}

func TestMiscCollisionZcOneBitOne(t *testing.T) {
	self := makeCanard(0, 10)
	fillBitmapExcept(&self, []int{1})
	nodeIDOccupancyUpdate(&self, 10)
	if self.NodeID != 1 {
		t.Fatalf("NodeID=%d, want 1", self.NodeID)
	}
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
}

func TestMiscCollisionZcOneBit127(t *testing.T) {
	self := makeCanard(0, 10)
	fillBitmapExcept(&self, []int{127})
	nodeIDOccupancyUpdate(&self, 10)
	if self.NodeID != 127 {
		t.Fatalf("NodeID=%d, want 127", self.NodeID)
	}
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
}

func TestMiscCollisionTwoFreeFirst(t *testing.T) {
	seed := findSeedDense(2, false, 0)
	fatalSeedNotFound(t, seed)
	self := makeCanard(seed, 10)
	fillBitmapExcept(&self, []int{5, 100})
	nodeIDOccupancyUpdate(&self, 10)
	if self.NodeID != 5 {
		t.Fatalf("NodeID=%d, want 5", self.NodeID)
	}
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	pc := popcount(self.NodeIDOccupancyBitmap[0]) + popcount(self.NodeIDOccupancyBitmap[1])
	if pc != 126 {
		t.Fatalf("popcount=%d, want 126 (no purge)", pc)
	}
}

func TestMiscCollisionTwoFreeSecond(t *testing.T) {
	seed := findSeedDense(2, false, 1)
	fatalSeedNotFound(t, seed)
	self := makeCanard(seed, 10)
	fillBitmapExcept(&self, []int{5, 100})
	nodeIDOccupancyUpdate(&self, 10)
	if self.NodeID != 100 {
		t.Fatalf("NodeID=%d, want 100", self.NodeID)
	}
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	pc := popcount(self.NodeIDOccupancyBitmap[0]) + popcount(self.NodeIDOccupancyBitmap[1])
	if pc != 126 {
		t.Fatalf("popcount=%d, want 126 (no purge)", pc)
	}
}

func TestMiscCollisionRepeated(t *testing.T) {
	self := makeCanard(0, 42)
	for i := range 3 {
		oldID := self.NodeID
		nodeIDOccupancyUpdate(&self, self.NodeID)
		if self.Err.Collision != uint64(i+1) {
			t.Fatalf("Collision=%d, want %d", self.Err.Collision, i+1)
		}
		if oldID == self.NodeID {
			t.Fatal("node-ID did not change on collision")
		}
		if self.NodeID == 0 || self.NodeID > NodeIDMax {
			t.Fatalf("node-ID out of range: %d", self.NodeID)
		}
		if bitmapTest(&self.NodeIDOccupancyBitmap, int(self.NodeID)) {
			t.Fatal("new node-ID bit is set (should be free)")
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// Group 4: Probabilistic Purge
// ---------------------------------------------------------------------------------------------------------------------

func TestMiscPurgeBelowThresholdNoCall(t *testing.T) {
	self := makeCanard(123, 120)
	for i := 1; i < 63; i++ {
		bitmapSet(&self.NodeIDOccupancyBitmap, i)
	}
	// pc=63 now; adding src=63 => pc=64, 64>64 is false => chance NOT called.
	prng := self.PRNGState
	nodeIDOccupancyUpdate(&self, 63)
	if !bitmapTest(&self.NodeIDOccupancyBitmap, 63) {
		t.Fatal("bit 63 not set")
	}
	if self.PRNGState != prng {
		t.Fatalf("PRNGState changed (chance should not have been called): %d vs %d", self.PRNGState, prng)
	}
	if self.Err.Collision != 0 {
		t.Fatalf("Collision=%d, want 0", self.Err.Collision)
	}
	if self.NodeID != 120 {
		t.Fatalf("NodeID=%d, want 120", self.NodeID)
	}
}

func TestMiscPurgeAboveThresholdFires(t *testing.T) {
	seed := findSeedChance(63, true)
	fatalSeedNotFound(t, seed)
	self := makeCanard(seed, 120)
	for i := 1; i < 64; i++ {
		bitmapSet(&self.NodeIDOccupancyBitmap, i)
	}
	// pc=64; add src=64 => pc=65 > 64 => chance(self, 63) returns true => purge fires.
	nodeIDOccupancyUpdate(&self, 64)
	if self.Err.Collision != 0 {
		t.Fatalf("Collision=%d, want 0", self.Err.Collision)
	}
	if self.NodeID != 120 {
		t.Fatalf("NodeID=%d, want 120", self.NodeID)
	}
	if !bitmapTest(&self.NodeIDOccupancyBitmap, 0) || !bitmapTest(&self.NodeIDOccupancyBitmap, 64) {
		t.Fatal("purge did not reset bitmap to {0, 64}")
	}
	pc := popcount(self.NodeIDOccupancyBitmap[0]) + popcount(self.NodeIDOccupancyBitmap[1])
	if pc != 2 {
		t.Fatalf("popcount=%d, want 2", pc)
	}
}

func TestMiscPurgeWithCollision(t *testing.T) {
	seed := findSeedDense(2, true, 1)
	fatalSeedNotFound(t, seed)
	self := makeCanard(seed, 10)
	fillBitmapExcept(&self, []int{5, 100})
	nodeIDOccupancyUpdate(&self, 10)
	if self.NodeID != 100 {
		t.Fatalf("NodeID=%d, want 100", self.NodeID)
	}
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	if !self.rx.filtersDirty {
		t.Fatal("filters_dirty not set")
	}
	if !bitmapTest(&self.NodeIDOccupancyBitmap, 0) || !bitmapTest(&self.NodeIDOccupancyBitmap, 10) {
		t.Fatal("purge did not reset bitmap to {0, 10}")
	}
	pc := popcount(self.NodeIDOccupancyBitmap[0]) + popcount(self.NodeIDOccupancyBitmap[1])
	if pc != 2 {
		t.Fatalf("popcount=%d, want 2", pc)
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// Group 5: TX Purge on Collision
// ---------------------------------------------------------------------------------------------------------------------

func TestMiscCollisionEmptyTXQueue(t *testing.T) {
	self := makeTXCanard(42, 0)
	nodeIDOccupancyUpdate(self, 42)
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	if self.NodeID == 42 {
		t.Fatal("node-ID not rerolled")
	}
	if self.NodeID == 0 || self.NodeID > NodeIDMax {
		t.Fatalf("node-ID out of range: %d", self.NodeID)
	}
	assertNoTransferQueued(t, self)
}

func TestMiscCollisionPurgesStartedMultiframe(t *testing.T) {
	self := makeTXCanard(42, 0)
	tr := enqueueTransfer(self, true)
	if tr == nil {
		t.Fatal("enqueue failed")
	}
	if !tr.multiFrame {
		t.Fatal("expected multi-frame transfer")
	}
	tr.firstFrameDeparted = true
	nodeIDOccupancyUpdate(self, 42)
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	// Multi-frame transfer whose first frame departed must be purged.
	assertNoTransferQueued(t, self)
}

func TestMiscCollisionKeepsUndeparted(t *testing.T) {
	self := makeTXCanard(42, 0)
	tr := enqueueTransfer(self, true)
	if tr == nil {
		t.Fatal("enqueue failed")
	}
	if !tr.multiFrame {
		t.Fatal("expected multi-frame transfer")
	}
	if tr.firstFrameDeparted {
		t.Fatal("transfer should not be marked departed")
	}
	nodeIDOccupancyUpdate(self, 42)
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	// Not departed => not purged.
	assertTransferQueued(t, self)
	freeAllTransfers(self)
	assertNoTransferQueued(t, self)
}

func TestMiscCollisionKeepsSingleframe(t *testing.T) {
	self := makeTXCanard(42, 0)
	tr := enqueueTransfer(self, false)
	if tr == nil {
		t.Fatal("enqueue failed")
	}
	if tr.multiFrame {
		t.Fatal("expected single-frame transfer")
	}
	tr.firstFrameDeparted = true
	nodeIDOccupancyUpdate(self, 42)
	if self.Err.Collision != 1 {
		t.Fatalf("Collision=%d, want 1", self.Err.Collision)
	}
	// Single-frame transfers are never purged.
	assertTransferQueued(t, self)
	freeAllTransfers(self)
	assertNoTransferQueued(t, self)
}

// ---------------------------------------------------------------------------------------------------------------------
// Group 6: Exhaustive / Property Tests
// ---------------------------------------------------------------------------------------------------------------------

func TestMiscCollisionExhaustiveZcOne(t *testing.T) {
	for p := 1; p <= NodeIDMax; p++ {
		occupiedID := uint8(10)
		if p == 10 {
			occupiedID = 11
		}
		self := makeCanard(0, occupiedID)
		fillBitmapExcept(&self, []int{p})
		colliding := self.NodeID
		nodeIDOccupancyUpdate(&self, colliding)
		if self.NodeID != uint8(p) {
			t.Fatalf("p=%d: NodeID=%d, want %d", p, self.NodeID, p)
		}
		if self.Err.Collision != 1 {
			t.Fatalf("p=%d: Collision=%d, want 1", p, self.Err.Collision)
		}
	}
}

func TestMiscCollisionNeverPicksOccupied(t *testing.T) {
	rng := uint64(0xDEADBEEFCAFEBABE)
	for iter := range 1000 {
		var self Canard
		self.PRNGState = splitmix64(&rng)
		// Bit 0 always set; bits [1,127] randomly set with ~50% probability.
		self.NodeIDOccupancyBitmap[0] = 1
		self.NodeIDOccupancyBitmap[1] = 0
		for b := 1; b <= NodeIDMax; b++ {
			if (splitmix64(&rng) % 2) == 0 {
				bitmapSet(&self.NodeIDOccupancyBitmap, b)
			}
		}
		// Ensure at least 2 free bits.
		pc := popcount(self.NodeIDOccupancyBitmap[0]) + popcount(self.NodeIDOccupancyBitmap[1])
		if pc >= 127 {
			self.NodeIDOccupancyBitmap[0] &^= uint64(1) << 1
			self.NodeIDOccupancyBitmap[0] &^= uint64(1) << 2
		}
		// Pick an occupied node-ID to collide on.
		nid := uint8(0)
		for b := 1; b <= NodeIDMax; b++ {
			if bitmapTest(&self.NodeIDOccupancyBitmap, b) {
				nid = uint8(b)
				break
			}
		}
		if nid == 0 {
			bitmapSet(&self.NodeIDOccupancyBitmap, 1)
			nid = 1
		}
		self.NodeID = nid
		nodeIDOccupancyUpdate(&self, nid)
		if self.Err.Collision != 1 {
			t.Fatalf("iter %d: Collision=%d, want 1", iter, self.Err.Collision)
		}
		if self.NodeID == 0 || self.NodeID > NodeIDMax {
			t.Fatalf("iter %d: node-ID out of range: %d", iter, self.NodeID)
		}
		if bitmapTest(&self.NodeIDOccupancyBitmap, int(self.NodeID)) {
			t.Fatalf("iter %d: collision landed on occupied bit %d", iter, self.NodeID)
		}
		if self.NodeID == nid {
			t.Fatalf("iter %d: node-ID unchanged after collision", iter)
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// Group 7: Nearly-Full Bitmap and Purge Reset
// ---------------------------------------------------------------------------------------------------------------------

func TestMiscOccupancyBitmapNearlyFull(t *testing.T) {
	except := []int{50, 100}
	for _, want := range []uint8{50, 100} {
		var z uint64
		if want == 100 {
			z = 1 // second free bit
		}
		seed := findSeedDense(2, false, z) // 0 => first free (50), 1 => second free (100)
		fatalSeedNotFound(t, seed)
		self := makeCanard(seed, 10)
		fillBitmapExcept(&self, except)
		nodeIDOccupancyUpdate(&self, 10)
		if self.Err.Collision != 1 {
			t.Fatalf("Collision=%d, want 1", self.Err.Collision)
		}
		if self.NodeID != want {
			t.Fatalf("NodeID=%d, want %d", self.NodeID, want)
		}
	}
}

func TestMiscCollisionPurgeResetsBitmap(t *testing.T) {
	seed := findSeedChance(62, true)
	fatalSeedNotFound(t, seed)
	self := makeCanard(seed, 120)
	for i := 1; i < 65; i++ {
		bitmapSet(&self.NodeIDOccupancyBitmap, i)
	}
	// pc=65; add src=65 => pc=66 > 64 => chance(self, 62) returns true => purge fires.
	nodeIDOccupancyUpdate(&self, 65)
	if self.Err.Collision != 0 {
		t.Fatalf("Collision=%d, want 0", self.Err.Collision)
	}
	if self.NodeID != 120 {
		t.Fatalf("NodeID=%d, want 120", self.NodeID)
	}
	if !bitmapTest(&self.NodeIDOccupancyBitmap, 0) || !bitmapTest(&self.NodeIDOccupancyBitmap, 65) {
		t.Fatal("purge did not reset bitmap to {0, 65}")
	}
	pc := popcount(self.NodeIDOccupancyBitmap[0]) + popcount(self.NodeIDOccupancyBitmap[1])
	if pc != 2 {
		t.Fatalf("popcount=%d, want 2", pc)
	}
	for i := 1; i < 65; i++ {
		if bitmapTest(&self.NodeIDOccupancyBitmap, i) {
			t.Fatalf("bit %d not cleared by purge", i)
		}
	}
}
