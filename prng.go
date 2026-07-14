package libcanard

import "math/bits"

const (
	tailSOT       = 128
	tailEOT       = 64
	tailToggle    = 32
	transferIDMax = TransferIDMax

	byteMax           = 0xFF
	bigBang           = int64(-1 << 63) // INT64_MIN
	canExtIDMask      = (uint32(1) << 29) - 1
	paddingByteValue  = 0
	prioShift         = 26
	dlcBits           = 4
	canIDMSbBits      = 29 - 7 // Bits [28:7] of the CAN ID, used for TX ordering.
)

// popcount returns the number of set bits in x.
func popcount(x uint64) uint8 { return uint8(bits.OnesCount64(x)) }

// splitmix64 is the splitmix64 PRNG (public domain, by Sebastiano Vigna).
func splitmix64(state *uint64) uint64 {
	*state += 0x9E3779B97F4A7C15
	z := *state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// random obtains a pseudo-random number in [0, bound). Returns 0 if bound <= 1.
func random(self *Canard, bound uint64) uint64 {
	if bound > 0 {
		return splitmix64(&self.PRNGState) % bound
	}
	return 0
}

// chance returns true with a probability of approximately 1/pReciprocal. Always true if pReciprocal <= 1.
func chance(self *Canard, pReciprocal uint64) bool {
	return random(self, pReciprocal) == 0
}

func smaller(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func later(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func bitmapSet(b *[2]uint64, i int) {
	b[i/64] |= uint64(1) << uint(i%64)
}

func bitmapTest(b *[2]uint64, i int) bool {
	return (b[i/64] & (uint64(1) << uint(i%64))) != 0
}
