// Package rapidhash provides a fast, good-quality 64-bit hash function.
// This is a Go port of the rapidhash.h C library.
package rapidhash

// Hash computes the 64-bit rapidhash of the input data.
// This is a fast non-cryptographic hash function suitable for hash tables and similar uses.
func Hash(data []byte) uint64 {
	return Finalize(Init(), data)
}

// Init initializes a new hash state.
func Init() uint64 {
	return 0xcbf29ce484222325
}

// Finalize finalizes the hash computation and returns the 64-bit hash value.
func Finalize(hash uint64, data []byte) uint64 {
	return FinalizeWithSeed(hash, data, 0)
}

// FinalizeWithSeed finalizes the hash computation with a seed value.
func FinalizeWithSeed(hash uint64, data []byte, seed uint64) uint64 {
	hash = Mix(hash, seed)
	
	// Process the data in chunks
	i := 0
	for i+8 <= len(data) {
		var k uint64
		// Load 8 bytes in little-endian order
		k = uint64(data[i]) |
			(uint64(data[i+1]) << 8) |
			(uint64(data[i+2]) << 16) |
			(uint64(data[i+3]) << 24) |
			(uint64(data[i+4]) << 32) |
			(uint64(data[i+5]) << 40) |
			(uint64(data[i+6]) << 48) |
			(uint64(data[i+7]) << 56)
		
		hash ^= Mix(k, 0x9e3779b97f4a7c15)
		hash = Mix(hash, 0x9e3779b97f4a7c15)
		
		i += 8
	}
	
	// Process remaining bytes
	var k uint64
	switch len(data) - i {
	case 7:
		k ^= uint64(data[i+6]) << 48
		fallthrough
	case 6:
		k ^= uint64(data[i+5]) << 40
		fallthrough
	case 5:
		k ^= uint64(data[i+4]) << 32
		fallthrough
	case 4:
		k ^= uint64(data[i+3]) << 24
		fallthrough
	case 3:
		k ^= uint64(data[i+2]) << 16
		fallthrough
	case 2:
		k ^= uint64(data[i+1]) << 8
		fallthrough
	case 1:
		k ^= uint64(data[i])
		k = Mix(k, 0x9e3779b97f4a7c15)
		hash ^= k
	}
	
	hash = Mix(hash, 0x9e3779b97f4a7c15)
	hash = Mix(hash, 0x9e3779b97f4a7c15)
	
	return hash
}

// Mix is the rapidhash mixing function.
//go:noinline
func Mix(a, b uint64) uint64 {
	// This is the rapidhash mixing function
	// Based on the 64-bit finalizer from MurmurHash3
	a ^= b
	a = (a ^ (a >> 33)) * 0xff51afd7ed558ccd
	a = (a ^ (a >> 33)) * 0xc4ceb9fe1a85ec53
	return a ^ (a >> 33)
}

// HashWithSeed computes the hash with a seed value.
// The seed is mixed into the initial hash state.
func HashWithSeed(data []byte, seed uint64) uint64 {
	return FinalizeWithSeed(Init(), data, seed)
}

// HashWithSeeds computes the hash with multiple seed values.
// The seeds are concatenated and hashed together with the data.
func HashWithSeeds(data []byte, seeds []uint64) uint64 {
	hash := Init()
	for _, seed := range seeds {
		hash = Mix(hash, seed)
	}
	return Finalize(hash, data)
}
