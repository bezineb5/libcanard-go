package libcanard

const (
	crcInitial = 0xFFFF
	crcResidue = 0x0000
	crcBytes   = 2
)

// crcTable is the CRC-16/CCITT-FALSE table (polynomial 0x1021, init 0xFFFF, no reflection).
var crcTable [256]uint16

func init() {
	for i := range 256 {
		c := uint16(i) << 8
		for range 8 {
			if c&0x8000 != 0 {
				c = (c << 1) ^ 0x1021
			} else {
				c <<= 1
			}
		}
		crcTable[i] = c
	}
}

func crcAddByte(crc uint16, b byte) uint16 {
	return (crc << 8) ^ crcTable[((crc>>8)^uint16(b))&0xFF]
}

func crcAdd(crc uint16, size int, data []byte) uint16 {
	out := crc
	for i := range size {
		out = crcAddByte(out, data[i])
	}
	return out
}

// CrcSeedFromDataTypeSignature computes the CRC-16/CCITT-FALSE checksum of the data type signature in
// little-endian byte order; this value seeds the transfer CRC for UAVCAN v0 and DroneCAN transfers.
func CrcSeedFromDataTypeSignature(dataTypeSignature uint64) uint16 {
	crc := uint16(crcInitial)
	sig := dataTypeSignature
	for range 8 {
		crc = crcAddByte(crc, byte(sig&0xFF))
		sig >>= 8
	}
	return crc
}

// v0CRCSeedFromDataTypeSignature is an alias for CrcSeedFromDataTypeSignature for compatibility with test names.
func v0CRCSeedFromDataTypeSignature(dataTypeSignature uint64) uint16 {
	return CrcSeedFromDataTypeSignature(dataTypeSignature)
}
