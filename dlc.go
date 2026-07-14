package libcanard

// DlcToLen maps a CAN DLC (data length code) to the number of data bytes it represents.
var DlcToLen = [16]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 20, 24, 32, 48, 64}

// LenToDlc maps a number of data bytes to the smallest CAN DLC that can hold them (rounding up).
var LenToDlc = [65]uint8{
	0, 1, 2, 3, 4, 5, 6, 7, 8, // 0-8
	9, 9, 9, 9, // 9-12
	10, 10, 10, 10, // 13-16
	11, 11, 11, 11, // 17-20
	12, 12, 12, 12, // 21-24
	13, 13, 13, 13, 13, 13, 13, 13, // 25-32
	14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, // 33-48
	15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, // 49-64
}
