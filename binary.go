package libfat

import "encoding/binary"

// ReadUint16LE decodes a little-endian uint16 at offset. It returns 0 when the
// slice is too short rather than panicking, so that parsers operating on
// truncated or corrupt image data remain panic-free.
func ReadUint16LE(data []byte, offset int) uint16 {
	if offset < 0 || offset+2 > len(data) {
		return 0
	}
	return binary.LittleEndian.Uint16(data[offset : offset+2])
}

// ReadUint32LE decodes a little-endian uint32 at offset. It returns 0 when the
// slice is too short rather than panicking.
func ReadUint32LE(data []byte, offset int) uint32 {
	if offset < 0 || offset+4 > len(data) {
		return 0
	}
	return binary.LittleEndian.Uint32(data[offset : offset+4])
}

func trimASCIISpaces(data []byte) string {
	end := len(data)
	for end > 0 && data[end-1] == ' ' {
		end--
	}
	return string(data[:end])
}
