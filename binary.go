package libfat

import "encoding/binary"

func ReadUint16LE(data []byte, offset int) uint16 {
	return binary.LittleEndian.Uint16(data[offset : offset+2])
}

func ReadUint32LE(data []byte, offset int) uint32 {
	return binary.LittleEndian.Uint32(data[offset : offset+4])
}

func trimASCIISpaces(data []byte) string {
	end := len(data)
	for end > 0 && data[end-1] == ' ' {
		end--
	}
	return string(data[:end])
}
