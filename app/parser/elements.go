package parser

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
)

type BytesReader struct {
	B   []byte
	Off int
}

func (br *BytesReader) CanRead(n int) bool {
	return br.Off+n <= len(br.B)
}

func ReadInt8(br *BytesReader) int8 {
	if !br.CanRead(1) {
		return 0
	}
	v := int8(br.B[br.Off])
	br.Off++
	return v
}

func ReadInt16(br *BytesReader) int16 {
	if !br.CanRead(2) {
		return 0
	}
	v := int16(binary.BigEndian.Uint16(br.B[br.Off : br.Off+2]))
	br.Off += 2
	return v
}

func ReadInt32(br *BytesReader) int32 {
	if !br.CanRead(4) {
		return 0
	}
	v := int32(binary.BigEndian.Uint32(br.B[br.Off : br.Off+4]))
	br.Off += 4
	return v
}

func ReadInt64(br *BytesReader) int64 {
	if !br.CanRead(8) {
		return 0
	}
	v := int64(binary.BigEndian.Uint64(br.B[br.Off : br.Off+8]))
	br.Off += 8
	return v
}

func ReadUVarInt(br *BytesReader) uint32 {
	var x uint32
	var s uint
	for i := 0; i < 5; i++ {
		if !br.CanRead(1) {
			return x
		}
		b := br.B[br.Off]
		br.Off++
		if b < 0x80 {
			if i == 4 && b > 1 {
				return x
			}
			return x | uint32(b)<<s
		}
		x |= uint32(b&0x7F) << s
		s += 7
	}
	return x
}

func ReadVarInt(br *BytesReader) int64 {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		if !br.CanRead(1) {
			return int64(x>>1) ^ -(int64(x) & 1)
		}
		b := br.B[br.Off]
		br.Off++
		if b < 0x80 {
			return int64(x|uint64(b)<<s)>>1 ^ -(int64(x|uint64(b)<<s) & 1)
		}
		x |= uint64(b&0x7F) << s
		s += 7
	}
	return 0
}

func ReadCompactString(br *BytesReader) string {
	l := int(ReadUVarInt(br)) - 1
	if l <= 0 {
		return ""
	}
	if !br.CanRead(l) {
		return ""
	}
	s := string(br.B[br.Off : br.Off+l])
	br.Off += l
	return s
}

func ReadCompactNullableString(br *BytesReader) (string, bool) {
	l := int(ReadUVarInt(br))
	if l == 0 {
		return "", true
	}
	n := l - 1
	if n < 0 || !br.CanRead(n) {
		return "", false
	}
	s := string(br.B[br.Off : br.Off+n])
	br.Off += n
	return s, false
}

func AppendInt16(b []byte, v int16) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	return append(b, tmp[:]...)
}

func AppendInt32(b []byte, v int32) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	return append(b, tmp[:]...)
}

func AppendInt64(b []byte, v int64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	return append(b, tmp[:]...)
}

func AppendUVarInt(b []byte, x uint32) []byte {
	for {
		if (x & ^uint32(0x7F)) == 0 {
			return append(b, byte(x))
		}
		b = append(b, byte(x&0x7F|0x80))
		x >>= 7
	}
}

func AppendCompactString(b []byte, s string) []byte {
	b = AppendUVarInt(b, uint32(len(s)+1))
	return append(b, []byte(s)...)
}

func ParseUUID(in string) ([16]byte, error) {
	var out [16]byte
	s := strings.ReplaceAll(strings.TrimSpace(in), "-", "")
	if len(s) != 32 {
		return out, fmt.Errorf("uuid must be 32 hex chars, got %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

func NilUUID() [16]byte {
	return [16]byte{}
}
