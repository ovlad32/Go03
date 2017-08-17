package B8

import "encoding/binary"

const HashLength  = 8
type B8Type [HashLength  ]byte

func UInt64ToB8(value uint64) []byte {
	var buff B8Type
	binary.LittleEndian.PutUint64(buff[:], value)
	return buff[:]
}

func UInt64ToBuff(dst []byte, src uint64) {
	binary.LittleEndian.PutUint64(dst, src)
}
func Clear(dst []byte) []byte {
	if dst == nil || cap(dst) < HashLength   {
		dst = make([]byte, HashLength )
	} else {
		dst = append(dst[0:0], ([]byte{0, 0, 0, 0, 0, 0, 0, 0})...)
	}
	return dst
}

func B8ToUInt64(buff []byte) (result uint64, valid bool) {
	if buff != nil {
		valid = true
		result = binary.LittleEndian.Uint64(buff)
	}
	return
}

func Int64ToB8(value int64) []byte {
	var buff B8Type
	binary.LittleEndian.PutUint64(buff[:], uint64(value))
	return buff[:]
}

func B8ToInt64(buff []byte) (result int64, valid bool) {
	if buff != nil {
		valid = true
		result = int64(binary.LittleEndian.Uint64(buff))
	}
	return
}
