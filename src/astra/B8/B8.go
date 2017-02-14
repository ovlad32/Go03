package B8

import "encoding/binary"

type B8Type [8]byte

func UInt64ToB8(value uint64) []byte {
	var buff B8Type
	binary.LittleEndian.PutUint64(buff[:], value)
	return buff[:]
}

func UInt64ToBuff(dst []byte, src uint64) {
	binary.LittleEndian.PutUint64(dst[:], src)
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
