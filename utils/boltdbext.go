package boltdb

import (
	"encoding/binary"
)

type Uint64Type struct {
	Valid bool
	Uint64 uint64
}
type B8Type [8]byte
type B5Type [5]byte

func ToUInt64(key []byte, value uint64) []byte{
	var buff B8Type
	binary.BigEndian.PutUint64(buff[:],value)
	return buff
}

func FromUInt64(buff []byte)  (result Uint64Type){
	if buff != nil {
		result.Valid = true
		result.Uint64 = binary.BigEndian.Uint64(buff)
	}
	return
}

