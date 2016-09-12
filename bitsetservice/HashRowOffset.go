package bitsetservice

import (
	"encoding/binary"
	"io"
	"fmt"
)


type HRO struct {
	Hash       uint64
	Category   string
	Data1  uint64
	Data2  uint64
}


func HROKey(hash uint64) string {
	return fmt.Sprintf("%X", hash & 0x00000000000000FF);
}
func (h HRO) getKey() string {
	return HROKey(h.Hash)
}

var uint64Size = int64(binary.Size(uint64(0)));
var uint32Size = int64(binary.Size(uint32(0)));

func (h HRO) WriteTo(writer io.Writer)  (int64, error) {
	var err error

	err = binary.Write(writer, binary.BigEndian, h.Hash)
	//_,err = writer.Write( (*(*[8]byte)(unsafe.Pointer(&h.Hash)))[0:8] )
	if err != nil {
		return uint64Size, err
	}
	err = binary.Write(writer, binary.BigEndian, h.Data1)
	//_,err = writer.Write( (*(*[4]byte)(unsafe.Pointer(&h.RowNumber)))[0:4] )
	if err != nil {
		return uint64Size, err
	}
	err = binary.Write(writer, binary.BigEndian, h.Data2)
	//_,err = writer.Write( (*(*[8]byte)(unsafe.Pointer(&h.FileOffset)))[0:8] )
	if err != nil {
		return uint64Size, err
	}
	return uint64Size + uint64Size + uint64Size, nil
}



func (h *HRO) ReadFrom(reader io.Reader)  (int64, error) {
	/*		if _,err := reader.Read((*(*[8]byte)(unsafe.Pointer(&(*h).Hash)))[0:8]); err != nil {
				return int64(binary.Size(uint32(0))), err
			}
			if _,err := reader.Read((*(*[4]byte)(unsafe.Pointer(&(*h).RowNumber)))[0:4]); err != nil {
				return int64(binary.Size(uint32(0))), err
			}
			if _,err := reader.Read((*(*[8]byte)(unsafe.Pointer(&(*h).FileOffset)))[0:8]); err != nil {
				return int64(binary.Size(uint32(0))), err
			}*/
	//fmt.Println(h.RowNumber)

	var err error
	err = binary.Read(reader, binary.BigEndian, &(*h).Hash)
	if err != nil {
		return uint64Size, err
	}
	err = binary.Read(reader, binary.BigEndian, &(*h).Data1)
	if err != nil {
		return uint64Size, err
	}
	err = binary.Read(reader, binary.BigEndian, &(*h).Data2)
	if err != nil {
		return uint64Size, err
	}
	return uint64Size + uint64Size + uint64Size, nil
}

