package dataflow

import (
	"encoding/binary"
	"sparsebitset"
)

type ColumnBlockType struct {
	Data []byte
}

var (
	columnIdPosition    = uint64(0)
	countKeyValPosition = uint64(8)
	keyValStartPosition = uint64(16)
	keyLen              = uint64(8)
	valLen              = uint64(8)
)

func (s *ColumnBlockType) Append(columnId int64, offset uint64) (columns []int64) {
	//ColumnId[1],
	//      CountBitsetKeyVal,key,val...,
	// ColumnId[2],
	//      CountBitsetKeyVal,key,val...,
	// ColumnId[N],
	//      CountBitsetKeyVal,key,val...
	base, bitPosition := sparsebitset.OffsetBits(offset)
	//fmt.Println(base,bitPosition);
	sourcePosition := uint64(0)
	destPosition := uint64(0)
	dataLen := uint64(len(s.Data))
	columnFound := false
	columns = make([]int64,0,10)
	// making a new buffer as large like the worst scenario: we add a new column
	newBuffer := make([]byte, 0, dataLen+keyValStartPosition+keyLen+valLen)
	for dataLen > 0 && sourcePosition < dataLen {
		storedColumnId := binary.LittleEndian.Uint64(s.Data[sourcePosition+columnIdPosition:])
		columns = append(columns,int64(storedColumnId))
		keyValCount := binary.LittleEndian.Uint64(s.Data[sourcePosition+countKeyValPosition:])
		bytesToCopy := (keyValStartPosition + keyValCount*(keyLen+valLen))
		newBuffer = append(newBuffer, s.Data[sourcePosition:sourcePosition+bytesToCopy]...)
		if storedColumnId == uint64(columnId) {
			columnFound = true
			currentKeyValPosition := destPosition + keyValStartPosition
			baseFound := false
			for index := uint64(0); index < keyValCount; index++ {
				storedBase := binary.LittleEndian.Uint64(newBuffer[currentKeyValPosition:])
				currentKeyValPosition += keyLen
				if storedBase == base {
					storedBits := binary.LittleEndian.Uint64(newBuffer[currentKeyValPosition:])
					newBits := storedBits | (1 << bitPosition)
					binary.LittleEndian.PutUint64(newBuffer[currentKeyValPosition:], newBits)
					baseFound = true
					break
				}
				currentKeyValPosition += valLen
			}
			if !baseFound {
				keyValCount += 1
				newBuffer = append(newBuffer, make([]byte, valLen+keyLen)...)
				binary.LittleEndian.PutUint64(newBuffer[destPosition+countKeyValPosition:], keyValCount)
				binary.LittleEndian.PutUint64(newBuffer[destPosition+bytesToCopy:], base)
				destPosition += keyLen
				binary.LittleEndian.PutUint64(newBuffer[destPosition+bytesToCopy:], (1 << bitPosition))
				destPosition += valLen
			}
		}
		sourcePosition += bytesToCopy
		destPosition += bytesToCopy
	}
	if !columnFound {
		newBuffer = append(newBuffer, make([]byte, keyValStartPosition+keyLen+valLen)...)
		binary.LittleEndian.PutUint64(newBuffer[destPosition+columnIdPosition:], uint64(columnId))
		binary.LittleEndian.PutUint64(newBuffer[destPosition+countKeyValPosition:], uint64(1))
		binary.LittleEndian.PutUint64(newBuffer[destPosition+keyValStartPosition:], base)
		binary.LittleEndian.PutUint64(newBuffer[destPosition+keyValStartPosition+keyLen:], 1<<bitPosition)
		columns = append(columns,columnId)
	}
	s.Data = newBuffer
	//ioutil.WriteFile("./block",newBuffer,700)
	return
}

