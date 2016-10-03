package sb2

import (
//"sync"
//"fmt"
)
import (
	//"fmt"
	"fmt"
	"sync"
)

const (
	// Size of a word -- `uint64` -- in bits.
	wordSize = uint64(64)

	// modWordSize is (`wordSize` - 1).
	modWordSize = wordSize - 1

	// Number of bits to right-shift by, to divide by wordSize.
	log2WordSize = uint64(6)

	// allOnes is a word with all bits set to `1`.
	allOnes uint64 = 0xffffffffffffffff

	subWordSize        = uint64(16)
	modSubWordSize     = subWordSize - 1
	log2SubWordSize    = uint64(4)
	stampSegmentIdSize = byte(2) // used both: 2 bits for Id and 2 bytes for the chunk word
	stampSegmentIdMask = byte(3) // 00000011b
)

const (
	slot5IdPosition     = (0)
	stampHeaderPosition = (1)
)

/*
func get64Shift(n uint64) (uint64) {
	return n & modWordSize
}
func get16Shift(n uint16) (uint16) {
	return  n & modSubWordSize
}
func split64To2x32(n uint64) (uint32, uint64) {
	return uint32(n >> 32), n & 0xFFFFFFFF
}
//uint32(log2SubWordSize)
func split32To2x16(offset uint32) (uint16, uint16)  {
	return uint16(offset >> 16), uint16(offset & 0xFFFF)
}*/
/*
type stamp64Type struct {
	pos  uint16
	bits uint64
}

type stampType struct {
	header uint8
	shift uint16
	chunks [3] uint16
}
*/

type stampConfigType struct {
	chunkCount  uint8
	sizeInBytes uint8
	expander    []byte
}

/*
type slotType struct {
	buffer map[uint16]map[uint16]map[uint16]map[uint8]*[]byte
	accessing sync.Mutex
}*/

var conf4 = stampConfigType{chunkCount: 4, sizeInBytes: 1 + 8}
var conf1 = stampConfigType{chunkCount: 1, sizeInBytes: 1 + 1 + 2}
var conf2 = stampConfigType{chunkCount: 2, sizeInBytes: 1 + 1 + 4}
var conf3 = stampConfigType{chunkCount: 3, sizeInBytes: 1 + 1 + 6}
var backwardConfigs = [4]*stampConfigType{&conf4, &conf3, &conf2, &conf1}
var forwardConfigs = [4]*stampConfigType{&conf1, &conf2, &conf3, &conf4}

type slotStackType map[uint8]map[uint16]map[uint16]map[uint16]map[uint8][]byte

type SB2 struct {
	accessing sync.Mutex
	slot      slotStackType
}

func New(n uint) *SB2 {
	for index := range forwardConfigs {
		forwardConfigs[index].expander = make([]byte,
			forwardConfigs[index].sizeInBytes,
			forwardConfigs[index].sizeInBytes,
		)
	}
	return &SB2{
		slot: make(slotStackType),
	}

}

/*

func convertShift64toChunk(shift64 uint64) (uint8, uint16) {
	if shift64 == 0 {
		return 0,0
	}
	for chunkIndex := uint16(0); chunkIndex <= uint16(log2SubWordSize); chunkIndex++ {
		position16 := uint16(uint64(modSubWordSize) & (shift64>>(chunkIndex*uint16(log2SubWordSize))))

		if position16 > 0 {
			if chunkIndex>0 {
				position16 -= 1
			}
			return uint8(chunkIndex), position16
		}
	}
	panic(fmt.Sprintf("Conversion of 64bit shift failed %v", shift64))
	return 0,0
}*/

func Offset64Bits(n uint64) (
	slot1Id uint16,
	slot2Id uint16,
	slot3Id uint16,
	slot4Id uint8,
	slot5Id uint8,
	segmentId byte,
	segmentValueHigh byte,
	segmentValueLow byte,
) {
	offset64 := n >> log2WordSize
	shift64 := n & modWordSize

	offset32Hi := offset64 >> 32 //modSubWordSize
	offset32Low := offset64 & 0xFFFFFFFF

	slot1Id = uint16(offset32Hi >> 16)
	slot2Id = uint16(offset32Hi & 0xFFFF)
	slot3Id = uint16(offset32Low >> 16)
	slot4Id = uint8(offset32Low>>8) & 0xFF
	slot5Id = uint8(offset32Low & 0xFF)

	segmentId = byte(shift64>>log2SubWordSize) + 1
	segmentValue := 1 << uint16(shift64&modSubWordSize)
	segmentValueHigh = byte(segmentValue >> 8)
	segmentValueLow = byte(segmentValue & 0xFF)
	return
}

func (b *SB2) Set(data uint64) {
	type searchResult struct {
		config   *stampConfigType
		offset16 uint16
		stamp    uintptr
		found    bool
	}

	slot1Id, slot2Id, slot3Id, slot4Id, slot5Id,
		segmentId, segmentValueHigh, segmentValueLow := Offset64Bits(data)
	//fmt.Println(Offset64Bits(data))
	//var resultValue *searchResult
	//resultChannel := make(map[slotIndexType]chan *searchResult);
	//findWithChannel := func(offset32i uint32, conf *stampConfigType, result chan <- *searchResult) {
	//	r := &searchResult{}
	//		r.offset16, r.stamp, r.found = b.findStamp(offset32i,conf)
	//		result <- r
	//	}
	//	for _,conf := range backwardConfigs {
	//		if b.routes[conf.index] != nil {
	////			resultChannel[conf.index] = make(chan *searchResult)
	//			go findWithChannel(offset32, conf, resultChannel[conf.index])
	////		}
	//	}
	getBuffer := func(conf *stampConfigType, toMake bool) (buff []byte, found bool) {
		var slot0 map[uint16]map[uint16]map[uint16]map[uint8][]byte
		var slot1 map[uint16]map[uint16]map[uint8][]byte
		var slot2 map[uint16]map[uint8][]byte
		var slot3 map[uint8][]byte
		buff = nil
		found = false

		if slot0, found = b.slot[conf.chunkCount]; !found {
			if !toMake {
				return
			}
			slot0 = make(map[uint16]map[uint16]map[uint16]map[uint8][]byte)
			b.slot[conf.chunkCount] = slot0
		}
		if slot1, found = slot0[slot1Id]; !found {
			if !toMake {
				return

			}
			slot1 = make(map[uint16]map[uint16]map[uint8][]byte)
			slot0[slot1Id] = slot1
		}
		if slot2, found = slot1[slot2Id]; !found {
			if !toMake {
				return
			}
			slot2 = make(map[uint16]map[uint8][]byte)
			slot1[slot2Id] = slot2
		}

		if slot3, found = slot2[slot3Id]; !found {
			if !toMake {
				return
			}
			slot3 = make(map[uint8][]byte)
			slot2[slot3Id] = slot3
		}
		if buff, found = slot3[slot4Id]; !found {
			if !toMake {
				return
			}

			found = true
			buff = make([]byte, 0, conf.sizeInBytes*100)
			slot3[slot4Id] = buff
		}
		return
	}

	{
		//var foundSlot *slotStackType
		var foundBuffer *[]byte
		var foundBufferOffset int = -1
		var foundStampConfig *stampConfigType
	over:
		for index, conf := range forwardConfigs {
			//			b.accessing.Lock()
			if buff, found := getBuffer(forwardConfigs[index], false); found {
				bufferSize := len(buff)
				if bufferSize > 0 {
					for bufferOffset := 0; bufferOffset < bufferSize; bufferOffset += int(conf.sizeInBytes) {
						if slot5Id == (buff)[bufferOffset] {
							foundBuffer = &buff
							foundBufferOffset = bufferOffset
							foundStampConfig = forwardConfigs[index]
							break over
						}

					}
				}

			}
		}

		if foundStampConfig == nil {
			foundStampConfig = &conf1
			b, _ := getBuffer(foundStampConfig, true)
			foundBuffer = &b
			foundBufferOffset = -1
		}

		if foundBufferOffset == -1 {
			(*foundBuffer) = append(*foundBuffer, slot5Id, segmentId, segmentValueHigh, segmentValueLow)
			return
		}

		if foundStampConfig.chunkCount == conf4.chunkCount {
			//fmt.Println(" -- ",segmentId,segmentValueHigh,segmentValueLow)
			//value := 1 << shift64
			//fmt.Println(foundBuffer)
			(*foundBuffer)[foundBufferOffset+int(2*(segmentId-1))+1] |= segmentValueHigh
			(*foundBuffer)[foundBufferOffset+int(2*(segmentId-1))+2] |= segmentValueLow
			/*for bIndex := uint64(0); bIndex < 64; bIndex +=8 {
				bValue := byte((value >> bIndex) & 0xFF)
				if bValue > 0 {
					//fmt.Println(foundBufferOffset,bIndex/4,len(foundSlot.buffer))
					foundSlot.buffer[uint64(foundBufferOffset) + stampIdLowPosition + bIndex/4] |= bValue
				}
			}*/
			fmt.Println(foundBuffer)
			return
		}

		segmentIdFound := false
		segmentOrder := byte(0)
		stampHeader := (*foundBuffer)[foundBufferOffset+stampHeaderPosition]

		for ; segmentOrder < byte(foundStampConfig.chunkCount); segmentOrder++ {
			// see stampSegmentIdSize declaration comment
			segmentIndex := byte(stampSegmentIdSize * segmentOrder)
			foundSegmentId := ((stampHeader >> segmentIndex) & stampSegmentIdMask)
			segmentIdFound = segmentId == foundSegmentId

			if segmentIdFound {
				offset :=
					foundBufferOffset +
						stampHeaderPosition +
						int(segmentIndex)
				(*foundBuffer)[offset+1] |= segmentValueHigh
				(*foundBuffer)[offset+2] |= segmentValueLow
				return
			}
		}

		nextConfig := forwardConfigs[foundStampConfig.chunkCount-1+1]
		nb, _ := getBuffer(nextConfig, true)
		nextBuffer := &nb
		nextBufferOffset := len(*nextBuffer)
		(*nextBuffer) = append(*nextBuffer, nextConfig.expander...)

		if nextConfig.chunkCount == conf4.chunkCount {
			(*nextBuffer)[nextBufferOffset] = slot5Id
			header := (*foundBuffer)[foundBufferOffset+stampHeaderPosition]
			//fmt.Println(nextBuffer)
			for index := byte(0); index < foundStampConfig.chunkCount; index++ {
				foundSegmentId := (header >> (stampSegmentIdSize * index)) & stampSegmentIdMask
				nextSegmentBufferIndex := nextBufferOffset + int(foundSegmentId-1)*2
				foundSegmentBufferIndex := foundBufferOffset + stampHeaderPosition + int(index*2)
				(*nextBuffer)[nextSegmentBufferIndex+1] =
					(*foundBuffer)[foundSegmentBufferIndex+1]
				(*nextBuffer)[nextSegmentBufferIndex+2] =
					(*foundBuffer)[foundSegmentBufferIndex+2]

			}
			(*nextBuffer)[nextBufferOffset+int(segmentId-1)*2+1] = segmentValueHigh
			(*nextBuffer)[nextBufferOffset+int(segmentId-1)*2+2] = segmentValueLow
			//fmt.Println(nextBuffer)
		} else {
			nextStart := nextBufferOffset
			nextFinish := nextStart + int(foundStampConfig.sizeInBytes)
			foundStart := foundBufferOffset
			foundFinish := foundBufferOffset + int(foundStampConfig.sizeInBytes)
			copy((*nextBuffer)[nextStart:nextFinish], (*foundBuffer)[foundStart:foundFinish])

			/*
				nextSlot.buffer[nextBufferOffset + stampIdHighPosition] = stampIdHigh;
				nextSlot.buffer[nextBufferOffset + stampIdLowPosition] = stampIdLow;

				nextSlot.buffer[nextBufferOffset + stampHeaderPosition] =
					foundSlot.buffer[foundBufferOffset + stampHeaderPosition] |
						segmentId << ((nextConfig.chunkCount -1) * stampSegmentIdSize)

				for index := uint64(0); index < 2 * uint64(foundStampConfig.chunkCount); index ++ {
					nextSlot.buffer[index + nextBufferOffset + stampHeaderPosition + 1] =
						foundSlot.buffer[index + foundBufferOffset + stampHeaderPosition + 1]
				}
				nextSlot.buffer[nextBufferOffset + stampHeaderPosition + uint64(segmentId -1) * 2 + 1] = segmentValueHigh
				nextSlot.buffer[nextBufferOffset + stampHeaderPosition + uint64(segmentId -1) * 2 + 2] = segmentValueLow
			*/
			(*nextBuffer)[nextBufferOffset+stampHeaderPosition] =
				(*foundBuffer)[foundBufferOffset+stampHeaderPosition] |
					segmentId<<((nextConfig.chunkCount-1)*stampSegmentIdSize)
			(*nextBuffer)[nextFinish+0] = segmentValueHigh
			(*nextBuffer)[nextFinish+1] = segmentValueLow
		}
		// Decreasing the buffer by copying the top stamp
		// and truncating the buffer by size of stamp
		topStampOffset := len(*foundBuffer) - int(foundStampConfig.sizeInBytes)
		if topStampOffset > 0 {
			//fmt.Println(topStampOffset,foundBufferOffset)
		}
		/*if topStampOffset >= int(foundBufferOffset) {
			for index := int(0); index < int(foundStampConfig.sizeInBytes); index ++ {
				(*foundBuffer)[foundBufferOffset + index] =
					(*foundBuffer)[int(topStampOffset) + index]
				(*foundBuffer)[int(topStampOffset) + index] = 0
			}
		}*/
		(*foundBuffer) = (*foundBuffer)[0:topStampOffset]
	}
}

//fmt.Println(" -- ",foundStampConfig.chunkCount, " ",nextConfig.chunkCount, " ", len(foundSlot.buffer), cap(foundSlot.buffer))

//}
/*	} else {
			var slotRouter *slotRouterType
			var found bool
//			b.accessing.Lock()
			if slotRouter, found = b.routes[conf1.chunkCount];!found {
			//	b.accessing.Unlock()
				slotRouterLocal := &slotRouterType{
					slot:make(map[uint32]*slotType),
				}
			//	b.accessing.Lock()
				b.routes[conf1.chunkCount] = slotRouterLocal
			//	b.accessing.Unlock()
				slotRouter = slotRouterLocal
			} else {
			//	b.accessing.Unlock()
			}
			var slot *slotType
//			slotRouter.accessing.Lock()
			bufferOffset := uint64(0);
			if slot,found = (*slotRouter).slot[slotId];!found {
//				slotRouter.accessing.Unlock()
				slot = &slotType{
					buffer:make([]byte,conf1.sizeInBytes,uint(conf1.sizeInBytes*100)),
				}
//				(*slotRouter).accessing.Lock()
				(*slotRouter).slot[slotId] = slot
//				(*slotRouter).accessing.Unlock()
			} else {
//				slotRouter.accessing.Unlock()
				bufferOffset = uint64(len(slot.buffer))
				slot.buffer = append(slot.buffer,conf1.expander...)

			}
			slot.buffer[bufferOffset + stampIdHighPosition] = stampIdHigh
			slot.buffer[bufferOffset + stampIdLowPosition] = stampIdLow
			slot.buffer[bufferOffset + stampHeaderPosition] = segmentId
			slot.buffer[bufferOffset + stampHeaderPosition + 1] = segmentValueHigh
			slot.buffer[bufferOffset + stampHeaderPosition + 2] = segmentValueLow
		}
	}
}
*/
