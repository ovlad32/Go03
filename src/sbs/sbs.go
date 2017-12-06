package sbs

import (
	"io"
	"encoding/binary"
	"context"
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

	// Density of bits, expressed as a fraction of the total space.
	bitDensity = 0.1
)

var deBruijn = [...]byte{
	0, 1, 56, 2, 57, 49, 28, 3, 61, 58, 42, 50, 38, 29, 17, 4,
	62, 47, 59, 36, 45, 43, 51, 22, 53, 39, 33, 30, 24, 18, 12, 5,
	63, 55, 48, 27, 60, 41, 37, 16, 46, 35, 44, 21, 52, 32, 23, 11,
	54, 26, 40, 15, 34, 20, 31, 10, 25, 14, 19, 9, 13, 8, 7, 6,
}

type SparseBitsetType struct {
	bases []uint64
	bits []uint64
	blockExpansionSize int
}

func New() (result *SparseBitsetType) {
  return NewWithSize(4*1024);
}

func NewWithSize(blocks int) (result *SparseBitsetType) {
    result = new(SparseBitsetType);
    result.blockExpansionSize = blocks;

	result.bases = make([]uint64,0,blocks);
	result.bits = make([]uint64,0,blocks);

	return
}

func (s *SparseBitsetType) insert(index int) {
	if len(s.bases) == cap(s.bases) {
		bases := make([]uint64, cap(s.bases)+1, cap(s.bases)+s.blockExpansionSize);
		copy(bases, s.bases[0:index]);
		copy(bases[index+1:], s.bases[index:]);
		s.bases = bases;

		bits := make([]uint64, cap(s.bits)+1, cap(s.bits)+s.blockExpansionSize);
		copy(bits, s.bits[0:index]);
		copy(bits[index+1:], s.bits[index:]);
		s.bits = bits;
	} else {
		s.bases = append(s.bases, 0);
		copy(s.bases[index+1:], s.bases[index:]);
		s.bases[index] = 0

		s.bits = append(s.bits, 0);
		copy(s.bits[index+1:], s.bits[index:]);
		s.bits[index] = 0
	}

}


func Split(n uint64) (uint64, uint64) {
	return (n >> log2WordSize), (n & modWordSize)
}

func trailingZeroes64(v uint64) uint64 {
	return uint64(deBruijn[((v&-v)*0x03f79d71b4ca8b09)>>58])
}

// popcount answers the number of bits set to `1` in this word.  It
// uses the bit population count (Hamming Weight) logic taken from
// https://code.google.com/p/go/issues/detail?id=4988#c11.  Original
// by 'https://code.google.com/u/arnehormann/'.
func popcount(x uint64) (n uint64) {
	x -= (x >> 1) & 0x5555555555555555
	x = (x>>2)&0x3333333333333333 + x&0x3333333333333333
	x += x >> 4
	x &= 0x0f0f0f0f0f0f0f0f
	x *= 0x0101010101010101
	return x >> 56
}

// Cardinality answers the number of bits set to `1` in this set.
func(s *SparseBitsetType) Cardinality() uint64{
	cardinality := uint64(0)
	for _, aValue := range s.bits {
		cardinality += popcount(aValue)
	}
	return cardinality
}

func (s* SparseBitsetType) index(base uint64,makeNew bool) (index int) {
	if len(s.bases) == 0 {
		if makeNew {
			s.insert(0);
			return 0;
		} else {
			return -1;
		}
	}

	top := len(s.bases);
	bottom := 0;
	if s.bases[top - 1] < base {
		if makeNew {
			s.insert(top)
			return top
		} else {
			return -1;
		}
	}
	if s.bases[bottom] > base {
		if makeNew {
			s.insert(0)
			return 0
		} else {
			return -1;
		}
	}
	delta := top - bottom;
	for delta > 0{
		index = bottom + int(delta/2)
		if s.bases[index]  == base {
			return index;
		} else if s.bases[index] > base {
			top = index
		} else {
			bottom = index + 1;
		}
		delta = top - bottom;
	}
	if makeNew {
		if s.bases[index]<base {
			index ++
		}
		s.insert(index)
		return index
	} else {
		return -1;
	}
}

func (b *SparseBitsetType) Len() int {
	return len(b.bases)
}

func (s *SparseBitsetType) SetValue(n uint64) (wasSet bool) {
	base, bit := Split(n);
	index := s.index(base,true);
	s.bases[index] = base;
	prevValue := s.bits[index];
	newValue := uint64(1) << bit
	s.bits[index] = prevValue | newValue;
	wasSet = (prevValue & newValue) > 0
	return
}


func (s *SparseBitsetType) ReadFrom(ctx context.Context, r io.Reader) (err error) {

	// Read length of the data that follows.
	var lb uint32
	err = binary.Read(r, binary.BigEndian, &lb)
	if err != nil {
		return fmt.Errorf("reading bitset [header] data: %v",err)
	}

	n := int(lb) / (2 * binary.Size(uint64(0)))
	s.bases = make([]uint64,0,int(lb))
	s.bits = make([]uint64,0,int(lb))
	for i := 0; i < n; i++ {
		var base, bits uint64
		select {
		case <-ctx.Done():
				return nil
		default:
			err = binary.Read(r, binary.BigEndian, &base)
			if err != nil {
				return fmt.Errorf("reading bitset [base] data: %v",err)
			}
			err = binary.Read(r, binary.BigEndian, &bits)
			if err != nil {
				return fmt.Errorf("reading bitset [bits] data: %v", err)
			}
			s.bases = append(s.bases,base)
			s.bits = append(s.bits,bits)
		}
	}

	return
}

func (s *SparseBitsetType) WriteTo(ctx context.Context, w io.Writer) (err error) {
	// Write length of the data to follow.
	//b.prune()

	lb := len(s.bases)
	lb *= 2 * binary.Size(uint64(0))
	err = binary.Write(w, binary.BigEndian, uint32(lb))
	if err != nil {
		return fmt.Errorf("writing bitset [header] data: %v",err)
	}

	for index, base := range s.bases {
		select {
		case <-ctx.Done():
				return nil
		default:
			err = binary.Write(w, binary.BigEndian, base)
			if err != nil {
				return fmt.Errorf("writing bitset [base] data: %v",err)
			}
			err = binary.Write(w, binary.BigEndian, s.bits[index])
			if err != nil {
				return fmt.Errorf("writing bitset [bits] data: %v", err)
			}
		}
	}

	return
}

func (s *SparseBitsetType) Intersection(c *SparseBitsetType) (result*SparseBitsetType, cardinality uint64){
	if c == nil {
		return nil,0
	}

	result = NewWithSize(s.blockExpansionSize);
	cardinality = 0;

	for sIndex, base := range s.bases {
		if cIndex := c.index(base,false); cIndex >= 0 {
			if resultBits := s.bits[sIndex] & c.bits[cIndex]; resultBits > 0 {
				result.bases = append(result.bases,base);
				result.bits = append(result.bits,resultBits);
				cardinality += popcount(resultBits)
			}
		}
	}

	return

}


func (s *SparseBitsetType) BitChan(ctx context.Context) chan uint64 {
	var wg sync.WaitGroup

	out := make(chan uint64)
	wg.Add(1)
	go func() {
		wg.Wait()
		close(out)
	}()

	go func() {
		for index, base := range s.bases {
			bits := s.bits[index]
			prod := base * wordSize
			rsh := uint64(0)
			prev := uint64(0)
			for {
				w := bits >> rsh
				if w == 0 {
					break
				}
				result := rsh + trailingZeroes64(w) + prod
				if result != prev {
					select {
					case out <- result:
					case <-ctx.Done():
						break
					}
					prev = result
				}
				rsh++
			}
		}
		wg.Done()
	}()
	return out
}
