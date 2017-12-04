package sbs

import "fmt"

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
	blockSize int
}

func New() (result *SparseBitsetType) {
  return NewWithSize(1024);
}

func NewWithSize(blocks int) (result *SparseBitsetType) {
    result = new(SparseBitsetType);
    result.blockSize = blocks;

	result.bases = make([]uint64,0,blocks);
	result.bits = make([]uint64,0,blocks);

	return
}

func (s *SparseBitsetType) grow(index int) {
	bases := make([]uint64, cap(s.bases) + 1, cap(s.bases) + s.blockSize,);
	copy(bases,s.bases[0:index]);
	copy(bases[index+1:],s.bases[index:]);
	s.bases = bases;


	bits := make([]uint64, cap(s.bits) +1, cap(s.bits)+ s.blockSize);
	copy(bits,s.bits[0:index]);
	copy(bits[index+1:],s.bits[index:]);
	s.bits = bits;
}


func Split(n uint64) (uint64, uint64) {
	return (n >> log2WordSize), (n & modWordSize)
}

func trailingZeroes64(v uint64) uint64 {
	return uint64(deBruijn[((v&-v)*0x03f79d71b4ca8b09)>>58])
}

func (s* SparseBitsetType) index(base uint64) (index int) {
	if len(s.bases) == 0 {
		s.bases = append(s.bases, 0);
		s.bits = append(s.bits, 0);
		return 0;
	}

	top := len(s.bases)-1;
	bottom := 0;
	if s.bases[top]<base {
		s.grow(top)
		return top
	}
	if s.bases[bottom]> base {
		s.grow(0)
		return 0
	}
	delta := top - bottom;
	iteration := 10;
	for {
		iteration --;
		fmt.Printf("Top:%v; Bottom:%v Delta: %v",top,bottom,delta)
		if delta == 1 {
			index = bottom;
		} else {
			index = bottom + int(delta/2)
		}

		fmt.Printf(" Index: %v ",index)
		if s.bases[index]  == base {
			fmt.Printf(": match\n")
			return index;
		} else if s.bases[index] < base {
			bottom = index;
		} else {
			top = index
		}
		delta = top - bottom;
		if delta ==0 {
			fmt.Printf("\nGrow with Top:%v; Bottom:%v Delta: %v\n",top,bottom,delta)
			s.grow(index)
			return index
		}


		fmt.Printf(" \n")
		if iteration <0 {
			break;
		}
	}
	return -1;
}

func (s *SparseBitsetType) SetValue(n uint64) (wasSet bool){
	//base, bit := Split(n);

	return false;
}