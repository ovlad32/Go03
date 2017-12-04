package sbs

import (
	"testing"
	"fmt"
)


func Test_Index(t *testing.T) {
   bitset := NewWithSize(5);
   bitset.bases = append(bitset.bases, 1,2,13,24,35)
   bitset.bits = append(bitset.bits, 1,2,13,24,35)
   bitset.index(4);
}

func Test_Index2(t *testing.T) {
	bitset := NewWithSize(5);
	bitset.bases = append(bitset.bases, 3,7,13,24,35)
	bitset.bits = append(bitset.bits, 3,7,13,24,35)
	fmt.Println("%v",bitset.bases)
	fmt.Println(bitset.index(4));
	fmt.Println("%v",bitset.bases)
	fmt.Println("%v",bitset.bits)
}
