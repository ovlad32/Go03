package sbs

import (
	"testing"
	"reflect"
)


func Test_Index(t *testing.T) {
   for _, test := range []struct{
   	value int
    index int
    bases []uint64
    } {
   		{0,0,[]uint64{0,1,3,13,24,35}},
   		{1,0,[]uint64{1,3,13,24,35}},
   		{3,1,[]uint64{1,3,13,24,35}},
   		{13,2,[]uint64{1,3,13,24,35}},
   		{14,3,[]uint64{1,3,13,0,24,35}},
   		{25,4,[]uint64{1,3,13,24,0,35}},
   		{33,4,[]uint64{1,3,13,24,0,35}},
   		{37,5,[]uint64{1,3,13,24,35,0}} } {
	   bitset := NewWithSize(5);
	   bitset.bases = append(bitset.bases, 1, 3, 13, 24, 35)
	   bitset.bits = append(bitset.bits, 1, 3, 13, 24, 35)
	   gotIndex := bitset.index(uint64(test.value));
	   if test.index != gotIndex {
	   	  t.Errorf("index mismatch: expected %v, got %v",test.index,gotIndex)
	   }
	   if !reflect.DeepEqual(bitset.bases,test.bases) {
		   t.Errorf("array mismatch: expected %v, got %v",test.bases,bitset.bases)
	   }
   }

}


func Test_Index3(t *testing.T) {
	bitset := NewWithSize(5);
	bitset.bases[bitset.index(7)] = 7
	bitset.bases[bitset.index(4)] = 4
	bitset.bases[bitset.index(2)] = 2
	bitset.bases[bitset.index(17)] = 17
	expected := []uint64{2,4,7,17}
	if !reflect.DeepEqual(bitset.bases,expected) {
		t.Errorf("array mismatch: expected %v, got %v",expected,bitset.bases)
	}

}