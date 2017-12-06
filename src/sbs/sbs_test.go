package sbs

import (
	"testing"
	"reflect"
	"context"
)


func TestSparseBitsetType_Index0(t *testing.T) {
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
	   gotIndex := bitset.index(uint64(test.value),true);
	   if test.index != gotIndex {
	   	  t.Errorf("index mismatch: expected %v, got %v",test.index,gotIndex)
	   }
	   if !reflect.DeepEqual(bitset.bases,test.bases) {
		   t.Errorf("array mismatch: expected %v, got %v",test.bases,bitset.bases)
	   }
   }

}


func TestSparseBitsetType_Index1(t *testing.T) {
	bitset := NewWithSize(5);
	bitset.bases[bitset.index(7,true)] = 7
	bitset.bases[bitset.index(4,true)] = 4
	bitset.bases[bitset.index(2,true)] = 2
	bitset.bases[bitset.index(17,true)] = 17
	expected := []uint64{2,4,7,17}
	if !reflect.DeepEqual(bitset.bases,expected) {
		t.Errorf("array mismatch: expected %v, got %v",expected,bitset.bases)
	}

}
func TestSparseBitsetType_BitChan(t *testing.T) {
	s := NewWithSize(5);
	// has to be sorted
	data := []uint64{1,2,300,301,1000,1000000}
	for _, n := range data{
		s.SetValue(n)
	}
	index := 0;
	for n := range s.BitChan(context.Background()) {
		if n != data[index] {
			t.Errorf("stored data mismatch! expected:%v, got %v: ",data[index],n)
		}
		index ++;
	}
}

func TestSparseBitsetType_Intersection(t *testing.T) {
	s1 := NewWithSize(5);
	dataS1 := []uint64{1,2,300,3001,4000,1000000}
	for _, n := range dataS1{
		s1.SetValue(n)
	}

	s2 := NewWithSize(5);
	dataS2 := []uint64{5,13,300,3001,1000000}
	for _, n := range dataS2{
		s2.SetValue(n)
	}

	result := []uint64{300,3001,1000000}
	sResult,card := s1.Intersection(s2);
	if int(card) != len(result) {
		t.Errorf("intersect data length mismatch! expected:%v, got %v: ",len(result),card)
	}
	index := 0;
	for n := range sResult.BitChan(context.Background()) {
		if n != result[index] {
			t.Errorf("intersect data mismatch! expected:%v, got %v: ",result[index],n)
		}
		index ++;
	}


}