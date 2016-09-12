package bitsetservice

import (
	"fmt"
	"time"
	"os"
	"math/rand"
	"github.com/js-ojus/sparsebitset"
)

func main212() {
	//bs:= bit.NewSparseSet(1000000)
	startt := time.Now()

	bs := sparsebitset.New(0)
	//start := uint64(500000000)// uint64(0x00000000FFFFFFFFFFFFFFFF)
	start := uint64(20000)// uint64(0x00000000FFFFFFFFFFFFFFFF)
	//bs.Set(b)
	//arr:=make([]uint64,start+1)
	value := uint64(0)
	for b := uint64(0); b <= start; b++ {
		value = uint64(rand.Int63())
		bs.Set(value)
		//fmt.Println(value)
		//arr[b] = value
		/*for i:=uint64(start);i>b;i-- {


		/*for i:=uint64(start);i>b;i-- {
			if !bs.Test(i) {
				fmt.Println(b)
				return
			}
		}*/
	}
	fmt.Println(time.Since(startt),value)

	fmt.Println("------",bs.Cardinality())
	startt = time.Now()
	/*11for pos, e :=bs.NextSet(0);e; pos,e = bs.NextSet(pos+1){
		fmt.Println(pos)
	}*/
	file, _ := os.Create("c:/1/bs.data")
	bs.WriteTo(file)
	fmt.Println(time.Since(startt))
	rt := sparsebitset.New(0)
	rd,_:= os.Open("c:/1/bs.data")
	rt.ReadFrom(rd)
	file1, _ := os.Create("c:/1/bs2.data")
	rt.WriteTo(file1)
	fmt.Println(rt.Cardinality())

	/*for a,v := range(arr){
		fmt.Println(a,v,bs.Test(v))
	}*/
}

