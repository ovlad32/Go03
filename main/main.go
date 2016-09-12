package main

import (
	"fmt"

	"../metadata"
	"../bitsetservice"
	"sync"
	"runtime"
	"time"
	"../sparsebitset"
	"../sb2"
	"os"
	"hash"
	"hash/fnv"
	"unsafe"
	//"strconv"
	"strconv"
	"math/rand"
	_ "net/http/pprof"
	/*"log"
	"net/http"
	"github.com/pkg/profile"*/
	"github.com/pkg/profile"
)


var wg sync.WaitGroup
//var hs hash.Hash64;

//var match uint32


func m1ain() {
	/*go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()*/
	defer profile.Start(profile.CPUProfile).Stop()
	//fmt.Println(*(*[8]byte)(unsafe.Pointer(&a)))

	//fmt.Println(sb2.Offset64Bits(256+64+1))
	//return
	var h hash.Hash64
	h = fnv.New64a()
	start0 := time.Now()
	start := start0
	s1 := sb2.New(0)
	r := rand.NewSource(30)
	_=uint64(r.Int63())
	_=h
	/*s1.Set(0)
	s1.Set(15)
	s1.Set(16)
	s1.Set(65535)
	s1.Set(65536)
	s1.Set(8388608)
	s1.Set(8388608+1)*/
	//return;
	//f,_ := os.Create("cprof")
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()
	for i := uint64(0); i <= 64; i+=16 {
		h.Reset()
		/*b:=(*[8]byte)(unsafe.Pointer(&i))
		fmt.Println((*b),i)
		h.Write((*b)[:])*/
		//		h.Write([]byte(fmt.Sprintf("%v",i)))
		h.Write([]byte(strconv.FormatUint(i, 10)))
		//h.Sum64()
//s1.Set(h.Sum64())
		s1.Set(	i)
	}
	fmt.Println("Build BS1 :", time.Since(start))
	//fmt.Println(" Cardinality ",s1.Cardinality())
	return;
}

/*
func buildTableBitSets(in chan table) {
	for table := range(in) {
		table.
	}
}
*/


func mai1n() {
	/*a:= sparsebitset.New(0)
	a.Set(1)
	a.Set(10)
	a.Set(100)
	a.Set(105)
	for ai := range a.BitChan(){
		fmt.Println(ai)
	}
	filea,_ := os.Create("c:/1/1.bs")
	//a.WriteTo(filea)
	fmt.Println("---")
	b := sparsebitset.New(0);
	fileb,_ := os.Open("c:/1/1.bs")
	//b.ReadFrom(fileb)
	for bi := range b.BitChan(){
		fmt.Println(bi)
	}
	fmt.Println("------")
	hro := bitsetservice.HRO{
		Hash:uint64(1),
		RowNumber:uint32(2),
		FileOffset:uint64(3),
	}
	hro.WriteTo(filea)
	hro = bitsetservice.HRO{
		Hash:uint64(9073077961759281772),
		RowNumber:uint32(49786),
		FileOffset:uint64(0),
	}

	hro.WriteTo(filea)
	hro = bitsetservice.HRO{
		Hash:uint64(9073077961759281772),
		RowNumber:uint32(49786),
		FileOffset:uint64(0),
	}
	hro.WriteTo(filea)
	filea.Close()

	hro2 := bitsetservice.HRO{}
	hro2.ReadFrom(fileb)
	fmt.Println(hro2)
	hro2.ReadFrom(fileb)
	fmt.Println(hro2)
	hro2.ReadFrom(fileb)
	fmt.Println(hro2)
	fileb.Close() */
}


func mainzz()  {
	a:=uint64(0Xffff)
	fmt.Println(*(*[8]byte)(unsafe.Pointer(&a)))
	//return
	var h hash.Hash64
	h = fnv.New64a()
	start0 := time.Now()
	start := start0
	s1 := sparsebitset.New(0)
	r := rand.NewSource(30)
	for i := uint64(0);i<100*1000*1000+0;i++{
		h.Reset()
		/*b:=(*[8]byte)(unsafe.Pointer(&i))
		fmt.Println((*b),i)
		h.Write((*b)[:])*/
//		h.Write([]byte(fmt.Sprintf("%v",i)))
		h.Write([]byte(strconv.FormatUint(i,10)))
		h.Sum64()
		s1.Set(/**/uint64(r.Int63()))
	}
	fmt.Println("Build BS1 :",time.Since(start))
	//fmt.Println(" Cardinality ",s1.Cardinality())
	return;
	start = time.Now()
	fileS1,_:= os.Create("S1_500M")
	//s1.WriteTo(fileS1)
	//fmt.Println("Write BS1:",time.Since(start))
	fileS1.Close()

	s2 := sparsebitset.New(0)
	for i := uint64(0);i<500000000*0;i++{
		//h.Reset()
		//h.Write([]byte(strconv.FormatUint(i,10)))
//		h.Write([]byte(fmt.Sprintf("%v",i)))
		//s2.Set(h.Sum64())
		s2.Set(i)
	}
	fmt.Println("Build BS2 :",time.Since(start)," Cardinality ",s2.Cardinality())

	start = time.Now()
	fileS2,_:= os.Create("S2_500M")
	//s2.WriteTo(fileS2)
	//fmt.Println("Write BS2:",time.Since(start))
	fileS2.Close()
	start = time.Now()
	result := s1.Intersection(s2)
	fmt.Println("Intersection:", result.Cardinality(),time.Since(start))
	start = time.Now()
	fileS3,_:= os.Create("IS_500M")
	//result .WriteTo(fileS3)
	//fmt.Println("Write Intersection:",time.Since(start))
	fileS3.Close()
	fmt.Println("total:",time.Since(start0))
}

func main() {
	start := time.Now()
	bs := &bitsetservice.BitsetServiceConfig{
		DumpRootPath:"C:/home/",
		BSRootPath: "C:/home/BS/",
		HRORootPath:"C:/home/BS/",
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	//runtime.GOMAXPROCS(1)

	tableChannel := make(chan * metadata.TableInfo)
	doneBitSets := make(chan bool)

	go bitsetservice.TableBitSetProcessor(
		tableChannel,
		doneBitSets,
		bs,

	)
	tables := metadata.GetMetadataInfoHC()
	for n := range tables {
		//metadata.CreateDumpTables(&t)
		tableChannel <- &tables[n]
	}
	close(tableChannel)
	<- doneBitSets

	table1 := tables[0]
	table2 := tables[1]
	toJoinData := make(chan *bitsetservice.Pair)
	toJoinRows := make(chan *bitsetservice.Pair)
	doneJoinRows := make(chan bool)
	go bitsetservice.JoinDataBitSetProcessor(toJoinData,toJoinRows,bs)
	go bitsetservice.JoinRowBitSetProcessor(toJoinRows,doneJoinRows,bs)
	for n1,_ := range table1.Columns {
		for n2,_ := range table2.Columns {
			pair := &bitsetservice.Pair{
				BSConfig:bs,
				LeftColumnToRowBitSet: bitsetservice.ColumnToBitSet{Column:&table1.Columns[n1]},
				RightColumnToRowBitSet:	bitsetservice.ColumnToBitSet{Column:&table2.Columns[n2]},
			}
			pair.TruePositive =
				(pair.LeftColumnToRowBitSet.Column.ColumnName.String == "CONTRACT_ID" &&
			   		pair.RightColumnToRowBitSet.Column.ColumnName.String == "INFORMER_DEAL_ID") ||
				(pair.LeftColumnToRowBitSet.Column.ColumnName.String == "CONTRACT_DATE" &&
					pair.RightColumnToRowBitSet.Column.ColumnName.String == "LIABILITY_DATE") ||
				(pair.LeftColumnToRowBitSet.Column.ColumnName.String == "CURRENCY" &&
					pair.RightColumnToRowBitSet.Column.ColumnName.String == "CURRENCY") ||
				(pair.LeftColumnToRowBitSet.Column.ColumnName.String == "COLLATERAL_AMOUNT" &&
					pair.RightColumnToRowBitSet.Column.ColumnName.String == "COLLATERAL_VALUE") ||
				(pair.LeftColumnToRowBitSet.Column.ColumnName.String == "CONTRACT_NUMBER" &&
					pair.RightColumnToRowBitSet.Column.ColumnName.String == "LIABILITY_NUMBER") ||
				(pair.LeftColumnToRowBitSet.Column.ColumnName.String == "CURRENCY" &&
						pair.RightColumnToRowBitSet.Column.ColumnName.String == "COLLATERAL_VALUE_CURRENCY") ||
				(pair.LeftColumnToRowBitSet.Column.ColumnName.String == "INITIAL_AMOUNT" &&
					pair.RightColumnToRowBitSet.Column.ColumnName.String == "ORIGINAL_AMOUNT") ||
				(pair.LeftColumnToRowBitSet.Column.ColumnName.String == "CLOSING_DATE" &&
					pair.RightColumnToRowBitSet.Column.ColumnName.String == "DUE_DATE")
			toJoinData <- pair
		}
	}
	close(toJoinData)
	<- doneJoinRows
		fmt.Println(time.Since(start))

}
