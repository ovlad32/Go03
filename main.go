package main

import (
	"astra/dataflow"
	"astra/nullable"
	"context"
	"flag"
	"github.com/goinggo/tracelog"
	_ "github.com/npat-efault/crc16"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
	//	 _ "net/http/pprof"
	"sort"
	"astra/utils"
	//"github.com/couchbase/moss"

	"fmt"
	"astra/metadata"
)

//-workflow_id 57 -metadata_id 331 -cpuprofile cpu.prof.out

var packageName = "main"

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

var pathToConfigFile = flag.String("configfile", "./config.json", "path to config file")
var argMetadataIds = flag.String("metadata_id", string(-math.MaxInt64), "")
var argWorkflowIds = flag.String("workflow_id", string(-math.MaxInt64), "")

/*
func readConfig() (*dataflow.AstraConfigType, error) {
	funcName := "readConfig"
	if _, err := os.Stat(*pathToConfigFile); os.IsNotExist(err) {
		tracelog.Error(err, funcName, "Specify correct path to config.json")
		return nil, err
	}

	conf, err := os.Open(*pathToConfigFile)
	if err != nil {
		tracelog.Errorf(err, funcName, "Opening config file %v", *pathToConfigFile)
		return nil, err
	}
	jd := json.NewDecoder(conf)
	var result dataflow.AstraConfigType
	err = jd.Decode(&result)
	if err != nil {
		tracelog.Errorf(err, funcName, "Decoding config file %v", *pathToConfigFile)
		return nil, err
	}
	return &result, nil
}
*/


func main() {
	//	funcName := "main"
	flag.Parse()
	tracelog.Start(tracelog.LevelInfo)
	defer tracelog.Stop()
	/*
			b := sparsebitset.New(0);
			fmt.Println(b.Set(15));
			fmt.Println(b.Set(1501232123));
			for i:= uint64(0); i<0xFFFFFFFFFFFFFFFF; i++{
				if b.Test(i) {
					fmt.Println(i)
				}
			}
		return

			b := sparsebitset.New(0);
			fmt.Println(b.Set(15));
			fmt.Println(b.Test(5));
			fmt.Println(b.Clear(5));
			fmt.Println(b.Test(6));
			return
	*/
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		defer f.Close()
	}

	err := dataflow.Init()
	if err != nil {
		os.Exit(1)
	}

	//testBitsetBuilding()
	testBitsetCompare()

}

func testBitsetBuilding() (err error) {
	funcName := "testBitsetBuilding"
	metadataIds := make(map[int64]bool)
	workflowIds := make(map[int64]bool)

	if *argMetadataIds != string(-math.MaxInt64) {
		values := strings.Split(*argMetadataIds, ",")
		for _, value := range values {
			if value != "" {
				if iValue, cnvErr := strconv.ParseInt(value, 10, 64); cnvErr != nil {
					tracelog.Errorf(err, packageName, funcName, "Cannot convert %v to integer ", value)
				} else {
					metadataIds[iValue] = true
				}
			}
		}
	}

	if *argWorkflowIds != string(-math.MaxInt64) {
		values := strings.Split(*argWorkflowIds, ",")
		for _, value := range values {
			if value != "" {
				if iValue, cnvErr := strconv.ParseInt(value, 10, 64); cnvErr != nil {
					tracelog.Errorf(err, packageName, funcName, "Cannot convert %v to integer ", value)
				} else {
					workflowIds[iValue] = true
				}
			}
		}
	}
	tracelog.Started(packageName, funcName)

	dr, err := dataflow.NewInstance()

	tracelog.Started(packageName, funcName)
	start := time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())

	for id, _ := range workflowIds {
		metadataId1, metadataId2, err := dr.Repository.MetadataByWorkflowId(nullable.NewNullInt64(id))
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return err
		}
		if metadataId1.Valid() {
			metadataIds[metadataId1.Value()] = true
		}
		if metadataId2.Valid() {
			metadataIds[metadataId2.Value()] = true
		}
	}
	tablesToProcess := make([]*dataflow.TableInfoType, 0, 100)

	for id, _ := range metadataIds {
		meta, err := dr.Repository.MetadataById(nullable.NewNullInt64(id))
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return err
		}

		tables, err := dr.Repository.TableInfoByMetadata(meta)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return err
		}

		for _, table := range tables {
			//if table.String() == "CRA.LIABILITIES" {
			tablesToProcess = append(tablesToProcess, dataflow.ExpandFromMetadataTable(table))
			//}
		}
	}

	var processTableChan chan *dataflow.TableInfoType

	processTable := func(runContext context.Context) (err error) {
		funcName := "processTable"
		tracelog.Started(packageName, funcName)
	outer:
		for {
			select {
			case <-runContext.Done():
				break outer
			case inTable, open := <-processTableChan:
				if !open && inTable == nil {
					break outer
				}
				tracelog.Info(packageName, funcName, "Start processing table %v", inTable)

				err := dr.BuildHashBitset(runContext, inTable)
				if err != nil {
					return err
				}
				if err == nil {
					tracelog.Info(packageName, funcName, "Table processing %v has been done", inTable)
				}
			}
		}
		tracelog.Completed(packageName, funcName)
		return err
	}

	var wg sync.WaitGroup
	if len(tablesToProcess) > 0 {

		processTableChan = make(chan *dataflow.TableInfoType, dr.Config.TableWorkers)
		processTableContext, processTableContextCancelFunc := context.WithCancel(context.Background())
		for index := 0; index < dr.Config.TableWorkers; index++ {
			wg.Add(1)
			go func() {
				err = processTable(processTableContext)
				wg.Done()
				if err != nil {
					tracelog.Errorf(err, packageName, funcName, "Сancel сontext called ")
					if false {
						processTableContextCancelFunc()

					}

				}
				return
			}()
		}
		for _, table := range tablesToProcess {
			processTableChan <- table
		}
		close(processTableChan)
		wg.Wait()
		tracelog.Info(packageName, funcName, "All tables processed")
		//dr.CloseStores()
	}
	tracelog.Info(packageName, funcName, "Elapsed time: %v", time.Since(start))
	tracelog.Completed(packageName, funcName)
	return err
}




func testBitsetCompare() (err error) {
	funcName := "testBitsetCompare"
	ctx, ctxCancelFunc := context.WithCancel(context.Background())
	_ = ctxCancelFunc
	//var bruteForcePairCount = 0;
	actor,err  := dataflow.NewComplexKeyDiscovery(
			&dataflow.ComplexKeyDiscoveryConfigType{
				CollisionLevel:          1.2,
				NumberOfKeysPerFilePass: 5,
			},
			)


	if *argMetadataIds == string(-math.MaxInt64) {
		return err
	}

	metadataIds,err := utils.ParseToInt64UniqueArray(*argMetadataIds,",")


	tracelog.Started(packageName, funcName)




	tracelog.Started(packageName, funcName)
	start := time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())

	columnToProcess, err := actor.ExtractColumns(ctx,metadataIds)

	pairsFilteredByFeatures,err := actor.ComposeAndFilterColumnPairsByFeatures(ctx,columnToProcess)

	pairsFilteredByHash,err := actor.FilterColumnPairsByBitSets(ctx,pairsFilteredByFeatures)


/*	tracelog.Info(packageName, funcName,
		"Feature analysis efficiency: %v%%. Left %v from %v",
		100.0-math.Trunc(float64(len(pairsFilteredByFeatures))*100/float64(bruteForcePairCount)),
		len(pairsFilteredByFeatures),
			bruteForcePairCount,
		)


	tracelog.Info(packageName, funcName,
		"Content analysis efficiency: %v%%. Left %v from %v",
		100.0-math.Trunc(float64(len(pairsFilteredByContent))*100/float64(len(pairsFilteredByFeatures))),
		len(pairsFilteredByContent),
		len(pairsFilteredByFeatures),
	)

	for _, pair := range pairsFilteredByFeatures {
		pair.PKColumn.ResetBitset(dataflow.ItemBitsetSuffix)
		pair.FKColumn.ResetBitset(dataflow.ItemBitsetSuffix)
	}

	if false {
		for _, pair := range pairsFilteredByContent {
			fmt.Printf("PKColumn:%v(%v) - FKColumn:%v(%v)%v\n", pair.PKColumn, pair.PKColumn.HashUniqueCount, pair.FKColumn, pair.FKColumn.HashUniqueCount, pair.FKColumn.Id)
		}
	}

	tracelog.Info(packageName, funcName,
		"Hash analysis efficiency: %v%%. Left %v from %v",
		100.0-math.Trunc(float64(len(pairsFilteredByHash))*100/float64(len(pairsFilteredByContent))),
		len(pairsFilteredByHash),
		len(pairsFilteredByContent),
	)
	//	fmt.Println(bruteForcePairCount, len(pairsFilteredByHash), float64(len(pairsFilteredByHash))*100/float64(bruteForcePairCount))

	for _, pair := range pairsFilteredByContent {
		pair.PKColumn.ResetBitset(dataflow.HashBitsetSuffix)
		pair.FKColumn.ResetBitset(dataflow.HashBitsetSuffix)
	}

	if true {
		fmt.Printf("List of pairs after filtration:\n")
		for _, pair := range pairsFilteredByHash {
			fmt.Printf("PKColumn:%v(%v) - FKColumn:%v(%v)%v\n", pair.PKColumn, pair.PKColumn.HashUniqueCount, pair.FKColumn, pair.FKColumn.HashUniqueCount, pair.FKColumn.Id)
		}

	}*/

	//TODO: LOAD data here
	if false {
		columnMap := make(map[*dataflow.TableInfoType][]*dataflow.ColumnInfoType)
		appendToColumnMap := func(column *dataflow.ColumnInfoType) {
			if arr, found := columnMap[column.TableInfo]; !found {
				arr = make([]*dataflow.ColumnInfoType, 0, 5)
				arr = append(arr, column)
				columnMap[column.TableInfo] = arr
			} else {
				columnMap[column.TableInfo] = append(arr, column)
			}
		}

		for _, pair := range pairsFilteredByHash {
			appendToColumnMap(pair.ParentColumn)
		}

		for table, columns := range columnMap {
			_ = table

			if len(columns) == 1 {
				var integerUnique bool = true
				for _, dataCategory := range columns[0].Categories {
					if integerUnique = dataCategory.IsInteger.Value() && dataCategory.IsNumeric.Value(); !integerUnique {
						break
					}
				}
			}

			sort.Slice(columns, func(i, j int) bool {
				if columns[i].HashUniqueCount.Value() == columns[j].HashUniqueCount.Value() {
					return columns[i].Id.Value() > columns[j].Id.Value()
				}
				return columns[i].HashUniqueCount.Value() > columns[j].HashUniqueCount.Value()
			})

			for _, col := range columns {
				tracelog.Info(packageName, funcName, "%v(%v)", col, col.HashUniqueCount)
			}
			fmt.Println()
		}

		if true {
			//appendToColumnMap(pair.FKColumn)
			//columns := make([]*dataflow.ColumnInfoType,0)

		}

	}


		//TODO: Assume real data count doesn't differ to hash data count
	//	for _, pair := range pairsFilteredByHash {
	//		pair.PKColumn.UniqueRowCount = pair.PKColumn.HashUniqueCount
	//		pair.FKColumn.UniqueRowCount = pair.FKColumn.HashUniqueCount
	//	}

		 tablePairMap,err := actor.ComposeTablePairs(ctx, pairsFilteredByHash)
			if  err!= nil{

			}

			count := len(tablePairMap)
			if count == 0 {
				tracelog.Info(packageName, funcName, "There is no complex key candidates found!")
				return
			}



		//if false {
		//	for _, columnPairs := range tablePairMap {
		//		if len(columnPairs) > 1 {
		//			fmt.Printf("\nPKColumn:%v - FKColumn:%v:\n", columnPairs[0].PKColumn.TableInfo, columnPairs[0].FKColumn.TableInfo)
		//			for _, pair := range columnPairs {
		//				fmt.Printf("%v - %v -- %v\n", pair.PKColumn, pair.FKColumn, pair.TablePairKeyString())
		//			}
		//			fmt.Printf("++++++\n")
		//		}
		//
		//	}
		//	fmt.Printf("-------------------")
		//}
	parentColumnCombinations,err := actor.CollectColumnCombinationsByParentTable(ctx,tablePairMap)
	_ =parentColumnCombinations

	for parentTable,columnCombinationsMap := range parentColumnCombinations {
		for tablePair,columnPairArray := range tablePairMap {
			if tablePair.ParentTable == parentTable {
				for combinationKey, parentColumns := range columnCombinationsMap {
					_ = combinationKey
					parentAndChildColumns := make(map[*dataflow.ColumnInfoType][]*dataflow.ColumnInfoType)
					for _, parentColumn := range parentColumns.Columns {
						for _, columnPair := range columnPairArray {

						}
						if arr, found0 := parentAndChildColumns[parentColumn]; !found0 {
							arr = make([]*dataflow.ColumnInfoType, 0, 10);
							arr = append()
						}

					}
				}
			}

		}

	}

		/*
		 A,B,C,D ->  a,b,bb,c,cc,ccc,c,cc,ccc
		 A a, B b, C c
		 A a, B bb, C c
		 A a, B b, C, cc
		 A a, B bb, C, cc
		 A a, B b, C, ccc
		 A a, B bb, C, ccc
		 A a, B b, D c
		 A a, B bb, D c
		 A a, B b, D, cc
		 A a, B bb, D, cc
		 A a, B b, D, ccc
		 A a, B bb, D, ccc

		for table,columnCombinationMap := range tableCPKs {
			fkCombinations:= make(map[*dataflow.TableInfoType]map[int][]*dataflow.ColumnInfoType)

			for tablePair, _ := range tablePairMap {
				if table.Id.Value() != tablePair.PKTable.Id.Value() {
					continue
				}
				if _, found := fkCombinations[tablePair.FKTable]; !found {
					fkCombinations[tablePair.FKTable] = make(map[int][]*dataflow.ColumnInfoType)

				}
			}
			for key, columnCombination := range columnCombinationMap {
				mx := make([]int,len(columnCombination.Columns))
				for position, _ := range columnCombination.Columns {
					for _, pair := range pairsFilteredByHash {
						if table.Id.Value() != pair.PKColumn.TableInfo.Id.Value() {
							continue
						}
						if peerMap, peerMapFound := fkCombinations[pair.FKColumn.TableInfo]; !peerMapFound{
							continue;
						} else {
							mx[position]++
							if peers,peersFound := peerMap[position];!peersFound {
								peers = make([]*dataflow.ColumnInfoType, 0, 3);
								peers = append(peers,pair.FKColumn)
								peerMap[position] = peers
							} else {
								peers = append(peers,pair.FKColumn)
								peerMap[position] = peers
							}
						}
					}
				}
				//A->[E,D]; B->[F,G]; C->[H] :
				// ABC -> [EFH,DFH,EGH,FGH]
				//3
				for fkTable, peersMap := range fkCombinations {
					ix := make([]int,len(columnCombination.Columns))
					for {
						FKey := &ComplexKeyType{
							Columns: make(ColumnArrayType, len(columnCombination.Columns)),
						}
						for position, _ := range columnCombination.Columns {
								FKey.Columns[position]=peersMap[position][ix[position]]
						}
						ix[0]++
						for position, _ := range columnCombination.Columns {
							if ix[position]<mx[position] {
								break
							} else if position == len(mx){
								break

							}
						}

					}
				}

			}
		}*/


	tracelog.Info(packageName, funcName, "Elapsed time: %v", time.Since(start))
	tracelog.Completed(packageName, funcName)
	return err
}

