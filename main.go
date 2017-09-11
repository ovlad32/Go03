package main

import (
	"astra/dataflow"
	"astra/nullable"
	"context"
	"flag"
	"github.com/goinggo/tracelog"
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
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"sort"
	"sparsebitset"
)

//-workflow_id 57 -metadata_id 331 -cpuprofile cpu.prof.out

var packageName = "main"

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

var pathToConfigFile = flag.String("configfile", "./config.json", "path to config file")
var argMetadataIds = flag.String("metadata_id", string(-math.MaxInt64), "")
var argWorkflowIds = flag.String("workflow_id", string(-math.MaxInt64), "")

/*
func readConfig() (*dataflow.DumpConfigType, error) {
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
	var result dataflow.DumpConfigType
	err = jd.Decode(&result)
	if err != nil {
		tracelog.Errorf(err, funcName, "Decoding config file %v", *pathToConfigFile)
		return nil, err
	}
	return &result, nil
}
*/
func f1() {
	i := 0
	start := time.Now()
	file, err := os.Open("F:/home/data.253.4/data/100020/86/ORCL.CRA.LIABILITIES.dat")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	bfile := bufio.NewReaderSize(file, 4096)
	zfile, err := gzip.NewReader(bfile)
	if err != nil {
		panic(err)
	}
	defer zfile.Close()
	buf := bufio.NewReaderSize(zfile, 4096*5)
	for {
		b, err := buf.ReadSlice(byte(10))
		if err == nil {
			s := string(bytes.Split(b, []byte{31})[0])
			f, err := strconv.ParseFloat(s, 64)
			if err == nil {
				bi := math.Float64bits(f)
				frac, exp := math.Frexp(f)
				fmt.Printf("%v, %v  %v ^ %v\n", f, bi, frac, math.Pow(2, float64(exp)))
			}

			i += len(s)

		} else if err == io.EOF {
			break
		} else {
			if err != nil {
				panic(err)
			}
		}
	}
	fmt.Println(i)
	tracelog.Info(packageName, "f1", "Elapsed time: %v", time.Since(start))
}

func main() {
	//	funcName := "main"
	flag.Parse()
	tracelog.Start(tracelog.LevelInfo)
	defer tracelog.Stop()

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

type keyColumnPairType struct {
	PK *dataflow.ColumnInfoType
	FK *dataflow.ColumnInfoType
}

type keyColumnPairArrayType []*keyColumnPairType

func (a keyColumnPairArrayType) Len() int      { return len(a) }
func (a keyColumnPairArrayType) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a keyColumnPairArrayType) Less(i, j int) bool {
	if a[i].PK.HashUniqueCount.Value() == a[j].PK.HashUniqueCount.Value() {
		return a[i].FK.HashUniqueCount.Value() < a[j].FK.HashUniqueCount.Value()
	} else {
		return a[i].PK.HashUniqueCount.Value() < a[j].PK.HashUniqueCount.Value()
	}
}

func testBitsetCompare() (err error) {
	funcName := "testBitsetBuilding"
	metadataIds := make(map[int64]bool)

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

	tracelog.Started(packageName, funcName)

	dr, err := dataflow.NewInstance()

	tracelog.Started(packageName, funcName)
	start := time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())

	columnToProcess := make([]*dataflow.ColumnInfoType, 0, 100)

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
			exTable := dataflow.ExpandFromMetadataTable(table)
			for _, column := range exTable.Columns {
				column.Categories, err = dr.Repository.DataCategoryByColumnId(column)
				column.HashUniqueCount = nullable.NullInt64{}
				column.NonNullCount = nullable.NullInt64{}
				columnToProcess = append(columnToProcess, column)
			}
		}
	}

	PopulateAggregatedStatistics := func(col *dataflow.ColumnInfoType) (err error) {
		var hashUniqueCount, nonNullCount int64 = 0, 0
		for _, category := range col.Categories {
			if !category.HashUniqueCount.Valid() {
				err = fmt.Errorf("HashUniqueCount statistics is empty in %v", category)
				tracelog.Error(err, packageName, funcName)
				return err
			}
			if !category.NonNullCount.Valid() {
				err = fmt.Errorf("NonNullCount statistics is empty in %v", category)
				tracelog.Error(err, packageName, funcName)
				return err
			}
			hashUniqueCount += category.HashUniqueCount.Value()
			nonNullCount += category.NonNullCount.Value()
		}

		col.HashUniqueCount = nullable.NewNullInt64(int64(hashUniqueCount))
		col.NonNullCount = nullable.NewNullInt64(int64(nonNullCount))
		return nil
	}

	CheckIfNonFK := func(colFK, colPK *dataflow.ColumnInfoType) (nonFK bool, err error) {
		if !colFK.TableInfo.RowCount.Valid() {
			err = fmt.Errorf("RowCount statistics is empty in %v", colFK.TableInfo)
			tracelog.Error(err, packageName, funcName)
			return false, err
		}

		nonFK = colFK.TableInfo.RowCount.Value() < 2
		if nonFK {
			return
		}

		categoryCount := len(colFK.Categories)

		nonFK = categoryCount == 0 || categoryCount > len(colPK.Categories)
		if nonFK {
			return
		}

		for categoryKey, categoryFK := range colFK.Categories {
			if categoryPK, found := colPK.Categories[categoryKey]; !found {
				return true, nil
			} else {
				if !categoryFK.IsNumeric.Valid() {
					err = fmt.Errorf("IsNumeric statistics is empty in %v", categoryFK)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if categoryFK.IsNumeric.Value() {
					if !categoryFK.IsInteger.Valid() {
						err = fmt.Errorf("IsInteger statistics is empty in %v", categoryFK)
						tracelog.Error(err, packageName, funcName)
						return
					}
					if categoryFK.IsInteger.Value() {
						if !categoryFK.ItemCount.Valid() {
							err = fmt.Errorf("ItemCount statistics is empty in %v", categoryFK)
							tracelog.Error(err, packageName, funcName)
							return
						}

						nonFK = categoryFK.ItemCount.Value() > categoryPK.ItemCount.Value()
						if nonFK {
							return
						}
					}

					if !categoryFK.MinNumericValue.Valid() {
						err = fmt.Errorf("MinNumericValue statistics is empty in %v", categoryFK)
						tracelog.Error(err, packageName, funcName)
						return
					}
					if !categoryFK.MaxNumericValue.Valid() {
						err = fmt.Errorf("MaxNumericValue statistics is empty in %v", categoryFK)
						tracelog.Error(err, packageName, funcName)
						return
					}
					if !categoryPK.MinNumericValue.Valid() {
						err = fmt.Errorf("MinNumericValue statistics is empty in %v", categoryPK)
						tracelog.Error(err, packageName, funcName)
						return
					}
					if !categoryPK.MaxNumericValue.Valid() {
						err = fmt.Errorf("MaxNumericValue statistics is empty in %v", categoryPK)
						tracelog.Error(err, packageName, funcName)
						return
					}
					nonFK =
						categoryFK.MaxNumericValue.Value() > categoryPK.MaxNumericValue.Value() ||
							categoryFK.MinNumericValue.Value() < categoryPK.MinNumericValue.Value()
					if nonFK {
						return
					}
				} else {
					nonFK = categoryFK.ItemCount.Value() > categoryPK.ItemCount.Value()
					if nonFK {
						return
					}
					nonFK = float64(categoryFK.HashUniqueCount.Value()) > float64(categoryPK.HashUniqueCount.Value())*1.2
					if nonFK {
						return
					}
				}

			}
		}

		// FK Hash unique count has to be less than PK Hash unique count
		nonFK = float64(colPK.HashUniqueCount.Value()) > float64(colPK.HashUniqueCount.Value())*1.2
		if nonFK {
			return true, nil
		}

		return false, nil
	}

	CheckIfNonPK := func(col *dataflow.ColumnInfoType) (nonPK bool, err error) {
		// Null existence
		if !col.TableInfo.RowCount.Valid() {
			err = fmt.Errorf("RowCount statistics is empty in %v", col.TableInfo)
			tracelog.Error(err, packageName, funcName)
			return false, err
		}

		nonPK = col.TableInfo.RowCount.Value() < 2
		if nonPK {
			return
		}
		var totalNonNullCount uint64 = 0
		for _, category := range col.Categories {
			if !category.NonNullCount.Valid() {
				err = fmt.Errorf("NonNullCount statistics is empty in %v", category)
				tracelog.Error(err, packageName, funcName)
				return false, err
			}
			totalNonNullCount += uint64(category.NonNullCount.Value())
		}
		nonPK = uint64(col.TableInfo.RowCount.Value()) != totalNonNullCount
		return nonPK, nil
	}

	pairs0 := make(keyColumnPairArrayType, 0, 1000)
	var bruteForcePairCount int = 0

	for leftIndex, leftColumn := range columnToProcess {
		if !leftColumn.NonNullCount.Valid() {
			err = PopulateAggregatedStatistics(leftColumn)
			if err != nil {
				return
			}
		}
		leftNonPK, err := CheckIfNonPK(leftColumn)
		if err != nil {
			return err
		}

		/*if leftNonPK {
			fmt.Printf("%v.%v is not a single PK", leftColumn.TableInfo, leftColumn.ColumnName);
		} else {
			fmt.Printf("%v.%v is a single PK", leftColumn.TableInfo, leftColumn.ColumnName);
		}*/

		for rightIndex := leftIndex + 1; rightIndex < len(columnToProcess); rightIndex++ {
			bruteForcePairCount = bruteForcePairCount + 1

			rightColumn := columnToProcess[rightIndex]
			if !rightColumn.HashUniqueCount.Valid() {
				err = PopulateAggregatedStatistics(rightColumn)
				if err != nil {
					return err
				}

			}

			rightNonPK, err := CheckIfNonPK(rightColumn)
			if err != nil {
				return err
			}

			if rightNonPK && leftNonPK {
				continue
			}
			if !leftNonPK {
				rightNonFK, err := CheckIfNonFK(rightColumn, leftColumn)
				if err != nil {
					return err
				}
				if !rightNonFK {
					pair := &keyColumnPairType{
						PK: leftColumn,
						FK: rightColumn,
					}
					pairs0 = append(pairs0, pair)
				}
			}
			if !rightNonPK {
				leftNonFK, err := CheckIfNonFK(leftColumn, rightColumn)
				if err != nil {
					return err
				}
				if !leftNonFK {
					pair := &keyColumnPairType{
						PK: rightColumn,
						FK: leftColumn,
					}
					pairs0 = append(pairs0, pair)
				}
			}

		}
	}

	fmt.Println(bruteForcePairCount, len(pairs0), float64(len(pairs0))*100/float64(bruteForcePairCount))
	sort.Sort(sort.Reverse(pairs0))
	var lastPKColumn *dataflow.ColumnInfoType

	analyzeContentBitsetFunc := func(ctx context.Context, dataCategoryPK, dataCategoryFK *dataflow.DataCategoryType) (bool, error) {
		if dataCategoryPK.Stats.ContentBitset == nil {
			dataCategoryPK.Stats.ContentBitset = sparsebitset.New(0)
			err = dataCategoryPK.ReadBitsetFromDisk(ctx, dr.Config.AstraDumpPath, dataflow.Cont)
		}
		if dataCategoryFK.Stats.ContentBitset == nil {
			dataCategoryFK.Stats.ContentBitset = sparsebitset.New(0)
			err = dataCategoryFK.ReadBitsetFromDisk(ctx, dr.Config.AstraDumpPath, dataflow.Cont)
		}
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return false, err
		}
		var cardinality uint64
		if dataCategoryFK.Stats.ContentBitset == nil {
			tracelog.Info(packageName, funcName, "Content Bitset for %v (%v) is null ", dataCategoryFK.Column, dataCategoryFK.Key)
			return false, nil
		} else {
			cardinality, err = dataCategoryFK.Stats.ContentBitset.IntersectionCardinality(dataCategoryPK.Stats.ContentBitset)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return false, err
			}
		}
		return cardinality == dataCategoryPK.Stats.ContentBitsetCardinality, nil
	}

	analyzeHashBitsetFunc := func(ctx context.Context, dataCategoryPK, dataCategoryFK *dataflow.DataCategoryType) (bool, error) {
		if dataCategoryPK.Stats.HashBitset == nil {
			dataCategoryPK.Stats.HashBitset = sparsebitset.New(0)
			err = dataCategoryPK.ReadBitsetFromDisk(ctx, dr.Config.AstraDumpPath, dataflow.Hash)
		}
		if dataCategoryFK.Stats.HashBitset == nil {
			dataCategoryFK.Stats.HashBitset = sparsebitset.New(0)
			err = dataCategoryFK.ReadBitsetFromDisk(ctx, dr.Config.AstraDumpPath, dataflow.Hash)
		}
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return false, err
		}
		var cardinality uint64
		if dataCategoryFK.Stats.HashBitset == nil {
			tracelog.Info(packageName, funcName, "Hash Bitset for %v (%v) is null ", dataCategoryFK.Column.Id, dataCategoryFK.Key)
			return false, nil
		} else {
				cardinality, err = dataCategoryFK.Stats.HashBitset.IntersectionCardinality(dataCategoryPK.Stats.HashBitset)
			/*if dataCategoryPK.Column.ColumnName.Value() == "EXPIRY_DATE" && dataCategoryFK.Column.ColumnName.Value() == "CORPORATE_DATE_FUNDED" {
				fmt.Printf(
					"SELECT %v from %v minus SELECT %v from %v /*%v,%v,%v*,%v/\n",
					dataCategoryFK.Column.ColumnName,
					dataCategoryFK.Column.TableInfo,
					dataCategoryPK.Column.ColumnName,
					dataCategoryPK.Column.TableInfo,
					cardinality,dataCategoryPK.Stats.HashBitsetCardinality,dataCategoryFK.Stats.HashBitset.Cardinality(),dataCategoryPK.Stats.HashBitset.Cardinality())
			}*/
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return false, err
			}
		}
		return cardinality == dataCategoryPK.Stats.HashBitsetCardinality, nil
	}

	traversePairs := func(
		pairs keyColumnPairArrayType,
		processPairFunc func(ctx context.Context, dataCategoryPK, dataCategoryFK *dataflow.DataCategoryType) (bool, error),
	) (nextPairs keyColumnPairArrayType, err error) {

		for _, pair := range pairs {
			if lastPKColumn != nil {
				if lastPKColumn != pair.PK {
					lastPKColumn.ResetBitset(dataflow.Cont)
				}
			}
			ctx, ctxCancelFunc := context.WithCancel(context.Background())
			var result = true
			for dataCategoryKey, dataCategoryFK := range pair.FK.Categories {
				if dataCategoryPK, found := pair.PK.Categories[dataCategoryKey]; !found {
					err = fmt.Errorf("The second pass for column pair PK:%v - FK:%v doesn't reveal datacategory for the key code %v.", pair.PK, pair.FK, dataCategoryKey)
					tracelog.Error(err, packageName, funcName)
					ctxCancelFunc()
					return
				} else {
					result, err = processPairFunc(ctx, dataCategoryPK, dataCategoryFK)
					if err != nil {
						tracelog.Error(err, packageName, funcName)
						ctxCancelFunc()
						return nil, err
					}
					if !result {
						break
					}
				}
			}
			if result {
				if nextPairs == nil {
					nextPairs = make(keyColumnPairArrayType, 0, 1000)
				}
				nextPairs = append(nextPairs, pair)
			}
		}
		return nextPairs, nil
	}

	pairs1, err := traversePairs(pairs0, analyzeContentBitsetFunc)
	if pairs1 == nil {
		return
	}
	fmt.Println(bruteForcePairCount, len(pairs1), float64(len(pairs1))*100/float64(bruteForcePairCount))

	for _, pair := range pairs0 {
		pair.PK.ResetBitset(dataflow.Cont)
		pair.FK.ResetBitset(dataflow.Cont)
	}

	pairs2, err := traversePairs(pairs1, analyzeHashBitsetFunc)
	if pairs2 == nil {
		return
	}
	fmt.Println(bruteForcePairCount, len(pairs2), float64(len(pairs2))*100/float64(bruteForcePairCount))
	if true {
		for _, pair := range pairs2 {
			fmt.Printf("PK:%v(%v) - FK:%v(%v)%v\n", pair.PK, pair.PK.HashUniqueCount, pair.FK, pair.FK.HashUniqueCount, pair.FK.Id)
		}
	}

	for _, pair := range pairs1 {
		pair.PK.ResetBitset(dataflow.Hash)
		pair.FK.ResetBitset(dataflow.Hash)
	}

	/*
		for _, pair := range pairs0 {
			fmt.Printf("PK:%v(%v) - FK:%v(%v)%v\n", pair.PK, pair.PK.HashUniqueCount, pair.FK, pair.FK.HashUniqueCount, pair.FK.Id)
		}*/
	//TODO: LOAD BITSETs here

	/*processTable := func(runContext context.Context) (err error) {
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

				err := dr.BuildHashBitset(runContext,inTable)
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

		processTableChan = make(chan *dataflow.TableInfoType,	dr.Config.TableWorkers)
		processTableContext, processTableContextCancelFunc := context.WithCancel(context.Background())
		for index := 0; index < dr.Config.TableWorkers; index++ {
			wg.Add(1)
			go func() {
				err = processTable(processTableContext)
				wg.Done()
				if err != nil {
					tracelog.Errorf(err,packageName,funcName,"Сancel сontext called ")
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
	}*/
	tracelog.Info(packageName, funcName, "Elapsed time: %v", time.Since(start))
	tracelog.Completed(packageName, funcName)
	return err
}
