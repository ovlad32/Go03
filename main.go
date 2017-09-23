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
	"errors"
	"fmt"
	"hash/fnv"
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
	IsSingle bool
	PK       *dataflow.ColumnInfoType
	FK       *dataflow.ColumnInfoType
}
/*
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
*/
type ColumnArrayType []*dataflow.ColumnInfoType

func (ca ColumnArrayType) ColumnIdString() (result string) {
	result = ""
	for index, col:= range ca {
		if index == 0 {
			result = strconv.FormatInt(int64(col.Id.Value()), 10)
		} else {
		}
		result = result + "-" + strconv.FormatInt(int64(col.Id.Value()), 10)
	}
	return result
}

type ComplexPKDupDataType struct {
	Data       [][]byte
	LineNumber uint64
}

type ComplexPKCombinationType struct {
	columns               ColumnArrayType
	columnPositions       []int
	lastSortedColumnIndex int
	cardinality           uint64
	firstPassBitset                *sparsebitset.BitSet
	duplicateBitset                *sparsebitset.BitSet
	duplicatesByHash      map[uint32][]*ComplexPKDupDataType
	dataDuplicationFound  bool
}

func (pkc ComplexPKCombinationType) ColumnIndexString() (result string) {
	result = ""
	for index, position := range pkc.columnPositions {
		if index == 0 {
			result = strconv.FormatInt(int64(position), 10)
		} else {
		}
		result = result + "-" + strconv.FormatInt(int64(position), 10)
	}
	return result
}

var DataDuplicateFoundError = errors.New("Data duplicatation found")

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
			tracelog.Info(funcName, packageName, "Column %v is not FK. RowCount < 2", colFK)
			return
		}

		categoryCountFK := len(colFK.Categories)
		nonFK = categoryCountFK == 0
		if nonFK {
			tracelog.Info(funcName, packageName, "Column %v is not FK. DataCategory count  = 0", colFK)
			return
		}

		categoryCountPK := len(colPK.Categories)

		nonFK = categoryCountFK == 0 || categoryCountFK > categoryCountPK
		if nonFK {
			tracelog.Info(funcName, packageName, "Column %v is not FK to %v. categoryCountFK > categoryCountPK; %v > %v", colFK, colPK, categoryCountFK, categoryCountPK)
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
						if !categoryFK.ItemUniqueCount.Valid() {
							err = fmt.Errorf("ItemUniqueCount statistics is empty in %v", categoryFK)
							tracelog.Error(err, packageName, funcName)
							return
						}

						nonFK = categoryFK.ItemUniqueCount.Value() > categoryPK.ItemUniqueCount.Value()
						if nonFK {
							tracelog.Info(funcName, packageName,
								"Column %v is not FK to %v for DataCategory %v: ItemUniqueCountFK > ItemUniqueCountPK; %v > %v",
								colFK, colPK, categoryFK.Key,
								categoryFK.ItemUniqueCount.Value(),
								categoryPK.ItemUniqueCount.Value())
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
					nonFK = categoryFK.MaxNumericValue.Value() > categoryPK.MaxNumericValue.Value()
					if nonFK {
						tracelog.Info(funcName, packageName,
							"Column %v is not FK to %v for DataCategory %v:  MaxNumericValueFK > MaxNumericValuePK; %v > %v",
							colFK, colPK, categoryFK.Key,
							categoryFK.MaxNumericValue.Value(),
							categoryPK.MaxNumericValue.Value())
						return
					}
					nonFK = categoryFK.MinNumericValue.Value() < categoryPK.MinNumericValue.Value()
					if nonFK {
						tracelog.Info(funcName, packageName,
							"Column %v is not FK to %v for DataCategory %v: MinNumericValueFK < MinNumericValuePK; %v < %v",
							colFK, colPK, categoryFK.Key,
							categoryFK.MinNumericValue.Value(),
							categoryPK.MinNumericValue.Value())
						return
					}
				} else {
					nonFK = categoryFK.ItemUniqueCount.Value() > categoryPK.ItemUniqueCount.Value()
					if nonFK {
						tracelog.Info(funcName, packageName,
							"Column %v is not FK to %v for DataCategory %v: ItemUniqueCountFK > ItemUniqueCountPK; %v > %v",
							colFK, colPK, categoryFK.Key,
							categoryFK.ItemUniqueCount.Value(),
							categoryPK.ItemUniqueCount.Value())
						return
					}
					ratio := 1.2
					nonFK = float64(categoryFK.HashUniqueCount.Value()) > float64(categoryPK.HashUniqueCount.Value())*ratio
					if nonFK {
						tracelog.Info(funcName, packageName,
							"Column %v is not FK to %v for DataCategory %v: HashUniqueCountFK > DataCategory.HashUniqueCountPK*ratio(%v); %v > %v",
							colFK, colPK, categoryFK.Key, ratio,
							categoryFK.HashUniqueCount.Value(),
							uint64(float64(categoryPK.HashUniqueCount.Value())*ratio),
						)
						return
					}
				}

			}
		}

		// FK Hash unique count has to be less than PK Hash unique count
		{
			ratio := 1.2
			nonFK = float64(colFK.HashUniqueCount.Value()) > float64(colPK.HashUniqueCount.Value())*ratio
			if nonFK {
				tracelog.Info(funcName, packageName,
					"Column %v is not FK to %v. HashUniqueCountFK > HashUniqueCountPK*ratio(%v); %v > %v",
					colFK, colPK, ratio,
					colPK.HashUniqueCount.Value(),
					uint64(float64(colPK.HashUniqueCount.Value())*ratio),
				)
				return true, nil
			}
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
		if nonPK {
			tracelog.Info(funcName, packageName,
				"Column %v is not PK. TotalRowCount != TotalNotNullCount. %v != %v",
				col, uint64(col.TableInfo.RowCount.Value()), totalNonNullCount,
			)
			return true, nil
		}

		nonPK = col.TotalRowCount.Value() == col.HashUniqueCount.Value()
		if nonPK {
			tracelog.Info(packageName, funcName,
				"Columns %v is not part of a complex PK. set of UniqueHashCount == TotalRowCount. %v == %v",
				col,
				col.HashUniqueCount.Value(),
				col.TotalRowCount.Value(),
			)
			return true, nil
		}
		return false, nil
	}

	pairsFilteredByFeatures := make([]*keyColumnPairType, 0, 1000)
	var bruteForcePairCount int = 0
	NonPKColumns := make(map[*dataflow.ColumnInfoType]bool)

	for leftIndex, leftColumn := range columnToProcess {
		if !leftColumn.NonNullCount.Valid() {
			err = PopulateAggregatedStatistics(leftColumn)
			if err != nil {
				return
			}
		}
		var leftNonPK, rightNonPK, columnFound bool

		if leftNonPK, columnFound = NonPKColumns[leftColumn]; !columnFound {
			leftNonPK, err = CheckIfNonPK(leftColumn)
			if err != nil {
				return err
			}
			if leftNonPK {
				NonPKColumns[leftColumn] = leftNonPK
			}

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
			rightNonPK = false
			if rightNonPK, columnFound = NonPKColumns[rightColumn]; !columnFound {
				rightNonPK, err = CheckIfNonPK(rightColumn)
				if err != nil {
					return err
				}
				if rightNonPK {
					NonPKColumns[rightColumn] = rightNonPK
				}
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
					pairsFilteredByFeatures = append(pairsFilteredByFeatures, pair)
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
					pairsFilteredByFeatures = append(pairsFilteredByFeatures, pair)
				}
			}

		}
	}

	sort.Slice(
			pairsFilteredByFeatures,
			func(i, j int) bool {
				if pairsFilteredByFeatures[i].PK.HashUniqueCount.Value() == pairsFilteredByFeatures[j].PK.HashUniqueCount.Value() {
					return pairsFilteredByFeatures[i].PK.Id.Value() < pairsFilteredByFeatures[j].PK.HashUniqueCount.Value()
				} else {
					return pairsFilteredByFeatures[i].PK.HashUniqueCount.Value() < pairsFilteredByFeatures[j].PK.HashUniqueCount.Value()
				}
			},
	)


	var lastPKColumn *dataflow.ColumnInfoType

	analyzeItemBitsetFunc := func(ctx context.Context, dataCategoryPK, dataCategoryFK *dataflow.DataCategoryType) (bool, error) {
		if dataCategoryPK.Stats.ItemBitset == nil {
			dataCategoryPK.Stats.ItemBitset = sparsebitset.New(0)
			err = dataCategoryPK.ReadBitsetFromDisk(ctx, dr.Config.BitsetPath, dataflow.ItemBitsetSuffix)
		}
		if dataCategoryFK.Stats.ItemBitset == nil {
			dataCategoryFK.Stats.ItemBitset = sparsebitset.New(0)
			err = dataCategoryFK.ReadBitsetFromDisk(ctx, dr.Config.BitsetPath, dataflow.ItemBitsetSuffix)
		}
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return false, err
		}
		var cardinality uint64
		if dataCategoryFK.Stats.ItemBitset == nil {
			tracelog.Info(packageName, funcName, "Item Bitset for %v (%v) is null ", dataCategoryFK.Column, dataCategoryFK.Key)
			return false, nil
		} else {
			cardinality, err = dataCategoryFK.Stats.ItemBitset.IntersectionCardinality(dataCategoryPK.Stats.ItemBitset)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return false, err
			}
		}
		result := cardinality == dataCategoryFK.Stats.ItemBitsetCardinality
		if !result {
			tracelog.Info(funcName, packageName,
				"Column %v is not FK to %v for DataCategory %v: IntersectionCardinality != FkCardinality for Content values %v != %v",
				dataCategoryFK.Column, dataCategoryPK.Column, dataCategoryFK.Key,
				cardinality, dataCategoryFK.Stats.ItemBitsetCardinality,
			)
		}
		return result, nil
	}

	analyzeHashBitsetFunc := func(ctx context.Context, dataCategoryPK, dataCategoryFK *dataflow.DataCategoryType) (bool, error) {
		if dataCategoryPK.Stats.HashBitset == nil {
			dataCategoryPK.Stats.HashBitset = sparsebitset.New(0)
			err = dataCategoryPK.ReadBitsetFromDisk(ctx, dr.Config.BitsetPath, dataflow.HashBitsetSuffix)
		}
		if dataCategoryFK.Stats.HashBitset == nil {
			dataCategoryFK.Stats.HashBitset = sparsebitset.New(0)
			err = dataCategoryFK.ReadBitsetFromDisk(ctx, dr.Config.BitsetPath, dataflow.HashBitsetSuffix)
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

			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return false, err
			}
		}
		result := cardinality == dataCategoryFK.Stats.HashBitsetCardinality
		if !result {
			tracelog.Info(funcName, packageName,
				"Column %v is not FK to %v for DataCategory %v: IntersectionCardinality != FkCardinality for Hash values %v != %v",
				dataCategoryFK.Column, dataCategoryPK.Column, dataCategoryFK.Key,
				cardinality, dataCategoryFK.Stats.HashBitsetCardinality,
			)
		}
		return result, nil
	}

	traversePairs := func(
		pairs []*keyColumnPairType,
		processPairFunc func(ctx context.Context, dataCategoryPK, dataCategoryFK *dataflow.DataCategoryType) (bool, error),
	) (nextPairs []*keyColumnPairType, err error) {

		for _, pair := range pairs {
			if lastPKColumn != nil {
				if lastPKColumn != pair.PK {
					lastPKColumn.ResetBitset(dataflow.ItemBitsetSuffix)
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
					nextPairs = make([]*keyColumnPairType, 0, 1000)
				}
				nextPairs = append(nextPairs, pair)
			}
		}
		return nextPairs, nil
	}

	tracelog.Info(packageName, funcName,
		"Feature analysis efficiency: %v%%. Left %v from %v",
		100.0-math.Trunc(float64(len(pairsFilteredByFeatures))*100/float64(bruteForcePairCount)),
		len(pairsFilteredByFeatures),
		bruteForcePairCount,
	)

	pairsFilteredByContent, err := traversePairs(pairsFilteredByFeatures, analyzeItemBitsetFunc)
	if pairsFilteredByContent == nil {
		return
	}

	tracelog.Info(packageName, funcName,
		"Content analysis efficiency: %v%%. Left %v from %v",
		100.0-math.Trunc(float64(len(pairsFilteredByContent))*100/float64(len(pairsFilteredByFeatures))),
		len(pairsFilteredByContent),
		len(pairsFilteredByFeatures),
	)

	for _, pair := range pairsFilteredByFeatures {
		pair.PK.ResetBitset(dataflow.ItemBitsetSuffix)
		pair.FK.ResetBitset(dataflow.ItemBitsetSuffix)
	}
	if false {
		for _, pair := range pairsFilteredByContent {
			fmt.Printf("PK:%v(%v) - FK:%v(%v)%v\n", pair.PK, pair.PK.HashUniqueCount, pair.FK, pair.FK.HashUniqueCount, pair.FK.Id)
		}
	}
	pairsFilteredByHash, err := traversePairs(pairsFilteredByContent, analyzeHashBitsetFunc)
	if pairsFilteredByHash == nil {
		return
	}
	tracelog.Info(packageName, funcName,
		"Hash analysis efficiency: %v%%. Left %v from %v",
		100.0-math.Trunc(float64(len(pairsFilteredByHash))*100/float64(len(pairsFilteredByContent))),
		len(pairsFilteredByHash),
		len(pairsFilteredByContent),
	)
	//	fmt.Println(bruteForcePairCount, len(pairsFilteredByHash), float64(len(pairsFilteredByHash))*100/float64(bruteForcePairCount))

	for _, pair := range pairsFilteredByContent {
		pair.PK.ResetBitset(dataflow.HashBitsetSuffix)
		pair.FK.ResetBitset(dataflow.HashBitsetSuffix)
	}

	if true {
		fmt.Printf("List of pairs after filtration:\n")
		for _, pair := range pairsFilteredByHash {
			fmt.Printf("PK:%v(%v) - FK:%v(%v)%v\n", pair.PK, pair.PK.HashUniqueCount, pair.FK, pair.FK.HashUniqueCount, pair.FK.Id)
		}

	}

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
			appendToColumnMap(pair.PK)
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
				if columns[i].HashUniqueCount.Value()  == columns[j].HashUniqueCount.Value() {
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
			//appendToColumnMap(pair.FK)
			//columns := make([]*dataflow.ColumnInfoType,0)

		}
		//columnMap
		/*
				var dataMap map[string][]byte
			var columnToTrack map[int]int

			for table, columns := range columnMap {
				columnToTrack  = make(map[int]int)
				for _, column := range columns {
					//fmt.Printf("PK:%v(%v) - FK:%v(%v)%v\n", pa

					dataMap = make(map[string][]byte)
					for index := range table.Columns {
						if column == table.Columns[index] {
							columnToTrack[index] = 0
						}
					}

					p1 := func(context context.Context, i uint64, rawData [][]byte) error {
						data := string(rawData[columnIndex])
						if len(data) > 0 {
							dataMap[data] = true
						}
						return nil
					}

					dr.ReadAstraDump(
						context.TODO(),
						table,
						p1,
						&dataflow.TableDumpConfigType{
							Path:            dr.Config.AstraDumpPath,
							GZip:            dr.Config.AstraDataGZip,
							ColumnSeparator: dr.Config.AstraColumnSeparator,
							LineSeparator:   dr.Config.AstraLineSeparator,
							BufferSize:      dr.Config.AstraReaderBufferSize,
						},
					)
					if len(dataMap) > 0 {

					}

				}
				fmt.Printf("\n")
			}*/

	}

	if true {
		//TODO: Assume real data count doesn't differ to hash data count
		for _, pair := range pairsFilteredByHash {
			pair.PK.UniqueRowCount = pair.PK.HashUniqueCount
			pair.FK.UniqueRowCount = pair.FK.HashUniqueCount
		}

		type tablePairType struct {
			PKT, FKT int64
		}
		printColumnArray := func(arr []*ComplexPKCombinationType) {
			if len(arr) ==0 {
				fmt.Println("No column combinations left")
				return
			}
			for _, key := range arr {
				fmt.Printf("%v\n", key.columns)
			}
		}
		_ = printColumnArray

		tablePairMap := make(map[tablePairType][]*keyColumnPairType)
		tableCPKeys := make(map[int64]map[string]*ComplexPKCombinationType)
		_ = tableCPKeys

		for _, pair := range pairsFilteredByHash {
			/*if !(pair.PK.TableInfo.TableName.Value() == "TX" && pair.FK.TableInfo.TableName.Value() == "TX_ITEM") {
				continue
			}*/
			tablePair := tablePairType{PKT: pair.PK.TableInfo.Id.Value(), FKT: pair.FK.TableInfo.Id.Value()}
			if arr, found := tablePairMap[tablePair]; !found {
				arr = make([]*keyColumnPairType, 0, 10)
				arr = append(arr, pair)
				tablePairMap[tablePair] = arr
			} else {
				tablePairMap[tablePair] = append(arr, pair)
			}
		}

		{
			count := 0
			for _, columnPairs := range tablePairMap {
				if len(columnPairs) > 1 {
					count++
				}
			}
			if count == 0 {
				tracelog.Info(packageName, funcName, "There is no complex key candidates found!")
				return
			}
		}

		for _, columnPairs := range tablePairMap {
			if len(columnPairs) > 1 {
				fmt.Printf("\nPK:%v) - FK:%v:\n", columnPairs[0].PK.TableInfo, columnPairs[0].FK.TableInfo)
				var SortedPKCols []*dataflow.ColumnInfoType
				ComplexPK1 := make([]*ComplexPKCombinationType, 0, 10)
				_ = ComplexPK1

				{
					pkCols := make(map[*dataflow.ColumnInfoType]bool)
					for _, columnPair := range columnPairs {
						pkCols[columnPair.PK] = true
					}

					SortedPKCols = make([]*dataflow.ColumnInfoType, 0, len(pkCols))

					for col, _ := range pkCols {
						SortedPKCols = append(SortedPKCols, col)
					}
					sort.Slice(SortedPKCols, func(i, j int) bool {
						return SortedPKCols[i].UniqueRowCount.Value() > SortedPKCols[j].UniqueRowCount.Value()
					})

					//buildCombinations := func() {
					/*printColumns:= func(arr[]*dataflow.ColumnInfoType) {
							for _,col:= range arr {
								fmt.Printf("%v,",col)
							}
							fmt.Printf("\n,")
					}*/

					for {
						if len(SortedPKCols) == 0 {
							break
						}
						var inputColumnCombinations []*ComplexPKCombinationType
						var CPKeysLast int

						inputColumnCombinations = make([]*ComplexPKCombinationType, 1, len(SortedPKCols))
						inputColumnCombinations[0] = &ComplexPKCombinationType{
							columns:               make([]*dataflow.ColumnInfoType, 1),
							lastSortedColumnIndex: 1,
						}
						inputColumnCombinations[0].columns[0] = SortedPKCols[0]
						//1,2,3,4 -> 12,13,14,123,124,134,23,24,234,34
						for {
							CPKeysLast = len(ComplexPK1)
							for _, inputColumnCombination := range inputColumnCombinations {
								var columnCombination *ComplexPKCombinationType
								inputLength := len(inputColumnCombination.columns)
								for index := inputColumnCombination.lastSortedColumnIndex; index < len(SortedPKCols); index++ {
									columnCombination = &ComplexPKCombinationType{
										columns:               make([]*dataflow.ColumnInfoType, inputLength, inputLength+1),
										lastSortedColumnIndex: index + 1,
									}
									copy(columnCombination.columns, inputColumnCombination.columns)
									columnCombination.columns = append(columnCombination.columns, SortedPKCols[index])
									ComplexPK1 = append(ComplexPK1, columnCombination)
								}
							}
							inputColumnCombinations = ComplexPK1[CPKeysLast:]
							if len(inputColumnCombinations) == 0 {
								break
							}
						}
						SortedPKCols = SortedPKCols[1:]
					}

				}

				ComplexPK2 := make([]*ComplexPKCombinationType, 0, len(ComplexPK1))
				for _, columnCombination := range ComplexPK1 {
					for index, column := range columnCombination.columns {
						if index == 0 {
							columnCombination.cardinality = uint64(column.UniqueRowCount.Value())
						} else {
							columnCombination.cardinality = columnCombination.cardinality * uint64(column.UniqueRowCount.Value())
						}
					}
					if columnCombination.cardinality >= uint64(columnCombination.columns[0].TableInfo.RowCount.Value()) {
						ComplexPK2 = append(ComplexPK2, columnCombination)
					} else {
						tracelog.Info(packageName, funcName,
							"Column permutation %v data volume is insufficient to fill the table row count volume. %v < %v",
							columnCombination.columns,
							columnCombination.cardinality,
							columnCombination.columns[0].TableInfo.RowCount.Value(),
						)
					}
				}

				//ComplexPK3 := make([]*ComplexPKCombinationType, 0, len(ComplexPK2))
				var currentPKTable *dataflow.TableInfoType
				if len(ComplexPK2) > 0 {
					for _, columnCombination := range ComplexPK2 {
						if currentPKTable == nil {
							currentPKTable = columnCombination.columns[0].TableInfo
						}
						columnCombination.columnPositions = make([]int, len(columnCombination.columns))
						columnCombination.firstPassBitset = sparsebitset.New(0)
						columnCombination.duplicateBitset = sparsebitset.New(0)
						columnCombination.duplicatesByHash = make(map[uint32][]*ComplexPKDupDataType)
						for keyColumnIndex, pkc := range columnCombination.columns {
							for tableColumnIndex := 0; tableColumnIndex < len(pkc.TableInfo.Columns); tableColumnIndex++ {
								if pkc.Id.Value() == pkc.TableInfo.Columns[tableColumnIndex].Id.Value() {
									columnCombination.columnPositions[keyColumnIndex] = tableColumnIndex
								}
							}
						}
					}
					//	fmt.Printf("%v\n", columnCombination.columns)

					//bs := sparsebitset.New(0)
					var verificationMode bool = false
					proc := func(ctc context.Context, LineNumber, DataPosition uint64, data [][]byte) (result dataflow.ReadDumpActionType, err error) {
						var truncateCombinations = false

						for _, columnCombination := range ComplexPK2 {

							/*if columnCombination.dataDuplicationFound {
							continue
						}*/

							hashMethod := fnv.New32()
							for _, index := range columnCombination.columnPositions {
								hashMethod.Write(data[index])
							}
							hashValue := hashMethod.Sum32()
							var prevSet bool
							if !verificationMode {
								prevSet = columnCombination.firstPassBitset.Set(uint64(hashValue))
							} else {
								prevSet = columnCombination.duplicateBitset.Test(uint64(hashValue))
							}

							addToDuplicateByHash := func(duplicates []*ComplexPKDupDataType) {
								newDup := &ComplexPKDupDataType{
									Data:       make([][]byte, len(columnCombination.columns)),
									LineNumber: LineNumber,
								}
								for index, position := range columnCombination.columnPositions {
									newDup.Data[index] = data[position]
								}
								duplicates = append(duplicates, newDup)
								columnCombination.duplicatesByHash[hashValue] = duplicates
							}

							if prevSet {
								columnCombination.duplicateBitset.Set(uint64(hashValue));
								if duplicates, found := columnCombination.duplicatesByHash[hashValue]; !found {
									if !verificationMode {
										duplicates = make([]*ComplexPKDupDataType, 0, 3)
										addToDuplicateByHash(duplicates)
									}
								} else {
									for _, dup := range duplicates {
										for index, position := range columnCombination.columnPositions {
											result := bytes.Compare(dup.Data[index], data[position])
											if result == 0 && dup.LineNumber != LineNumber {
												// TODO:DUPLICATE!
												truncateCombinations = true
												tracelog.Info(packageName, funcName, "Data duplication found for columns %v in line %v", columnCombination.columns, LineNumber)
												columnCombination.dataDuplicationFound = true

												//return DataDuplicateFoundError;
											}
										}
									}
									if !verificationMode && !columnCombination.dataDuplicationFound {
										addToDuplicateByHash(duplicates)
									}
								}
							}
							if truncateCombinations {
								for index := 0; index < len(ComplexPK2); index++ {
									if ComplexPK2[index].dataDuplicationFound {
										ComplexPK2 = append(ComplexPK2[:index], ComplexPK2[index+1:]...)
									}
								}
								if len(ComplexPK2) == 0 {
									return dataflow.ReadDumpActionAbort,nil
								}
							}
						}
						return dataflow.ReadDumpActionContinue, nil
					}

					/*file,err := os.OpenFile(fmt.Sprintf("%v%c%v.dup.data",
					dr.Config.BuildBinaryDump,
						os.PathSeparator,
							columnCombination.ColumnIndexString(),
							),
								os.O_APPEND
				)
				if os.IsNotExist(err) {

				}*/
					tracelog.Info(packageName, funcName, "First pass for %v ", currentPKTable)

					result, _, err := dr.ReadAstraDump(
						context.TODO(),
						currentPKTable,
						proc,
						&dataflow.TableDumpConfigType{
							GZip:            dr.Config.AstraDataGZip,
							Path:            dr.Config.AstraDumpPath,
							LineSeparator:   dr.Config.AstraLineSeparator,
							ColumnSeparator: dr.Config.AstraColumnSeparator,
							BufferSize:      dr.Config.AstraReaderBufferSize,
						},
					)
					if err != nil {
						//columnCombination.duplicatesByHash = nil
						return err
					}
					_ = result
					if len(ComplexPK2) > 0 {
						verificationMode = true
						tracelog.Info(packageName, funcName, "Second pass for %v ", currentPKTable)

						_, _, err = dr.ReadAstraDump(
							context.TODO(),
							currentPKTable,
							proc,
							&dataflow.TableDumpConfigType{
								GZip:            dr.Config.AstraDataGZip,
								Path:            dr.Config.AstraDumpPath,
								LineSeparator:   dr.Config.AstraLineSeparator,
								ColumnSeparator: dr.Config.AstraColumnSeparator,
								BufferSize:      dr.Config.AstraReaderBufferSize,
							},
						)
						if err == DataDuplicateFoundError {
							//columnCombination.duplicatesByHash = nil
							//tracelog.Info(packageName, funcName,
							//	"Data of column combination %v is not unique!",
							//	columnCombination.columns,
							//)
							continue
						} else if err != nil {
							//columnCombination.duplicatesByHash = nil
							return err
						}
					}


					//columnCombination.duplicatesByHash = nil
					//tracelog.Info(packageName, funcName,
					//	"Data of column combination %v is not unique!",
					//	columnCombination.columns,
					//	)
					/*	continue
				} else if err != nil {
					//columnCombination.duplicatesByHash = nil
					return err
				}*/


					fmt.Printf("\nPK:%v - FK:%v:\n", columnPairs[0].PK.TableInfo, columnPairs[0].FK.TableInfo)
					printColumnArray(ComplexPK2)
				}

			}
		}
	}
	/*
		map[]
		sort.slice
		var data1,data2 []byte
		l1 := len(data1)
		l2 := len(data2)
		if l1 == l2 {
			for index := 0; index<l1; index++ {
				if data1[index] == data2[index] {
					continue
				}
				return data1[index] < data2[index]
			}
		}
		return l2<l1
	*/

	//dr.ReadAstraDump(context.TODO(),)

	/*
		for _, pair := range pairsFilteredByFeatures {
			fmt.Printf("PK:%v(%v) - FK:%v(%v)%v\n", pair.PK, pair.PK.HashUniqueCount, pair.FK, pair.FK.HashUniqueCount, pair.FK.Id)
		}*/

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
