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
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"sparsebitset"
	"unsafe"
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

type keyColumnPairType struct {
	IsSingle bool
	PKColumn *dataflow.ColumnInfoType
	FKColumn *dataflow.ColumnInfoType
}

/*
type keyColumnPairArrayType []*keyColumnPairType

func (a keyColumnPairArrayType) Len() int      { return len(a) }
func (a keyColumnPairArrayType) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a keyColumnPairArrayType) Less(i, j int) bool {
	if a[i].PKColumn.HashUniqueCount.Value() == a[j].PKColumn.HashUniqueCount.Value() {
		return a[i].FKColumn.HashUniqueCount.Value() < a[j].FKColumn.HashUniqueCount.Value()
	} else {
		return a[i].PKColumn.HashUniqueCount.Value() < a[j].PKColumn.HashUniqueCount.Value()
	}
}
*/
type ColumnArrayType []*dataflow.ColumnInfoType

func (ca ColumnArrayType) ColumnIdString() (result string) {
	result = ""
	for index, col := range ca {
		if index == 0 {
			result = strconv.FormatInt(int64(col.Id.Value()), 10)
		} else {
		}
		result = result + "-" + strconv.FormatInt(int64(col.Id.Value()), 10)
	}
	return result
}
func (ca ColumnArrayType) Map() (result map[*dataflow.ColumnInfoType]bool) {
	result = make (map[*dataflow.ColumnInfoType]bool)
	for _, column := range ca {
		result[column] = true
	}
	return
}
func (ca ColumnArrayType) isSubset(another ColumnArrayType) (bool) {
	ext:
	for _,theirColumn:= range another{
		for _,curColumn  := range ca {
			if curColumn.Id.Value() == theirColumn.Id.Value() {
				continue ext
			}
		}
		return false;
	}
	return true
}

type ComplexPKDupDataType struct {
//	ColumnCombinationKey string
	Data                 []*[]byte
	LineNumber           uint64
}

type CPKBitsetBucketType struct {
	dataBitset *sparsebitset.BitSet
	startedFromPosition uint64;
}

type ComplexPKCombinationType struct {
	columns               ColumnArrayType
	columnPositions       []int
	lastSortedColumnIndex int
	cardinality           uint64
	firstBitset       *sparsebitset.BitSet
	duplicateBitset       *sparsebitset.BitSet
	duplicatesByHash      map[uint32][]*ComplexPKDupDataType
	dataBucket []*CPKBitsetBucketType
	hashPool []uint32;
	HashPoolPointer int;
	dataDuplicationFound  bool
}

func (pkc ComplexPKCombinationType) ColumnIndexString() (result string) {
	result = ""
	for index, column := range pkc.columns {
		if index == 0 {
			result = strconv.FormatInt(int64(column.Id.Value()), 10)
		} else {
			result = result + "-" + strconv.FormatInt(int64(column.Id.Value()), 10)
		}
	}
	return result
}


/*

func (pkc ComplexPKCombinationType) Flush(directory string) {

	fileName := pkc.columns.ColumnIdString()

	fullPath := fmt.Sprintf("%v%c",directory, os.PathSeparator)

	err := os.Mkdir(fullPath,0700);
	if err!= nil{

	}

	fullPath = fmt.Sprintf("%v%c%v.cb.data",fullPath )

	file,err := os.OpenFile(fullPath,os.O_APPEND,0700)
	if os.IsNotExist(err) {
		file,err = os.Create(fullPath)
	}


}




func composeBistsetFileFullPath(pathToDir, fileName string) string {
	fullPathFileName := fmt.Sprintf("%v%c%v", pathToDir, os.PathSeparator, fileName)
	return fullPathFileName
}

func (pkc ComplexPKCombinationType) WriteBitsetToDisk(ctx context.Context, pathToDir string) (err error) {
	funcName := "ComplexPKCombinationType.WriteHashBitsetToDisk"

	tracelog.Started(packageName, funcName)

	if pathToDir == "" {
		err = errors.New("Given path to binary dump directory is empty")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	err = os.MkdirAll(pathToDir, 700)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Making directories for path %v", pathToDir)
		return err
	}

	fileName := pkc.columns.ColumnIdString()

	fullPathFileName := composeBistsetFileFullPath(pathToDir, fileName)

	file,err := os.OpenFile(fullPathFileName,os.O_APPEND,0700)
	if os.IsNotExist(err) {
		file, err = os.Create(fullPathFileName)
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Creating file for PKC bitset %v", fullPathFileName)
			return err
		}
	}

	defer file.Close()

	buffered := bufio.NewWriter(file)
	defer buffered.Flush()

	binary.Write(buffered,binary.LittleEndian,pkc.firstPassBuiltFrom)

	_, err = pkc.firstPassBitset.WriteTo(ctx, buffered)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Writing PKC bitset data to file %v", fullPathFileName)
		return err
	}

	tracelog.Completed(packageName, funcName)

	return err
}

func (dataCategory *DataCategoryType) ReadBitsetFromDisk(ctx context.Context, pathToDir string, suffix BitsetFileSuffixType) (err error) {
	funcName := "DataCategoryType.ReadBitsetFromDisk"

	tracelog.Started(packageName, funcName)

	if pathToDir == "" {
		err = errors.New("Given path to binary dump directory is empty")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	fileName, err := dataCategory.BitsetFileName(suffix)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Creating filename for %v bitset %v", suffix, pathToDir)
		return err
	}

	fullPathFileName := composeBistsetFileFullPath(pathToDir, fileName)
	file, err := os.Open(fullPathFileName)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Creating file for %v bitset %v", suffix, fullPathFileName)
		return err
	}

	defer file.Close()

	buffered := bufio.NewReader(file)

	if suffix == HashBitsetSuffix {
		_, err = dataCategory.Stats.HashBitset.ReadFrom(ctx, buffered)
		if err == nil {
			dataCategory.Stats.HashBitsetCardinality = dataCategory.Stats.HashBitset.Cardinality()
		}
	} else {
		_, err = dataCategory.Stats.ItemBitset.ReadFrom(ctx, buffered)
		if err == nil {
			dataCategory.Stats.ItemBitsetCardinality = dataCategory.Stats.ItemBitset.Cardinality()
		}
	}
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Reading %v bitset data to file %v", suffix, fullPathFileName)
		return err
	}

	tracelog.Completed(packageName, funcName)

	return err
}


*/

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
			tracelog.Info(funcName, packageName, "Column %v is not FKColumn. RowCount < 2", colFK)
			return
		}

		categoryCountFK := len(colFK.Categories)
		nonFK = categoryCountFK == 0
		if nonFK {
			tracelog.Info(funcName, packageName, "Column %v is not FKColumn. DataCategory count  = 0", colFK)
			return
		}

		categoryCountPK := len(colPK.Categories)

		nonFK = categoryCountFK == 0 || categoryCountFK > categoryCountPK
		if nonFK {
			tracelog.Info(funcName, packageName, "Column %v is not FKColumn to %v. categoryCountFK > categoryCountPK; %v > %v", colFK, colPK, categoryCountFK, categoryCountPK)
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
								"Column %v is not FKColumn to %v for DataCategory %v: ItemUniqueCountFK > ItemUniqueCountPK; %v > %v",
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
							"Column %v is not FKColumn to %v for DataCategory %v:  MaxNumericValueFK > MaxNumericValuePK; %v > %v",
							colFK, colPK, categoryFK.Key,
							categoryFK.MaxNumericValue.Value(),
							categoryPK.MaxNumericValue.Value())
						return
					}
					nonFK = categoryFK.MinNumericValue.Value() < categoryPK.MinNumericValue.Value()
					if nonFK {
						tracelog.Info(funcName, packageName,
							"Column %v is not FKColumn to %v for DataCategory %v: MinNumericValueFK < MinNumericValuePK; %v < %v",
							colFK, colPK, categoryFK.Key,
							categoryFK.MinNumericValue.Value(),
							categoryPK.MinNumericValue.Value())
						return
					}
				} else {
					nonFK = categoryFK.ItemUniqueCount.Value() > categoryPK.ItemUniqueCount.Value()
					if nonFK {
						tracelog.Info(funcName, packageName,
							"Column %v is not FKColumn to %v for DataCategory %v: ItemUniqueCountFK > ItemUniqueCountPK; %v > %v",
							colFK, colPK, categoryFK.Key,
							categoryFK.ItemUniqueCount.Value(),
							categoryPK.ItemUniqueCount.Value())
						return
					}
					ratio := 1.2
					nonFK = float64(categoryFK.HashUniqueCount.Value()) > float64(categoryPK.HashUniqueCount.Value())*ratio
					if nonFK {
						tracelog.Info(funcName, packageName,
							"Column %v is not FKColumn to %v for DataCategory %v: HashUniqueCountFK > DataCategory.HashUniqueCountPK*ratio(%v); %v > %v",
							colFK, colPK, categoryFK.Key, ratio,
							categoryFK.HashUniqueCount.Value(),
							uint64(float64(categoryPK.HashUniqueCount.Value())*ratio),
						)
						return
					}
				}

			}
		}

		// FKColumn Hash unique count has to be less than PKColumn Hash unique count
		{
			ratio := 1.2
			nonFK = float64(colFK.HashUniqueCount.Value()) > float64(colPK.HashUniqueCount.Value())*ratio
			if nonFK {
				tracelog.Info(funcName, packageName,
					"Column %v is not FKColumn to %v. HashUniqueCountFK > HashUniqueCountPK*ratio(%v); %v > %v",
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
				"Column %v is not PKColumn. TotalRowCount != TotalNotNullCount. %v != %v",
				col, uint64(col.TableInfo.RowCount.Value()), totalNonNullCount,
			)
			return true, nil
		}

		nonPK = col.TotalRowCount.Value() == col.HashUniqueCount.Value()
		if nonPK {
			tracelog.Info(packageName, funcName,
				"Columns %v is not part of a complex PKColumn. set of UniqueHashCount == TotalRowCount. %v == %v",
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
			fmt.Printf("%v.%v is not a single PKColumn", leftColumn.TableInfo, leftColumn.ColumnName);
		} else {
			fmt.Printf("%v.%v is a single PKColumn", leftColumn.TableInfo, leftColumn.ColumnName);
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
						PKColumn: leftColumn,
						FKColumn: rightColumn,
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
						PKColumn: rightColumn,
						FKColumn: leftColumn,
					}
					pairsFilteredByFeatures = append(pairsFilteredByFeatures, pair)
				}
			}

		}
	}

	sort.Slice(
		pairsFilteredByFeatures,
		func(i, j int) bool {
			if pairsFilteredByFeatures[i].PKColumn.HashUniqueCount.Value() == pairsFilteredByFeatures[j].PKColumn.HashUniqueCount.Value() {
				return pairsFilteredByFeatures[i].PKColumn.Id.Value() < pairsFilteredByFeatures[j].PKColumn.HashUniqueCount.Value()
			} else {
				return pairsFilteredByFeatures[i].PKColumn.HashUniqueCount.Value() < pairsFilteredByFeatures[j].PKColumn.HashUniqueCount.Value()
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
				"Column %v is not FKColumn to %v for DataCategory %v: IntersectionCardinality != FkCardinality for Content values %v != %v",
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
				"Column %v is not FKColumn to %v for DataCategory %v: IntersectionCardinality != FkCardinality for Hash values %v != %v",
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
				if lastPKColumn != pair.PKColumn {
					lastPKColumn.ResetBitset(dataflow.ItemBitsetSuffix)
				}
			}
			ctx, ctxCancelFunc := context.WithCancel(context.Background())
			var result = true
			for dataCategoryKey, dataCategoryFK := range pair.FKColumn.Categories {
				if dataCategoryPK, found := pair.PKColumn.Categories[dataCategoryKey]; !found {
					err = fmt.Errorf("The second pass for column pair PKColumn:%v - FKColumn:%v doesn't reveal datacategory for the key code %v.", pair.PKColumn, pair.FKColumn, dataCategoryKey)
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
		pair.PKColumn.ResetBitset(dataflow.ItemBitsetSuffix)
		pair.FKColumn.ResetBitset(dataflow.ItemBitsetSuffix)
	}
	if false {
		for _, pair := range pairsFilteredByContent {
			fmt.Printf("PKColumn:%v(%v) - FKColumn:%v(%v)%v\n", pair.PKColumn, pair.PKColumn.HashUniqueCount, pair.FKColumn, pair.FKColumn.HashUniqueCount, pair.FKColumn.Id)
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
		pair.PKColumn.ResetBitset(dataflow.HashBitsetSuffix)
		pair.FKColumn.ResetBitset(dataflow.HashBitsetSuffix)
	}

	if true {
		fmt.Printf("List of pairs after filtration:\n")
		for _, pair := range pairsFilteredByHash {
			fmt.Printf("PKColumn:%v(%v) - FKColumn:%v(%v)%v\n", pair.PKColumn, pair.PKColumn.HashUniqueCount, pair.FKColumn, pair.FKColumn.HashUniqueCount, pair.FKColumn.Id)
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
			appendToColumnMap(pair.PKColumn)
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
		//columnMap
		/*
				var dataMap map[string][]byte
			var columnToTrack map[int]int

			for table, columns := range columnMap {
				columnToTrack  = make(map[int]int)
				for _, column := range columns {
					//fmt.Printf("PKColumn:%v(%v) - FKColumn:%v(%v)%v\n", pa

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
			pair.PKColumn.UniqueRowCount = pair.PKColumn.HashUniqueCount
			pair.FKColumn.UniqueRowCount = pair.FKColumn.HashUniqueCount
		}

		type tablePairType struct {
			PKTableId, FKTableId int64
		}
		printColumnArray := func(arr []*ComplexPKCombinationType) {
			if len(arr) == 0 {
				fmt.Println("No column combinations left")
				return
			}
			for _, key := range arr {
				fmt.Printf("%v\n", key.columns)
			}
		}
		_ = printColumnArray

		tablePairMap := make(map[tablePairType][]*keyColumnPairType)

		for _, pair := range pairsFilteredByHash {
			/*if !(pair.PKColumn.TableInfo.TableName.Value() == "TX" && pair.FKColumn.TableInfo.TableName.Value() == "TX_ITEM") {
				continue
			}*/
			tablePair := tablePairType{
				PKTableId: pair.PKColumn.TableInfo.Id.Value(),
				FKTableId: pair.FKColumn.TableInfo.Id.Value(),
			}
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

		ComplexPKStage1 := make([]*ComplexPKCombinationType, 0, 10)
		tableCPKs := make(map[*dataflow.TableInfoType]map[string]*ComplexPKCombinationType)
		for _, columnPairs := range tablePairMap {
			if len(columnPairs) > 1 {
				fmt.Printf("\nPKColumn:%v - FKColumn:%v:\n", columnPairs[0].PKColumn.TableInfo, columnPairs[0].FKColumn.TableInfo)

				//* Collecting possible PK columns
				uniqueColumns := make(map[*dataflow.ColumnInfoType]bool)

				for _, columnPair := range columnPairs {
					uniqueColumns[columnPair.PKColumn] = true
				}

				var SortedPKColumns []*dataflow.ColumnInfoType
				SortedPKColumns = make([]*dataflow.ColumnInfoType, 0, len(uniqueColumns))

				for column := range uniqueColumns {
					SortedPKColumns = append(SortedPKColumns, column)
				}

				//* Sort possible PK columns from the highest data cardinality
				sort.Slice(SortedPKColumns, func(i, j int) bool {
					if SortedPKColumns[i].UniqueRowCount.Value() == SortedPKColumns[j].UniqueRowCount.Value() {
						return SortedPKColumns[i].Id.Value() > SortedPKColumns[j].Id.Value()
					} else {
						return SortedPKColumns[i].UniqueRowCount.Value() > SortedPKColumns[j].UniqueRowCount.Value()
					}
				},
				)

				//* Making PK column combinations

				for {
					if len(SortedPKColumns) == 0 {
						break
					}
					var inputColumnCombinations []*ComplexPKCombinationType
					var CPKeysLast int

					inputColumnCombinations = make([]*ComplexPKCombinationType, 1, len(SortedPKColumns))
					inputColumnCombinations[0] = &ComplexPKCombinationType{
						columns:               make([]*dataflow.ColumnInfoType, 1),
						lastSortedColumnIndex: 1,
					}
					inputColumnCombinations[0].columns[0] = SortedPKColumns[0]
					//1,2,3,4 -> 12,13,14,123,124,134,23,24,234,34

					for {
						CPKeysLast = len(ComplexPKStage1)
						for _, inputColumnCombination := range inputColumnCombinations {
							var columnCombination *ComplexPKCombinationType
							inputLength := len(inputColumnCombination.columns)
							for index := inputColumnCombination.lastSortedColumnIndex; index < len(SortedPKColumns); index++ {
								columnCombination = &ComplexPKCombinationType{
									columns:               make([]*dataflow.ColumnInfoType, inputLength, inputLength+1),
									lastSortedColumnIndex: index + 1,
								}
								copy(columnCombination.columns, inputColumnCombination.columns)
								columnCombination.columns = append(columnCombination.columns, SortedPKColumns[index])
								ComplexPKStage1 = append(ComplexPKStage1, columnCombination)
							}
						}
						inputColumnCombinations = ComplexPKStage1[CPKeysLast:]
						if len(inputColumnCombinations) == 0 {
							break
						}
					}
					SortedPKColumns = SortedPKColumns[1:]
				}
			}

			for _, columnCombination := range ComplexPKStage1 {
				for index, column := range columnCombination.columns {
					if index == 0 {
						columnCombination.cardinality = uint64(column.UniqueRowCount.Value())
					} else {
						columnCombination.cardinality = columnCombination.cardinality * uint64(column.UniqueRowCount.Value())
					}
				}
				if columnCombination.cardinality < uint64(columnCombination.columns[0].TableInfo.RowCount.Value()) {
					/* tracelog.Info(packageName, funcName,
						"Data volume of column permutation %v is insufficient to fill the table row count. %v < %v",
						columnCombination.columns,
						columnCombination.cardinality,
						columnCombination.columns[0].TableInfo.RowCount.Value(),
					) */
					continue
				}
				table := columnCombination.columns[0].TableInfo
				//

				if collectedColumnCombinations, cccFound := tableCPKs[table]; !cccFound {
					collectedColumnCombinations = make(map[string]*ComplexPKCombinationType)
					collectedColumnCombinations[columnCombination.ColumnIndexString()] = columnCombination
					tableCPKs[table] = collectedColumnCombinations
				} else {
					collectedColumnCombinations[columnCombination.ColumnIndexString()] = columnCombination
					tableCPKs[table] = collectedColumnCombinations
				}
			}
		}
		if len(tableCPKs) == 0 {
		}

		for _, columnCombinationMap := range tableCPKs {
			for _, columnCombination := range columnCombinationMap {
				columnCombination.firstBitset = sparsebitset.New(0)
				columnCombination.duplicateBitset = sparsebitset.New(0)
				columnCombination.duplicatesByHash = make(map[uint32][]*ComplexPKDupDataType)
				columnCombination.columnPositions = make([]int, len(columnCombination.columns))
				for keyColumnIndex, pkc := range columnCombination.columns {
					for tableColumnIndex := 0; tableColumnIndex < len(pkc.TableInfo.Columns); tableColumnIndex++ {
						if pkc.Id.Value() == pkc.TableInfo.Columns[tableColumnIndex].Id.Value() {
							columnCombination.columnPositions[keyColumnIndex] = tableColumnIndex
						}
					}
				}

			}
		}
		//	fmt.Printf("%v\n", columnCombination.columns)

		//bs := sparsebitset.New(0)
		for currentPkTable, columnCombinationMap := range tableCPKs {
			if currentPkTable.TableName.Value() != "TX_ITEM" {
				continue //_ITEM_REVERSED
			}
			cumulativeSavedDataLength := uint64(0)
			LineNumberToCheckBySlaveHorseTo := uint64(0)
			leadChan, slaveChan := make(chan interface{}), make(chan interface{})

			leadHorseConfig := &dataflow.TableDumpConfigType{
				GZip:            dr.Config.AstraDataGZip,
				Path:            dr.Config.AstraDumpPath,
				LineSeparator:   dr.Config.AstraLineSeparator,
				ColumnSeparator: dr.Config.AstraColumnSeparator,
				BufferSize:      dr.Config.AstraReaderBufferSize,
			}
			slaveHorseConfig := new(dataflow.TableDumpConfigType)
			(*slaveHorseConfig) = (*leadHorseConfig)
			var leadHorseResult dataflow.ReadDumpResultType
			var slaveHorseResult dataflow.ReadDumpResultType
			var columnCombinationMapForFirstPass map[string]*ComplexPKCombinationType
			var columnCombinationMapForSecondPass map[string]*ComplexPKCombinationType

			horsesContext, horsesCancelFunc := context.WithCancel(context.Background())


			/*
			var  buckets map[string]*CPKBitsetBucketType;
			var LineNumberLimit uint64 = 1000000
			var hits uint64 = uint64(0)
			var loses uint64 = uint64(0)

			SlicingHorse :=  func(ctc context.Context, LineNumber, DataPosition uint64, data [][]byte) (result dataflow.ReadDumpActionType, err error) {
				 if LineNumber >= LineNumberLimit {
					 for columnCombinationMapKey, columnCombination := range columnCombinationMapToCheck {
						 if columnCombination.dataBucket == nil {
							 columnCombination.dataBucket = make([]*CPKBitsetBucketType, 0, 50)
						 }
						 columnCombination.dataBucket = append(columnCombination.dataBucket, buckets[columnCombinationMapKey])
						 buckets[columnCombinationMapKey] = nil

					 }
					 LineNumberLimit = LineNumberLimit +1000000;
					 fmt.Println(LineNumberLimit,hits,loses )

				 }

				hashMethod := fnv.New64a()
				for columnCombinationMapKey, columnCombination := range columnCombinationMapToCheck {
					hashMethod.Reset()
					for _, position := range columnCombination.columnPositions {
						hashMethod.Write(data[position])
					}
					hashValue := hashMethod.Sum64()

					if buckets[columnCombinationMapKey] == nil {
						v := &CPKBitsetBucketType {dataBitset:sparsebitset.New(0),
							startedFromPosition:DataPosition,
						}
						buckets[columnCombinationMapKey] = v
						loses = 0;
						hits = 0
					}

					if buckets[columnCombinationMapKey].dataBitset.Set(uint64(hashValue)) {
						if hashValue == 2805099685805779626 {
							for _, position := range columnCombination.columnPositions {
								fmt.Print(data[position])
								fmt.Print(" ")
							}
							fmt.Println()
						}
						hits ++;
					} else {
						loses++;
					}
				}

				return dataflow.ReadDumpActionContinue, nil
			}
			*/

			/*
			VanguardHorse :=  func(ctc context.Context, LineNumber, DataPosition uint64, data [][]byte) (result dataflow.ReadDumpActionType, err error) {
				var truncateCombinations bool
				var copiedDataMap map[int]*[]byte
				copiedDataMap = make(map[int]*[]byte)
				if int(math.Mod(float64(LineNumber),float64(100000))) == 0 {
					fmt.Println(LineNumber,len(columnCombinationMapToCheck))
				}
			columns:
				for columnCombinationMapKey, columnCombination := range columnCombinationMapToCheck {
					hashMethod := fnv.New32()
					for _, position := range columnCombination.columnPositions {
						hashMethod.Write(data[position])
					}

					hashValue := hashMethod.Sum32()
					if !columnCombination.firstBitset.Set(uint64(hashValue )) {
						continue
					}


					pData := make([]*[]byte,len(columnCombination.columns))
					addToDuplicateByHash := func(duplicates []*ComplexPKDupDataType) {
						if len(columnCombination.hashPool) < cap(columnCombination.hashPool) {
							columnCombination.hashPool = append(columnCombination.hashPool,hashValue)
						} else {
							columnCombination.HashPoolPointer++
							if columnCombination.HashPoolPointer == len(columnCombination.hashPool) {
								columnCombination.HashPoolPointer = 0;
							}
							prevHashValue := columnCombination.hashPool[columnCombination.HashPoolPointer]
							columnCombination.firstBitset.Clear(uint64(prevHashValue))
							delete(columnCombination.duplicatesByHash,uint32(prevHashValue))
							columnCombination.hashPool[columnCombination.HashPoolPointer] = hashValue
							runtime.GC()
						}

						newDup := &ComplexPKDupDataType{
							//dup.ColumnCombinationKey == columnCombinationMapKey
							Data:                 make([]*[]byte, len(columnCombination.columns)),
							LineNumber:           LineNumber,
						}
						for index, position := range columnCombination.columnPositions {
							pointer := pData[index]
							if  pointer != nil {
								newDup.Data[index] = pointer
							} else {
								if dataCopyRef, isDataCopied := copiedDataMap[position]; !isDataCopied {
									dataLength := len(data[position])
									cumulativeSavedDataLength = cumulativeSavedDataLength + 24 + uint64(dataLength) + 16
									//cumulativeSavedDataLength = cumulativeSavedDataLength + unsafe.Sizeof(newDup)
									dataCopy := make([]byte, dataLength)
									copy(dataCopy, data[position])
									copiedDataMap[position] = &dataCopy
									newDup.Data[index] = &dataCopy
								} else {
									newDup.Data[index] = dataCopyRef
								}
							}
						}
						duplicates = append(duplicates, newDup)
						columnCombination.duplicatesByHash[hashValue] = duplicates
					}

					if duplicates, found := columnCombination.duplicatesByHash[hashValue]; !found {
						duplicates = make([]*ComplexPKDupDataType, 0, 50)
						addToDuplicateByHash(duplicates)
					} else {
						for _, dup := range duplicates {
							countDifferentPieces := 0
							//if dup.ColumnCombinationKey == columnCombinationMapKey {
							for index, position := range columnCombination.columnPositions {
								var result int
								if len(*dup.Data[index]) == len(data[position]) {
									result = bytes.Compare(*dup.Data[index], data[position])
								} else {
									result = 1
								}
								if result != 0 {
									countDifferentPieces++
									//break;
								} else {
									if pData[index] == nil{
										pData[index] = dup.Data[index]
									}
								}
							}
							if countDifferentPieces == 0 {
								truncateCombinations = true
								tracelog.Info(packageName, funcName, "Vanguard Horse:Data duplication found for columns %v in lines %v and %v", columnCombination.columns, dup.LineNumber, LineNumber)
								columnCombination.duplicatesByHash = nil
								removedKeys[columnCombinationMapKey] = true
								delete(columnCombinationMapToCheck, columnCombinationMapKey)
								runtime.GC()
								continue columns
							}
						}
						addToDuplicateByHash(duplicates)
					}
				}

				if truncateCombinations {
					if len(columnCombinationMap) == 0 {
						tracelog.Info(packageName, funcName,
							"There is no column combination available for %v to check",
							currentPkTable,
						)
						return dataflow.ReadDumpActionAbort, nil
					}
				}
				return dataflow.ReadDumpActionContinue, nil
			} */

			LeadHorse := func(ctc context.Context, LineNumber, DataPosition uint64, data [][]byte) (result dataflow.ReadDumpActionType, err error) {
				var truncateCombinations = false
				if LineNumber == 0 {
					fmt.Println("Column combination(s) to check duplicates:")
					for _, columnCombination := range columnCombinationMap {
						fmt.Printf("--%v\n ", columnCombination.columns)
						columns := make([]string,0,len(columnCombination.columns))
						for _, c := range columnCombination.columns {
							columns = append(columns,c.ColumnName.Value())
						}
						s := strings.Join(columns,", ")
						fmt.Printf("select '%v' from dual where not exists (select %v,count(*) as ccount from %v.%v group by %v having count(*)>1) union all \n ",
							s,s,currentPkTable.SchemaName.Value(),
								currentPkTable.TableName.Value(),s)
					}
					fmt.Printf("\n")
				}

				if cumulativeSavedDataLength > 1024*1024 {
					LineNumberToCheckBySlaveHorseTo = LineNumber
					for columnCombinationKey, columnCombination := range columnCombinationMap {
						if len(columnCombination.duplicatesByHash) > 0 {
							columnCombinationMapForSecondPass[columnCombinationKey] = columnCombination
						}
						fmt.Printf("%v.duplicatesByHash %v\n",columnCombination.columns,unsafe.Sizeof(columnCombination.duplicatesByHash))

					}
					///

					slaveChan <- true
					<-leadChan
					tracelog.Info(packageName, funcName, "Lead Horse continues processing %v from line %v with %v column combinations",
						currentPkTable, LineNumber,
						len(columnCombinationMap),
					)
					/*leadHorseConfig.MoveToByte.Position = DataPosition
					leadHorseConfig.MoveToByte.FirstLineAs = LineNumber
					for columnCombinationKey, columnCombination := range (columnCombinationMap) {
							if len(columnCombination.duplicatesByHash)>0 {
								columnCombinationMapToCheck[columnCombinationKey] = columnCombination
							}
					}
					return dataflow.ReadDumpActionAbort, nil*/
				}
			var copiedDataMap map[int]*[]byte
			copiedDataMap = make(map[int]*[]byte)
			firstHashMethod :=  fnv.New32()
			columns:
				for columnCombinationMapKey, columnCombination := range columnCombinationMap {
					firstHashMethod.Reset()
					for _, position := range columnCombination.columnPositions {
						firstHashMethod.Write(data[position])
					}
					hv1 := firstHashMethod.Sum32();



					if !columnCombination.firstBitset.Set(uint64(hv1)) {
						continue
					}

					secondHashMethod := fnv.New32a()
					for _, position := range columnCombination.columnPositions {
						secondHashMethod.Write(data[position])
					}

					hashValue := secondHashMethod.Sum32()

					newDuplicate:= !columnCombination.duplicateBitset.Set(uint64(hashValue))

					pData := make([]*[]byte,len(columnCombination.columns))
					addToDuplicateByHash := func(duplicates []*ComplexPKDupDataType) {
						newDup := &ComplexPKDupDataType{
//							ColumnCombinationKey: columnCombinationMapKey,
							Data:                 make([]*[]byte, len(columnCombination.columns)),
							LineNumber:           LineNumber,
						}
						// Rough estimation of memory consumption
						//8 -  ref to columnCombinationMapKey
						//24+8N: (Slice internally (3*8)+ 8*count of Ref to columns)
						//8 - lineNumber
						//8 - ref for columnCombination.duplicatesByHash
						//8 - hashValue
						//24 +len(dataCopy):
						//16 ~  Map system internals
						cumulativeSavedDataLength = cumulativeSavedDataLength +
							uint64(8+24+8*len(columnCombination.columns)+8+8+8)
						for index, position := range columnCombination.columnPositions {
							pointer := pData[index]
							if  pointer != nil {
								newDup.Data[index] = pointer
							} else {
								if dataCopyRef, isDataCopied := copiedDataMap[position]; !isDataCopied {
									dataLength := len(data[position])
									cumulativeSavedDataLength = cumulativeSavedDataLength + 24 + uint64(dataLength) + 16
									//fmt.Printf("%v\n",unsafe.Sizeof(*newDup))

									dataCopy := make([]byte, dataLength)
									copy(dataCopy, data[position])
									copiedDataMap[position] = &dataCopy
									newDup.Data[index] = &dataCopy
								} else {
									newDup.Data[index] = dataCopyRef
								}
							}
						}
						duplicates = append(duplicates, newDup)
						columnCombination.duplicatesByHash[hashValue] = duplicates
					}

					if newDuplicate {
						duplicates := make([]*ComplexPKDupDataType, 0, 3)
						addToDuplicateByHash(duplicates)
						if false {
							fmt.Printf("--------%v ---\n", hv1)
							for _, position := range columnCombination.columnPositions {
								fmt.Printf("%v, ", data[position])
							}
							fmt.Printf("++++++\n")
						}
					} else if duplicates, found := columnCombination.duplicatesByHash[hashValue]; !found {
						duplicates = make([]*ComplexPKDupDataType, 0, 3)
						addToDuplicateByHash(duplicates)
					} else {
						for _, dup := range duplicates {
							countDifferentPieces := 0
							//if dup.ColumnCombinationKey == columnCombinationMapKey {
								for index, position := range columnCombination.columnPositions {
									var result int
									if len(*dup.Data[index]) == len(data[position]) {
										result = bytes.Compare(*dup.Data[index], data[position])
										/*if result == 0 {
											fmt.Println(*dup.Data[index], data[position])
										}*/
									} else {
										result = 1
									}
									if result != 0 {
										countDifferentPieces++
										//break;
									} else {
										if pData[index] == nil{
											pData[index] = dup.Data[index]
										}
									}
								}
								if countDifferentPieces == 0 {
									truncateCombinations = true
									tracelog.Info(packageName, funcName, "Lead Horse:Data duplication found for columns %v in lines %v and %v", columnCombination.columns, dup.LineNumber, LineNumber)
									delete(columnCombinationMapForFirstPass, columnCombinationMapKey)
									continue columns
								} else {
									fmt.Printf("--------%v ---\n",hashValue)
									for index, position := range columnCombination.columnPositions {
										fmt.Printf("%v:\n%v\n%v\n",bytes.Compare(*dup.Data[index], data[position]),*dup.Data[index],data[position])
									}
									fmt.Printf("++++++\n")

								}
							//}
						}
						addToDuplicateByHash(duplicates)
					}
				}

				if truncateCombinations {
					if len(columnCombinationMap) == 0 {
						tracelog.Info(packageName, funcName,
							"There is no column combination available for %v to check",
							currentPkTable,
						)
						return dataflow.ReadDumpActionAbort, nil
					}
				}
				return dataflow.ReadDumpActionContinue, nil
			}

			SlaveHorse := func(ctc context.Context, LineNumber, DataPosition uint64, data [][]byte) (result dataflow.ReadDumpActionType, err error) {
				var truncateCombinations = false
				if LineNumberToCheckBySlaveHorseTo == LineNumber {
					return dataflow.ReadDumpActionAbort, nil
				}
				firstHashMethod :=  fnv.New32()
				secondHashMethod := fnv.New32a()
			columns:
				for columnCombinationMapKey, columnCombination := range columnCombinationMapForSecondPass {
					firstHashMethod.Reset()
					secondHashMethod.Reset()

					for _, position := range columnCombination.columnPositions {
						secondHashMethod.Write(data[position])
					}
					hashValue := secondHashMethod.Sum32()
					/*if LineNumber == 341932 && currentPkTable.TableName.Value() == "TX_ITEM" {
						fmt.Printf("!!: %v%v\n",columnCombination.columns,hashValue)
						if hashValue == 3509682044 {
							fmt.Println(-1)
						}
						if hashValue == 353756200 {
							fmt.Println(-2)
						}
						if hashValue == 374838707 {
							fmt.Println(-3)
						}

					}*/
					if !columnCombination.duplicateBitset.Test(uint64(hashValue)) {
						continue
					}
					firstHashMethod.Reset()

					for _, position := range columnCombination.columnPositions {
						firstHashMethod.Write(data[position])
					}
					if !columnCombination.firstBitset.Test(uint64(firstHashMethod.Sum32()))  {
						continue
					}

					if duplicates, found := columnCombination.duplicatesByHash[hashValue]; found {
						for _, dup := range duplicates {
							countDifferentPieces := 0
							if dup.LineNumber == LineNumber {
								continue
							}
								for index, position := range columnCombination.columnPositions {
									result := bytes.Compare(*dup.Data[index], data[position])
									if result != 0 {
										countDifferentPieces++
										break;
									}
								}
								if countDifferentPieces == 0 {
									// TODO:DUPLICATE!
									truncateCombinations = true
									tracelog.Info(packageName, funcName, "Slave Horse:Data duplication found for columns %v in lines %v and %v", columnCombination.columns, LineNumber, dup.LineNumber)
									delete(columnCombinationMap, columnCombinationMapKey)
									delete(columnCombinationMapForFirstPass, columnCombinationMapKey)
									delete(columnCombinationMapForSecondPass, columnCombinationMapKey)
									continue columns
								}
						}
					}
				}

				if truncateCombinations {
					/*for columnCombinationKey, columnCombination := range (columnCombinationMap) {
						if columnCombination.dataDuplicationFound {
							delete(columnCombinationMap, columnCombinationKey)
							runtime.GC()
						}

					}*/
					if len(columnCombinationMapForSecondPass) == 0 {
						tracelog.Info(packageName, funcName,
							"There is no column combination available for %v to check",
							currentPkTable,
						)
						return dataflow.ReadDumpActionAbort, nil
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


			/*if false {
				columnCombinationMapToCheck = make(map[string]*ComplexPKCombinationType)

				for columnCombinationMapKey, c := range columnCombinationMap {
					columnCombinationMapToCheck[columnCombinationMapKey] = c
				}
				buckets = make(map[string]*CPKBitsetBucketType)

				dr.ReadAstraDump(
					horsesContext,
					currentPkTable,
					SlicingHorse,
					leadHorseConfig,
				)
				return
			}*/

			/*if false {


				keys := make([]string, 0, len(columnCombinationMap))
				for columnCombinationMapKey, _ := range columnCombinationMap {
					keys = append(keys, columnCombinationMapKey)
				}
				sort.Slice(keys, func(i, j int) bool {
					if len(columnCombinationMap[keys[i]].columns) == len(columnCombinationMap[keys[j]].columns) {
						return keys[i] > keys[j]
					} else {
						return len(columnCombinationMap[keys[i]].columns) > len(columnCombinationMap[keys[j]].columns)
					}
				})

				for {
					var allProcessed bool = true;
					tracelog.Info(packageName,funcName,
						"Column combinations: total %v, found duplicates in:%v, ready for the next test: %v, leftover: %v;  ",
						len(keys),len(removedKeys),len(passedKeys),
						len(keys) - len(removedKeys) - len(passedKeys),
						)
					for _, key := range keys {
						if _, found := passedKeys[key]; found {
							continue;
						}
						if _, found := removedKeys[key]; found {
							continue;
						}
						allProcessed = false;
						break;
					}
					if allProcessed {
						break;
					}

					var count int = -1;
					var columnsAdded int = 0
					columnCombinationMapToCheck = make(map[string]*ComplexPKCombinationType)

					for _, key := range keys {
						if _, found := passedKeys[key]; found {
							continue;
						}
						if _, found := removedKeys[key]; found {
							continue;
						}
						var found bool = false;
						for removedKey,_ := range removedKeys {
							if columnCombinationMap[key].columns.isSubset(columnCombinationMap[removedKey].columns) {
								tracelog.Info(packageName,funcName,
									"Combination %v is subset of already checked %v. Skipped ",
									columnCombinationMap[key].columns,
									columnCombinationMap[removedKey].columns,
									)
								removedKeys[key] = true
								found = true
							}
						}
						if !found {
							count  = len(columnCombinationMap[key].columns)
							if count + columnsAdded <= 16 {
								columnsAdded = columnsAdded +count
								columnCombinationMapToCheck[key] = columnCombinationMap[key]
								fmt.Println(columnCombinationMap[key].columns)
							} else{
								break;
							}
						}
					}
					if len(columnCombinationMapToCheck) == 0 {
						continue;
					}
					tracelog.Info(packageName, funcName, "Vanguard Horse starts processing %v  with %v column combinations",
						currentPkTable, len(columnCombinationMapToCheck),
					)

					{
						var columns map[int64]bool = make(map[int64]bool)
						for _,columnCombination := range(columnCombinationMapToCheck) {
							for _,column := range columnCombination.columns {
								columns[column.Id.Value()] = true
							}
							columnCombination.HashPoolPointer = -1;
							columnCombination.hashPool = make([]uint32, 0, len(columns)*256)
			}
					}

					dr.ReadAstraDump(
						horsesContext,
						currentPkTable,
						VanguardHorse,
						leadHorseConfig,
					)


					if len(columnCombinationMapToCheck) > 0 {
						for key, columnCombination := range columnCombinationMapToCheck {
							passedKeys[key] = true
							columnCombination.firstBitset = nil
							columnCombination.duplicateBitset = nil
							columnCombination.hashPool = nil
							columnCombination.duplicatesByHash = nil

						}
					}
				}
				for key, _:= range columnCombinationMap{
					if _,found := removedKeys[key]; found {
						delete(columnCombinationMap,key)
					}
				}
			} */


			if len(columnCombinationMap) > 0{
				for _, columnCombination := range columnCombinationMap {
					columnCombination.firstBitset = sparsebitset.New(0)
					columnCombination.duplicateBitset = sparsebitset.New(0)
					columnCombination.duplicatesByHash = make(map[uint32][]*ComplexPKDupDataType)
				}
			}

			cumulativeSavedDataLength = 0

			go func() {
				processedKeys := make(map[string]bool)


				keyList := make([]string, 0, len(columnCombinationMap))
				for columnCombinationMapKey, _ := range columnCombinationMap {
					keyList = append(keyList, columnCombinationMapKey)
				}

				sort.Slice(keyList, func(i, j int) bool {
					if len(columnCombinationMap[keyList[i]].columns) == len(columnCombinationMap[keyList[j]].columns) {
						return keyList[i] > keyList[j]
					} else {
						return len(columnCombinationMap[keyList[i]].columns) > len(columnCombinationMap[keyList[j]].columns)
					}
				})



				for {
					var allProcessed bool = true;
					{ var removed,processed int = 0,0
						for _, exists := range processedKeys {
							if exists {
								processed++
							} else {
								removed++
							}
						}

						tracelog.Info(packageName,funcName,
							"Column combinations: total %v, found duplicates in: %v, ready for exact test: %v, leftover: %v;  ",
							len(keyList),removed, processed,
							len(keyList) - removed - processed,
						)
					}


					for _, key := range keyList {
						if _, found := processedKeys[key]; found {
							continue;
						}
						allProcessed = false;
						break;
					}
					if allProcessed {
						break;
					}

					var count int = -1;
					var columnsAdded int = 0


					for removedKey, passed := range removedKeys {
						if !passed {
							for key := range columnCombinationMap {
								if columnCombinationMap[key].columns.isSubset(columnCombinationMap[removedKey].columns) {
									tracelog.Info(packageName,funcName,
										"Combination %v is subset of already checked %v. Skipped ",
										columnCombinationMap[key].columns,
										columnCombinationMap[removedKey].columns,
									)
									processedKeys[key] = false
								}
							}
						}
					}

					columnCombinationMapForFirstPass = make(map[string]*ComplexPKCombinationType)
					columnCombinationMapForSecondPass = make(map[string]*ComplexPKCombinationType)
					processingKeys := make(map[string]bool)

					for _, key := range keyList {
						if  _, found := processedKeys[key]; found {
							continue;
						}
						count  = len(columnCombinationMap[key].columns)
						if count + columnsAdded <= 16 {
							columnsAdded = columnsAdded +count
							columnCombinationMapForFirstPass[key] = columnCombinationMap[key]
							processingKeys[key] = true
							fmt.Println(columnCombinationMap[key].columns)
						} else{
							break;
						}
					}


					if len(columnCombinationMapForFirstPass) == 0 {
						continue;
					}
					tracelog.Info(packageName, funcName, "Leading Horse starts processing %v  with %v column combinations",
						currentPkTable, len(columnCombinationMapForFirstPass),
					)


					dr.ReadAstraDump(
						horsesContext,
						currentPkTable,
						LeadHorse,
						leadHorseConfig,
					)

					for key := range processingKeys {
						_, exists := columnCombinationMapForFirstPass[key]
						processedKeys[key] = exists
						if !exists {
							delete(columnCombinationMap,key)
						}

					}

					if len(columnCombinationMapForSecondPass) > 0 {
						slaveChan <- true
						<-leadChan
					}

				}


				tracelog.Info(packageName, funcName, "Lead Horse starts processing %v  with %v column combinations",
					currentPkTable, len(columnCombinationMap),
				)


				result, _, err := dr.ReadAstraDump(
					horsesContext,
					currentPkTable,
					LeadHorse,
					leadHorseConfig,
				)
				if len(columnCombinationMapToCheck) > 0 {
					slaveChan <- true
					<-leadChan
				}
				slaveChan <- false
				slaveChan <- result
				slaveChan <- err
				close(slaveChan)

				return
			}()

		horses2:
			for {
				if unresolved, open := <-slaveChan; open {
					switch continued :=  unresolved.(type) {
					case bool:
						if !continued {
							unresolved	= <- slaveChan
							err,_ = unresolved.(error)
							leadHorseResult,_= unresolved.(dataflow.ReadDumpResultType)
							columnCombinationMapToCheck = nil
							close(leadChan)
						}

					}
				}

			/*chanLoop:
				for {
					if unresolved, open := <-slaveChan; open {
						switch val := unresolved.(type) {
						case bool:
							if !val {
								break horses2
							} else {
								break chanLoop
							}
						case error:
							err = val
							if err != nil {
								return err
							}
						case dataflow.ReadDumpResultType:
							leadHorseResult = val
						}
					}

				}*/

				if columnCombinationMapToCheck!=nil && len(columnCombinationMapToCheck) > 0 {
					tracelog.Info(packageName, funcName, "Slave Horse works on %v up to the line %v with %v column combinations", currentPkTable, LineNumberToCheckBySlaveHorseTo, len(columnCombinationMapToCheck))

					/*fmt.Println("Column combination(s) of checking duplicates:")
					for _, columnCombination := range (columnCombinationMapToCheck) {
						fmt.Printf("%v,%v [", columnCombination.columns, len(columnCombination.duplicatesByHash))
						for h, v := range columnCombination.duplicatesByHash {
							fmt.Printf("%v:", h)
							for _, a := range v {
								fmt.Printf("%v,", a.LineNumber)
							}
						}
						fmt.Printf("]\n")
					}
					fmt.Printf("\n\n")*/

					slaveHorseResult, _, err = dr.ReadAstraDump(
						horsesContext,
						currentPkTable,
						SlaveHorse,
						slaveHorseConfig,
					)

					if err != nil {
						tracelog.Errorf(err, packageName, funcName, "err!")
						horsesCancelFunc()
						//if _, open := <-slaveChan; open {

							cumulativeSavedDataLength = 0
							leadChan <- true
//						} else {
//							close(leadChan)
						//}
						return err
					} else {
						for _, columnCombination := range columnCombinationMap {
							columnCombination.duplicateBitset = sparsebitset.New(0)
							columnCombination.duplicatesByHash = make(map[uint32][]*ComplexPKDupDataType)
							runtime.GC()
						}
						//if _, open := <-slaveChan; open {
							cumulativeSavedDataLength = 0
							leadChan <- true
						//} else {
//							close(leadChan)
//							break horses2
//						}
					}
				} else {
					slaveHorseResult = dataflow.ReadDumpResultOk
					break horses2
				}
			}
			if leadHorseResult == dataflow.ReadDumpResultOk && slaveHorseResult == dataflow.ReadDumpResultOk {

			}

			if false {
			horses:
				for {
					cumulativeSavedDataLength = 0
					columnCombinationMapToCheck = make(map[string]*ComplexPKCombinationType)

					if leadHorseConfig.MoveToByte.FirstLineAs == 0 {
						tracelog.Info(packageName, funcName, "Lead horse starts processing %v  with %v column combinations",
							currentPkTable, len(columnCombinationMap),
						)

					} else {
						tracelog.Info(packageName, funcName, "Lead horse continues processing %v from line %v with %v column combinations",
							currentPkTable, leadHorseConfig.MoveToByte.FirstLineAs,
							len(columnCombinationMap),
						)
					}
					if len(columnCombinationMap) > 0 {
						/*fmt.Println("Column combination(s) of checking duplicates:")
						for _, columnCombination := range (columnCombinationMap) {
							fmt.Printf("%v,%v\n",columnCombination.columns,len(columnCombination.duplicatesByHash))
						}
						fmt.Printf("\n\n")*/
						leadHorseResult, LineNumberToCheckBySlaveHorseTo, err = dr.ReadAstraDump(
							context.TODO(),
							currentPkTable,
							LeadHorse,
							leadHorseConfig,
						)
						if err != nil {
							tracelog.Errorf(err, packageName, funcName, "err!")
							return err
						}
					} else {
						leadHorseResult = dataflow.ReadDumpResultOk
					}

					switch leadHorseResult {
					case dataflow.ReadDumpResultOk, dataflow.ReadDumpResultAbortedByRowProcessing:
						if leadHorseResult == dataflow.ReadDumpResultOk {
							//TODO: to check if needed
							LineNumberToCheckBySlaveHorseTo = LineNumberToCheckBySlaveHorseTo + 1
						}
						if len(columnCombinationMapToCheck) > 0 {
							tracelog.Info(packageName, funcName, "Slave horse is seeking for duplicates on %v up to the line %v with %v column combinations", currentPkTable, LineNumberToCheckBySlaveHorseTo, len(columnCombinationMapToCheck))

							/*fmt.Println("Column combination(s) of checking duplicates:")
							for _, columnCombination := range (columnCombinationMapToCheck) {
								fmt.Printf("%v,%v [", columnCombination.columns, len(columnCombination.duplicatesByHash))
								for h, v := range columnCombination.duplicatesByHash {
									fmt.Printf("%v:", h)
									for _, a := range v {
										fmt.Printf("%v,", a.LineNumber)
									}
								}
								fmt.Printf("]\n")
							}
							fmt.Printf("\n\n") */

							slaveHorseResult, _, err = dr.ReadAstraDump(
								context.TODO(),
								currentPkTable,
								SlaveHorse,
								slaveHorseConfig,
							)
							if err != nil {
								tracelog.Errorf(err, packageName, funcName, "err!")
								return err
							}
						} else {
							slaveHorseResult = dataflow.ReadDumpResultOk
						}

						if len(columnCombinationMap) == 0 {
							tracelog.Info(packageName, funcName, "len(columnCombinationMap) == 0")
							break horses
						}

						for _, columnCombination := range columnCombinationMap {
							columnCombination.duplicateBitset = sparsebitset.New(0)
							columnCombination.duplicatesByHash = make(map[uint32][]*ComplexPKDupDataType)
							runtime.GC()
						}

						if leadHorseResult == dataflow.ReadDumpResultOk && slaveHorseResult == dataflow.ReadDumpResultOk {
							break horses
						}

					}
				}
			}
			if len(columnCombinationMap) > 0 {
				fmt.Println("Column combination(s) after checking duplicates:")
				for _, columnCombination := range columnCombinationMap {
					fmt.Println(columnCombination.columns)
				}
				fmt.Printf("\n\n")
			}
		}
		/*
			if len(columnCombinationMap) > 0 {


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
			}*/

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

		//fmt.Printf("\nPKColumn:%v - FKColumn:%v:\n", columnPairs[0].PKColumn.TableInfo, columnPairs[0].FKColumn.TableInfo)
		//printColumnArray(columnCombinationMap)

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
			fmt.Printf("PKColumn:%v(%v) - FKColumn:%v(%v)%v\n", pair.PKColumn, pair.PKColumn.HashUniqueCount, pair.FKColumn, pair.FKColumn.HashUniqueCount, pair.FKColumn.Id)
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
