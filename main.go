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
	"io"
	"sort"
	"astra/utils"
	//"github.com/couchbase/moss"

	"hash/fnv"
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

func (pair keyColumnPairType) TablePairKeyString() string {
	return strconv.FormatInt(int64(pair.PKColumn.TableInfo.Id.Value()), 16) +
		"-" +
		strconv.FormatInt(int64(pair.FKColumn.TableInfo.Id.Value()), 16)
}


/*

func (pkc ComplexPKCombinationType) SameColumnLengthsAt(columnLengths []int) (at int) {
	var combinationFound = false
	at = -1;
	if len(columnLengths) == 0 || len(pkc.duplicateLengths)==0 {
		return
	}
	for index := range pkc.duplicateLengths {
		combinationFound = true
		for position := range pkc.duplicateLengths[index] {
			if pkc.duplicateLengths[index][position] != columnLengths[position] {
				combinationFound = false
				break
			}
		}
		if combinationFound {
			at = index
			break;
		}
	}
	return
}
*/
/*

func (pkc ComplexPKCombinationType) Flush(directory string) {

	fileName := pkc.Columns.ColumnIdString()

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

	fileName := pkc.Columns.ColumnIdString()

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

	for parentTable, columnCombinationMap := range parentColumnCombinations {
		if parentTable.TableName.Value() != "TX_FAIL" {
			//		continue //_ITEM_REVERSED
		}

		storedKeys, err := dr.Repository.ComplexKeysByTable(parentTable)
		if err != nil {
			return err
		}

		columnCombinationMap.RemoveKeyColumnCombinations(storedKeys)

		if len(columnCombinationMap) == 0 {
			continue
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
			var columnCombinationMapForLeadHorse map[string]*ComplexPKCombinationType
			var columnCombinationMapForSlaveHorse map[string]*ComplexPKCombinationType

			horsesContext, horsesCancelFunc := context.WithCancel(context.Background())

			LeadHorse := func(ctc context.Context, LineNumber, DataPosition uint64, data [][]byte) (result dataflow.ReadDumpActionType, err error) {
				var truncateCombinations = false
				if LineNumber == 0 {
					fmt.Println("Column combination(s) to check duplicates:")
					statements := make([]string, 0, len(columnCombinationMapForLeadHorse))
					for _, columnCombination := range columnCombinationMapForLeadHorse {
						fmt.Printf("--%v\n ", columnCombination.Columns)
						columns := make([]string, 0, len(columnCombination.Columns))
						for _, c := range columnCombination.Columns {
							columns = append(columns, c.ColumnName.Value())
						}
						s := strings.Join(columns, ", ")
						statements = append(statements, fmt.Sprintf("select '%v' from dual where not exists (select %v,count(*) as ccount from %v.%v group by %v having count(*)>1)  ",
							s, s, currentPkTable.SchemaName.Value(),
							currentPkTable.TableName.Value(), s),
						)

					}
					{
						s := strings.Join(statements, "union all \n")
						fmt.Println(s)
					}
					fmt.Printf("\n")
				}

				if cumulativeSavedDataLength > 1024*1024 {
					LineNumberToCheckBySlaveHorseTo = LineNumber
					for columnCombinationKey, columnCombination := range columnCombinationMapForLeadHorse {
						if len(columnCombination.duplicatesByHash) > 0 {
							columnCombinationMapForSlaveHorse[columnCombinationKey] = columnCombination
						}
					}

					slaveChan <- true
					<-leadChan
					if len(columnCombinationMapForLeadHorse) > 0 {
						tracelog.Info(packageName, funcName, "Lead Horse continues processing %v from line %v with %v column combinations",
							currentPkTable, LineNumber,
							len(columnCombinationMapForLeadHorse),
						)
						for _, columnCombination := range columnCombinationMapForLeadHorse {
							columnCombination.ResetDuplicateStructures()
							runtime.GC()
							columnCombination.ReinitializeInternals()
						}
						cumulativeSavedDataLength = 0
					}
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
				firstHashMethod := fnv.New32()
			columns:
				for columnCombinationMapKey, columnCombination := range columnCombinationMapForLeadHorse {
					firstHashMethod.Reset()
					for _, position := range columnCombination.ColumnPositions {
						firstHashMethod.Write(data[position])
					}
					hv1 := firstHashMethod.Sum32()

					if !columnCombination.FirstBitset.Set(uint64(hv1)) {
						continue
					}

					secondHashMethod := fnv.New32a()
					for _, position := range columnCombination.ColumnPositions {
						secondHashMethod.Write(data[position])
					}

					hashValue := secondHashMethod.Sum32()

					newDuplicate := !columnCombination.duplicateBitset.Set(uint64(hashValue))

					pData := make([]*[]byte, len(columnCombination.Columns))
					addToDuplicateByHash := func(duplicates []*ComplexPKDupDataType) {
						newDup := &ComplexPKDupDataType{
							//							ColumnCombinationKey: columnCombinationMapKey,
							Data:       make([]*[]byte, len(columnCombination.Columns)),
							LineNumber: LineNumber,
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
							uint64(8+24+8*len(columnCombination.Columns)+8+8+8)
						columnLengths := make([]int, len(columnCombination.Columns))
						for index, position := range columnCombination.ColumnPositions {
							dataLength := len(data[position])
							columnLengths[index] = dataLength
							pointer := pData[index]
							if pointer != nil {
								newDup.Data[index] = pointer
							} else {
								if dataCopyRef, isDataCopied := copiedDataMap[position]; !isDataCopied {
									cumulativeSavedDataLength = cumulativeSavedDataLength + 24 + uint64(dataLength) + 16

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
							for _, position := range columnCombination.ColumnPositions {
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
							for index, position := range columnCombination.ColumnPositions {
								var result int
								if len(*dup.Data[index]) == len(data[position]) {
									result = bytes.Compare(*dup.Data[index], data[position])
								} else {
									result = 1
								}
								if result != 0 {
									countDifferentPieces++
								} else {
									if pData[index] == nil {
										pData[index] = dup.Data[index]
									}
								}
							}
							if countDifferentPieces == 0 {
								truncateCombinations = true
								tracelog.Info(packageName, funcName, "Lead Horse:Data duplication found for columns %v in lines %v and %v", columnCombination.Columns, dup.LineNumber, LineNumber)
								columnCombination.Reset()
								delete(columnCombinationMapForLeadHorse, columnCombinationMapKey)
								continue columns
							}
							//}
						}
						addToDuplicateByHash(duplicates)
					}
				}

				if truncateCombinations {
					if len(columnCombinationMapForLeadHorse) == 0 {
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
				if LineNumberToCheckBySlaveHorseTo > 0 && LineNumberToCheckBySlaveHorseTo == LineNumber {
					return dataflow.ReadDumpActionAbort, nil
				}
				firstHashMethod := fnv.New32()
				secondHashMethod := fnv.New32a()
			columns:
				for columnCombinationMapKey, columnCombination := range columnCombinationMapForSlaveHorse {

					firstHashMethod.Reset()
					secondHashMethod.Reset()

					for _, position := range columnCombination.ColumnPositions {
						secondHashMethod.Write(data[position])
					}
					hashValue := secondHashMethod.Sum32()

					if !columnCombination.duplicateBitset.Test(uint64(hashValue)) {
						continue
					}
					firstHashMethod.Reset()

					for _, position := range columnCombination.ColumnPositions {
						firstHashMethod.Write(data[position])
					}
					if !columnCombination.FirstBitset.Test(uint64(firstHashMethod.Sum32())) {
						continue
					}

					if duplicates, found := columnCombination.duplicatesByHash[hashValue]; found {
						for _, dup := range duplicates {
							countDifferentPieces := 0
							if dup.LineNumber == LineNumber {
								continue
							}
							for index, position := range columnCombination.ColumnPositions {
								result := bytes.Compare(*dup.Data[index], data[position])
								if result != 0 {
									countDifferentPieces++
									break
								}
							}
							if countDifferentPieces == 0 {
								// TODO:DUPLICATE!
								truncateCombinations = true
								tracelog.Info(packageName, funcName, "Slave Horse:Data duplication found for columns %v in lines %v and %v", columnCombination.Columns, LineNumber, dup.LineNumber)
								columnCombination.Reset()
								delete(columnCombinationMapForLeadHorse, columnCombinationMapKey)
								delete(columnCombinationMapForSlaveHorse, columnCombinationMapKey)
								continue columns
							}
						}
					}
				}

				if truncateCombinations {
					if len(columnCombinationMapForSlaveHorse) == 0 {
						tracelog.Info(packageName, funcName,
							"There is no column combination available for %v to check",
							currentPkTable,
						)
						return dataflow.ReadDumpActionAbort, nil
					}
				}
				return dataflow.ReadDumpActionContinue, nil
			}

			if len(columnCombinationMap) > 0 {
				for _, columnCombination := range columnCombinationMap {
					columnCombination.InitializeInternals()
				}
			}

			cumulativeSavedDataLength = 0

			go func() {
				processKeysPerPass := 5

				processedKeys := make(map[string]bool)

				keyList := make([]string, 0, len(columnCombinationMap))

				for columnCombinationMapKey, _ := range columnCombinationMap {
					keyList = append(keyList, columnCombinationMapKey)
				}

				sort.Slice(keyList, func(i, j int) bool {
					if len(columnCombinationMap[keyList[i]].Columns) == len(columnCombinationMap[keyList[j]].Columns) {
						return keyList[i] > keyList[j]
					} else {
						return len(columnCombinationMap[keyList[i]].Columns) > len(columnCombinationMap[keyList[j]].Columns)
					}
				})

			mainLoopLeadHorse:
				for {
					var allKeysProcessed bool = true

					for _, key := range keyList {
						if _, found := processedKeys[key]; found {
							continue
						}
						allKeysProcessed = false
						break
					}
					if allKeysProcessed {
						break mainLoopLeadHorse
					}

					{
						var removed, processed int = 0, 0
						for _, exists := range processedKeys {
							if exists {
								processed++
							} else {
								removed++
							}
						}

						tracelog.Info(packageName, funcName,
							"Column combinations: total %v, found duplicates in: %v, ready for exact test: %v, leftover: %v;  ",
							len(keyList), removed, processed,
							len(keyList)-removed-processed,
						)
					}

					for removedKey, exists := range processedKeys {
						if !exists {
							for key, columnCombination := range columnCombinationMap {
								if _, found := processedKeys[key]; !found {
									if key != removedKey && columnCombination.Columns.isSubsetOf(columnCombinationMap[removedKey].Columns) {
										tracelog.Info(packageName, funcName,
											"Combination %v is subset of already checked and rejected %v. Skipped ",
											columnCombination.Columns,
											columnCombinationMap[removedKey].Columns,
										)
										processedKeys[key] = false

										complexKey := columnCombination.NewComplexKeyInfo()
										complexKey.ProcessingStage = nullable.NewNullString("u")
										err = dr.Repository.PersistComplexKey(complexKey)
										if err != nil {
											tracelog.Error(err, packageName, funcName)
											break mainLoopLeadHorse
										}
									}
								}
							}
						}
					}

					columnCombinationMapForLeadHorse = make(map[string]*ComplexPKCombinationType)
					columnCombinationMapForSlaveHorse = make(map[string]*ComplexPKCombinationType)

					processingKeys := make(map[string]bool)

					for _, key := range keyList {
						if _, found := processedKeys[key]; found {
							continue
						}
						if len(processingKeys) >= processKeysPerPass {
							break
						}
						columnCombinationMapForLeadHorse[key] = columnCombinationMap[key]
						processingKeys[key] = true
					}

					if len(columnCombinationMapForLeadHorse) == 0 {
						continue
					}

					tracelog.Info(packageName, funcName, "Leading Horse starts processing %v with %v column combinations:",
						currentPkTable, len(columnCombinationMapForLeadHorse),
					)

					for _, columnCombination := range columnCombinationMapForLeadHorse {
						_ = columnCombination
						//	tracelog.Info(packageName, funcName, "%v", columnCombination.Columns)
					}

					dr.ReadAstraDump(
						horsesContext,
						currentPkTable,
						LeadHorse,
						leadHorseConfig,
					)

					LineNumberToCheckBySlaveHorseTo = 0
					for columnCombinationKey, columnCombination := range columnCombinationMapForLeadHorse {
						if len(columnCombination.duplicatesByHash) > 0 {
							columnCombinationMapForSlaveHorse[columnCombinationKey] = columnCombination
						}
					}

					for key := range processingKeys {
						_, exists := columnCombinationMapForLeadHorse[key]
						processedKeys[key] = exists
						columnCombination := columnCombinationMap[key]
						complexKey := columnCombination.NewComplexKeyInfo()
						if !exists {
							complexKey.ProcessingStage = nullable.NewNullString("u")
							err = dr.Repository.PersistComplexKey(complexKey)
							if err != nil {
								columnCombination.Reset()
								tracelog.Error(err, packageName, funcName)
								break mainLoopLeadHorse
							}
						} else if _, exists = columnCombinationMapForSlaveHorse[key]; !exists {
							complexKey.ProcessingStage = nullable.NewNullString("B")
							err = dr.Repository.PersistComplexKey(complexKey)
							if err != nil {
								tracelog.Error(err, packageName, funcName)
								columnCombination.Reset()
								break mainLoopLeadHorse
							}
							columnCombination.ComplexKeyInfoId = complexKey.Id.Value()

							err = dataflow.WriteBitsetToFile(horsesContext, dr.Config.BitsetPath, columnCombination)
							columnCombination.Reset()
							if err != nil {
								tracelog.Error(err, packageName, funcName)
								break mainLoopLeadHorse
							}
						}

					}

					if len(columnCombinationMapForSlaveHorse) == 0 {
						continue mainLoopLeadHorse
					} else {
						slaveChan <- true //Launch Slave horse
						<-leadChan        //Wait until Slave finishes

						for key := range processingKeys {

							_, exists := columnCombinationMapForSlaveHorse[key]

							processedKeys[key] = exists
							columnCombination := columnCombinationMap[key]
							complexKey := columnCombination.NewComplexKeyInfo()
							if !exists {
								columnCombination.Reset()
								complexKey.ProcessingStage = nullable.NewNullString("u")
								err = dr.Repository.PersistComplexKey(complexKey)
								if err != nil {
									tracelog.Error(err, packageName, funcName)
									break mainLoopLeadHorse
								}

							} else if _, exists = columnCombinationMapForLeadHorse[key]; exists {
								complexKey.ProcessingStage = nullable.NewNullString("B")

								err = dr.Repository.PersistComplexKey(complexKey)
								if err != nil {
									tracelog.Error(err, packageName, funcName)
									columnCombination.Reset()
									break mainLoopLeadHorse
								}

								columnCombination.ComplexKeyInfoId = complexKey.Id.Value()

								err = dataflow.WriteBitsetToFile(horsesContext, dr.Config.BitsetPath, columnCombination)
								columnCombination.Reset()
								if err != nil {
									tracelog.Error(err, packageName, funcName)
									break mainLoopLeadHorse
								}
							}
						}
					}
				} //mainLoopLeadHorse

				for key, exists := range processedKeys {
					if !exists {
						delete(columnCombinationMap, key)
					}
				}
				slaveChan <- false
				slaveChan <- dataflow.ReadDumpResultOk
				slaveChan <- err
				close(slaveChan)

			}()

		horses2:
			for {
				if unresolved, open := <-slaveChan; open {
					switch continued := unresolved.(type) {
					case bool:
						if !continued {
							unresolved = <-slaveChan
							err, _ = unresolved.(error)
							leadHorseResult, _ = unresolved.(dataflow.ReadDumpResultType)
							columnCombinationMapForSlaveHorse = nil
							close(leadChan)
						}

					}
				}

				if columnCombinationMapForSlaveHorse != nil && len(columnCombinationMapForSlaveHorse) > 0 {
					if LineNumberToCheckBySlaveHorseTo == 0 {
						tracelog.Info(packageName, funcName, "Slave Horse works on %v up to the EOF with %v column combinations", currentPkTable, len(columnCombinationMapForSlaveHorse))
					} else {
						tracelog.Info(packageName, funcName, "Slave Horse works on %v up to the line %v with %v column combinations", currentPkTable, LineNumberToCheckBySlaveHorseTo, len(columnCombinationMapForSlaveHorse))
					}

					slaveHorseResult, _, err = dr.ReadAstraDump(
						horsesContext,
						currentPkTable,
						SlaveHorse,
						slaveHorseConfig,
					)

					if err != nil {
						tracelog.Errorf(err, packageName, funcName, "err!")
						horsesCancelFunc()
						leadChan <- true
						return err
					} else {
						leadChan <- true
					}
				} else {
					slaveHorseResult = dataflow.ReadDumpResultOk
					break horses2
				}
			}

			if leadHorseResult == dataflow.ReadDumpResultOk && slaveHorseResult == dataflow.ReadDumpResultOk {

			}

			if len(columnCombinationMap) > 0 {
				fmt.Println("Column combination(s) after checking duplicates:")
				for _, columnCombination := range columnCombinationMap {
					fmt.Println(columnCombination.Columns)
				}
				fmt.Printf("\n\n")
			}
		}
/*

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
	}

	tracelog.Info(packageName, funcName, "Elapsed time: %v", time.Since(start))
	tracelog.Completed(packageName, funcName)
	return err
}
