package dataflow

import (
	"astra/metadata"
	"bufio"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"os"
)

type TableDumpConfig struct {
	Path            string
	GZip            bool
	ColumnSeparator byte
	LineSeparator   byte
	BufferSize      int
}

func defaultTableDumpConfig() *TableDumpConfig {
	return &TableDumpConfig{
		Path:            "./",
		GZip:            true,
		ColumnSeparator: 0x1F,
		LineSeparator:   0x0A,
		BufferSize:      4096,
	}
}

type ColumnInfoType struct {
	*metadata.ColumnInfoType
	TableInfo *TableInfoType

	//stringAnalysisLock  sync.Mutex
	//numericAnalysisLock sync.Mutex

	//categoryRLock                sync.RWMutex
	Categories map[string]*DataCategoryType
	//CategoriesB                  []map[string]*DataCategoryType
	//initCategories               sync.Once
	//numericPositiveBitsetChannel chan uint64
	//numericNegativeBitsetChannel chan uint64
	//NumericPositiveBitset        *sparsebitset.BitSet
	//NumericNegativeBitset        *sparsebitset.BitSet
	//drainBitsetChannels          sync.WaitGroup
}

func (ci *ColumnInfoType) CategoryByKey(key string, initFunc func() (result *DataCategoryType, err error),
) (result *DataCategoryType, err error) {
	funcName := "ColumnInfoType.CategoryByKey"
	if ci.Categories == nil {
		ci.Categories = make(map[string]*DataCategoryType)
	}
	if value, found := ci.Categories[key]; !found {
		if initFunc != nil {
			result, err = initFunc()
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return nil, err
			}
		}
		ci.Categories[key] = result
	} else {
		result = value
	}
	return result, err
}

type tableBinaryType struct {
	*bufio.Writer
	dFile         *os.File
	dFullFileName string
}

func (t *tableBinaryType) Close() (err error) {
	funcName := "tableBinaryType.Close"

	if t == nil {
		return nil
	}

	err = t.Flush()
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Flushing data to %v ", t.dFullFileName)
		return err
	}

	if t.dFile != nil {
		err = t.dFile.Close()
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Closing file %v ", t.dFullFileName)
			return err
		}
	}
	return nil
}

type TableInfoType struct {
	*metadata.TableInfoType
	Columns       []*ColumnInfoType
	DataDump      *tableBinaryType
	HashDump      *tableBinaryType
	bitSetStorage *bolt.DB
	currentTx     *bolt.Tx
}

func (ti *TableInfoType) openBinaryDump(
	pathToBinaryDir, suffix string,
	flags int,
) (result *tableBinaryType, err error) {

	funcName := "TableInfoType.openBinaryDump"
	tracelog.Started(packageName, funcName)

	if pathToBinaryDir == "" {
		err = errors.New("Given path to binary dump directory is empty")
		tracelog.Error(err, packageName, funcName)
		return nil, err
	}

	err = os.MkdirAll(pathToBinaryDir, 700)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Making directories for path %v", pathToBinaryDir)
		return nil, err
	}

	pathToBinaryFile := fmt.Sprintf("%v%v.%v.%v.dump",
		pathToBinaryDir,
		os.PathSeparator,
		suffix,
		ti.Id.String(),
	)

	file, err := os.OpenFile(pathToBinaryFile, flags, 0666)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening file %v", pathToBinaryFile)
		return nil, err
	}

	result = &tableBinaryType{
		Writer:        bufio.NewWriter(file),
		dFile:         file,
		dFullFileName: pathToBinaryFile,
	}

	tracelog.Completed(packageName, funcName)

	return result, nil
}

func (t *TableInfoType) NewDataDump(pathToBinaryDir string) (err error) {
	result, err := t.openBinaryDump(
		pathToBinaryDir,
		"data",
		os.O_CREATE,
	)
	if err == nil {
		t.DataDump = result
		return nil
	}
	return err
}

func (t *TableInfoType) NewHashDump(pathToBinaryDir string) (err error) {
	result, err := t.openBinaryDump(
		pathToBinaryDir,
		"hash",
		os.O_CREATE,
	)
	if err == nil {
		t.HashDump = result
		return nil
	}
	return err
}

func (t *TableInfoType) NewBoltDb(pathToBinaryDir string) (err error) {

	funcName := "TableInfoType.openBinaryDump"
	tracelog.Started(packageName, funcName)

	if pathToBinaryDir == "" {
		err = errors.New("Given path to bolt db directory is empty")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	err = os.MkdirAll(pathToBinaryDir, 700)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Making directories for path %v", pathToBinaryDir)
		return err
	}

	pathToBinaryFile := fmt.Sprintf("%v%v.%v.%v.boltdb",
		pathToBinaryDir,
		os.PathSeparator,
		t.Id.String(),
		"bitset",
	)

	t.bitSetStorage, err = bolt.Open(pathToBinaryFile, 0666, nil)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Creating bolt db %v "+pathToBinaryFile)
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (t *TableInfoType) CloseBoltDb() (err error) {

	if t.bitSetStorage != nil {
		err = t.bitSetStorage.Close()
	}
	return
}

func (c *ColumnInfoType) ResetBitset(suffixType BitsetFileSuffixType){
	if c.Categories != nil {
		for _, v := range c.Categories {
			v.ResetBitset(suffixType)
		}
	}
}

func (c *ColumnInfoType) IsNumericDataType() bool {
	realType := c.RealDataType.Value()
	result :=
		realType == "java.lang.Byte" ||
			realType == "java.lang.Short" ||
			realType == "java.lang.Integer" ||
			realType == "java.lang.Long" ||
			realType == "java.lang.Double" ||
			realType == "java.lang.Float" ||
			realType == "java.math.BigDecimal" ||
			realType == "java.math.BigInteger"
	return result
}

func ExpandFromMetadataTable(table *metadata.TableInfoType) (result *TableInfoType) {
	result = &TableInfoType{
		TableInfoType: table,
		Columns:       make([]*ColumnInfoType, 0, len(table.Columns)),
	}
	for index := range table.Columns {
		result.Columns = append(result.Columns, &ColumnInfoType{
			ColumnInfoType: table.Columns[index],
			TableInfo:      result,
		},
		)
	}
	return result
}

/*



func (ci *ColumnInfoType) CloseStorage(runContext context.Context) (err error) {
	if ci.Categories != nil {
		for _, category := range ci.Categories {
			_ =category
			//category.CloseAnalyzerChannels()
		}
	}

	var prevValue uint64
	var count uint64 = 0
	var countInDeviation uint64 = 0
	var gotPrevValue bool
	var meanValue, cumulativeDeviaton float64 = 0, 0
	increasingOrder := context.WithValue(runContext, "sort", true)
	reversedOrder := context.WithValue(context.WithValue(runContext, "sort", true), "desc", true)

ci.drainBitsetChannels.Wait()

if ci.NumericNegativeBitset != nil {
gotPrevValue = false
for value := range ci.NumericNegativeBitset.BitChan(reversedOrder) {
if !gotPrevValue {
prevValue = value
gotPrevValue = true
} else {
count++
cumulativeDeviaton += (float64(value) - float64(prevValue))
prevValue = value
}
}
}
if ci.NumericPositiveBitset != nil {
gotPrevValue = false
for value := range ci.NumericPositiveBitset.BitChan(increasingOrder) {
if !gotPrevValue {
prevValue = value
gotPrevValue = true
} else {
count++
cumulativeDeviaton += (float64(value) - float64(prevValue))
prevValue = value
}
}
}
if count > 0 {
meanValue = cumulativeDeviaton / float64(count)
totalDeviation := float64(0)

if ci.NumericNegativeBitset != nil {
gotPrevValue = false
for value := range ci.NumericNegativeBitset.BitChan(reversedOrder) {
if !gotPrevValue {
prevValue = value
gotPrevValue = true
} else {
countInDeviation++
totalDeviation = totalDeviation + math.Pow(meanValue-(float64(value)-float64(prevValue)), 2)
prevValue = value
}
}
}

if ci.NumericPositiveBitset != nil {
gotPrevValue = false
for value := range ci.NumericPositiveBitset.BitChan(increasingOrder) {
if !gotPrevValue {
prevValue = value
gotPrevValue = true
} else {
countInDeviation++
totalDeviation = totalDeviation + math.Pow(meanValue-(float64(value)-float64(prevValue)), 2)
prevValue = value
}
}
}
if countInDeviation > 1 {
stdDev := math.Sqrt(totalDeviation / float64(countInDeviation-1))
ci.MovingStandardDeviation = nullable.NewNullFloat64(stdDev)
}
ci.IntegerUniqueCount = nullable.NewNullInt64(int64(count))
ci.MovingMean = nullable.NewNullFloat64(meanValue)

}

return err
}


func (ci *ColumnInfoType) AnalyzeStringValue(stringValue string) {
	ci.stringAnalysisLock.Lock()
	defer ci.stringAnalysisLock.Unlock()

	(*ci.NonNullCount.Reference())++

	if !ci.MaxStringValue.Valid() || ci.MaxStringValue.Value() < stringValue {
		ci.MaxStringValue = nullable.NewNullString(stringValue)
	}
	if !ci.MinStringValue.Valid() || ci.MinStringValue.Value() > stringValue {
		ci.MinStringValue = nullable.NewNullString(stringValue)
	}

	lValue := int64(len(stringValue))
	if !ci.MaxStringLength.Valid() {
		ci.MaxStringLength = nullable.NewNullInt64(lValue)
	} else if ci.MaxStringLength.Value() < lValue {
		(*ci.MaxStringLength.Reference()) = lValue

	}
	if !ci.MinStringLength.Valid() {
		ci.MinStringLength = nullable.NewNullInt64(lValue)
	} else if ci.MinStringLength.Value() > lValue {
		(*ci.MinStringLength.Reference()) = lValue

	}
}

func (ci *ColumnInfoType) AnalyzeNumericValue(floatValue float64) {
	ci.numericAnalysisLock.Lock()
	defer ci.numericAnalysisLock.Unlock()

	(*ci.NumericCount.Reference())++
	if !ci.MaxNumericValue.Valid() {
		ci.MaxNumericValue = nullable.NewNullFloat64(floatValue)
	} else if ci.MaxNumericValue.Value() < floatValue {
		(*ci.MaxNumericValue.Reference()) = floatValue
	}

	if !ci.MinNumericValue.Valid() {
		ci.MinNumericValue = nullable.NewNullFloat64(floatValue)
	} else if ci.MinNumericValue.Value() > floatValue {
		(*ci.MinNumericValue.Reference()) = floatValue
	}

}


*/

/*

type boltStorageGroupType struct {
	storage           *bolt.DB
	currentTx         *bolt.Tx
	currentTxLock sync.Mutex
	currentBucket     *bolt.Bucket
	currentBucketLock sync.Mutex
	pathToStorageFile string
	storageName       string
	columnId          int64
}*/

/*func (s boltStorageGroupType) String() {
	return fmt.Sprintf("Storage for %v, column %v, datacategory %v, on %v",
		s.storageName,
		s.columnId,
		s.pathToStorageFile,
	)
}
func (s *boltStorageGroupType) Open(
	storageName string,
	pathToStorageDir string,
	columnID int64,
) (err error) {
	funcName := "boltStorageGroupType.Open." + storageName
	tracelog.Started(packageName, funcName)

	s.storageName = storageName
	s.columnId = columnID

	if pathToStorageDir == "" {
		err = fmt.Errorf("Given path to %v storage directory is empty ", storageName)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	err = os.MkdirAll(pathToStorageDir, 700)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Making directories for path %v", pathToStorageDir)
		return err
	}

	pathToStorageFile := fmt.Sprintf("%v%v%v.%v.bolt.db",
		pathToStorageDir,
		os.PathSeparator,
		columnID,
		storageName,
	)
	s.storage, err = bolt.Open(pathToStorageFile, 700, &bolt.Options{InitialMmapSize: 16})
	s.storage.MaxBatchSize = 100000
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening database for %v storage %v", storageName, pathToStorageFile)
		return err
	}
	s.currentTx, err = s.storage.Begin(true)
	//fmt.Println("begin transaction " +storageName)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening transaction on %v storage %v", storageName, pathToStorageFile)
		return err
	}
	err = s.OpenDefaultBucket()


	return
}
func(s *boltStorageGroupType) OpenDefaultBucket() (err error){
	funcName := "boltStorageGroupType.Open." + s.storageName
	tracelog.Started(packageName, funcName)
	if s.storageName == "hash" && s.currentBucket == nil{
		bucketName := []byte("0")
		s.currentBucketLock.Lock()
		s.currentBucket = s.currentTx.Bucket(bucketName)
		if s.currentBucket == nil {
			s.currentBucket,err = s.currentTx.CreateBucket(bucketName)
			s.currentBucketLock.Unlock()
			if err != nil {
				tracelog.Errorf(err, packageName, funcName, "Creating default bucket on %v  storage %v", s.storageName, s.pathToStorageFile)
				return err
			}
		}
	}
	tracelog.Completed(packageName,funcName)
	return
}

func (s *boltStorageGroupType) Close() (err error) {
	funcName := "boltStorageGroupType.Close"
	tracelog.Started(packageName, funcName)
	if s.currentTx != nil {
		s.currentTxLock.Lock()
		err = s.currentTx.Commit()
		s.currentTxLock.Unlock()
		s.currentTx = nil
		fmt.Println("close transaction " + s.storageName)
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Closing transaction on storage %v", s.pathToStorageFile)
			return
		}

	}

	if s.storage != nil {
		err = s.storage.Close()
		s.storage = nil
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Closing database for %v storage %v", s.storageName, s.pathToStorageFile)
			return
		}
	}
	tracelog.Completed(packageName, funcName)
	return
}
*/
