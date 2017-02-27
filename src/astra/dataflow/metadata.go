package dataflow

import (
	"astra/metadata"
	"astra/nullable"
	"bufio"
	"errors"
	"fmt"
	"github.com/goinggo/tracelog"
	"golang.org/x/net/context"
	"os"
	"sync"
	"sparsebitset"
	"math"
)
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

type ColumnInfoType struct {
	*metadata.ColumnInfoType

	IntegerUniqueCount           nullable.NullInt64
	MovingMean                   nullable.NullFloat64
	MovingStandardDeviation      nullable.NullFloat64

	stringAnalysisLock           sync.Mutex
	numericAnalysisLock          sync.Mutex

	categoryLock                 sync.RWMutex
	Categories                   map[string]*DataCategoryType
	initCategories               sync.Once
	numericPositiveBitsetChannel chan uint64
	numericNegativeBitsetChannel chan uint64
	NumericPositiveBitset        *sparsebitset.BitSet
	NumericNegativeBitset        *sparsebitset.BitSet
	drainBitsetChannels          sync.WaitGroup
}


func (ci *ColumnInfoType) CategoryByKey(key string, callBack func() (result *DataCategoryType, err error),
) (result *DataCategoryType, err error) {
	funcName := "ColumnInfoType.CategoryByKey"
	tracelog.Started(packageName,funcName)
	ci.initCategories.Do(func(){
		ci.Categories = make(map[string]*DataCategoryType)
	})

	ci.categoryLock.Lock()
	if value, found := ci.Categories[key]; !found {
		if callBack != nil {
			result,err = callBack()
			if err != nil {
				ci.categoryLock.Unlock()
				tracelog.Error(err,packageName,funcName)
				return nil,err
			}
		}
		ci.Categories[key] = result
		ci.categoryLock.Unlock()
	} else {
		ci.categoryLock.Unlock()
		result = value
	}

	tracelog.Completed(packageName,funcName)
	return result, err
}
func (ci *ColumnInfoType) CloseStorage(runContext context.Context) (err error) {
	if ci.Categories != nil {
		for _,category := range ci.Categories{
			category.CloseAnalyzerChannels()
		}
	}


	var prevValue uint64
	var count uint64 = 0
	var gotPrevValue bool
	var meanValue, cumulativeDeviaton float64 = 0,0
	increasingOrder := context.WithValue(runContext,"sort",true)
	reversedOrder := context.WithValue(context.WithValue(runContext,"sort",true),"desc",true)

	if ci.NumericNegativeBitset != nil {
		close(ci.numericNegativeBitsetChannel)
	}
	if ci.NumericPositiveBitset != nil {
		close(ci.numericPositiveBitsetChannel)
	}

	ci.drainBitsetChannels.Wait()

	if ci.NumericNegativeBitset != nil {
		gotPrevValue = false
		for value := range ci.NumericNegativeBitset.BitChan(reversedOrder) {
			count ++
			if !gotPrevValue {
				prevValue = value
				gotPrevValue = true
			} else {
				cumulativeDeviaton += (float64(value) - float64(prevValue))
				prevValue = value
			}
		}
	}
	if ci.NumericPositiveBitset != nil {
		gotPrevValue = false
		for value := range ci.NumericPositiveBitset.BitChan(increasingOrder) {
			count ++
			if !gotPrevValue {
				prevValue = value
				gotPrevValue = true
			} else {
				cumulativeDeviaton += (float64(value) - float64(prevValue))
				prevValue = value
			}
		}
	}
	if count> 0 {
		meanValue = cumulativeDeviaton / float64(count)
		totalDeviation := float64(0)

		if ci.NumericNegativeBitset != nil {
			gotPrevValue = false
			for value := range ci.NumericNegativeBitset.BitChan(reversedOrder) {
				if !gotPrevValue {
					prevValue = value
					gotPrevValue = true
				} else {
					totalDeviation = totalDeviation +  math.Pow(meanValue - (float64(value) - float64(prevValue)),2)
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
					totalDeviation = totalDeviation +  math.Pow(meanValue - (float64(value) - float64(prevValue)),2)
					prevValue = value
				}
			}
		}
		stddev:= math.Sqrt(totalDeviation /float64(count))
		ci.IntegerUniqueCount = nullable.NewNullInt64(int64(count))
		ci.MovingMean = nullable.NewNullFloat64(meanValue)
		ci.MovingStandardDeviation = nullable.NewNullFloat64(stddev)


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

func ExpandFromMetadataTable(table *metadata.TableInfoType) (result *TableInfoType) {
	result = &TableInfoType{
		TableInfoType: table,
		Columns:       make([]*ColumnInfoType, 0, len(table.Columns)),
	}
	for index := range table.Columns {
		result.Columns = append(result.Columns, &ColumnInfoType{
			ColumnInfoType: table.Columns[index],
		},
		)
	}
	return result
}

type TableInfoType struct {
	*metadata.TableInfoType
	Columns      []*ColumnInfoType
	TankWriter   *bufio.Writer
	tankFile     *os.File
	tankFileLock sync.Mutex
}

func (ti *TableInfoType) OpenTank(closeContext context.Context, pathToTankDir string, flags int) (err error) {
	funcName := "TableInfoType.OpenTank"
	tracelog.Started(packageName, funcName)

	if pathToTankDir == "" {
		err = errors.New("Given path to binary tank directory is empty")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	err = os.MkdirAll(pathToTankDir, 700)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Making directories for path %v", pathToTankDir)
		return err
	}

	pathToTankFile := fmt.Sprintf("%v%v%v.tank",
		pathToTankDir,
		os.PathSeparator,
		ti.Id.String(),
	)

	if ti.tankFile == nil && ((flags & os.O_CREATE) == os.O_CREATE) {
		ti.tankFileLock.Lock()
		defer ti.tankFileLock.Unlock()
		if ti.tankFile == nil && ((flags & os.O_CREATE) == os.O_CREATE) {
			file, err := os.OpenFile(pathToTankFile, flags, 0666)
			if err != nil {
				tracelog.Errorf(err, packageName, funcName, "Opening file %v", pathToTankFile)
				return err
			}
			ti.TankWriter = bufio.NewWriter(file)
			ti.tankFile = file
			go func() {
				funcName := "TableInfoType.OpenTank.delayedClose"
				tracelog.Started(packageName, funcName)
				if ti.TankWriter != nil {
					select {
					case <-closeContext.Done():
						ti.TankWriter.Flush()
						ti.tankFile.Close()
						return
					}
				}
				tracelog.Completed(packageName, funcName)
			}()
		}
	}

	return
}
