package dataflow

import (
	"astra/B8"
	"astra/metadata"
	"astra/nullable"
	"bufio"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"golang.org/x/net/context"
	"os"
	"sparsebitset"
	"sync"
)

type boltStorageGroupType struct {
	storage           *bolt.DB
	currentTx         *bolt.Tx
	currentTxLock sync.Mutex
	currentBucket     *bolt.Bucket
	currentBucketLock sync.Mutex
	pathToStorageFile string
	storageName       string
	columnId          int64
}

/*func (s boltStorageGroupType) String() {
	return fmt.Sprintf("Storage for %v, column %v, datacategory %v, on %v",
		s.storageName,
		s.columnId,
		s.pathToStorageFile,
	)
}
*/
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

type ColumnInfoType struct {
	*metadata.ColumnInfoType

	stringAnalysisLock  sync.Mutex
	numericAnalysisLock sync.Mutex

	categoryLock sync.RWMutex
	Categories   map[string]*DataCategoryType

	hashStorage    *boltStorageGroupType
	bitsetStorage  *boltStorageGroupType
	columnDataChan chan *ColumnDataType
}

func (ci *ColumnInfoType) CategoryByKey(key string, callBack func() (result *DataCategoryType, err error),
) (result *DataCategoryType, err error) {
	funcName := "ColumnInfoType.CategoryByKey"
	tracelog.Started(packageName,funcName)
	if ci.Categories == nil {
		ci.categoryLock.Lock()
		if ci.Categories == nil {
			ci.Categories = make(map[string]*DataCategoryType)
			ci.categoryLock.Unlock()
		}
	}

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
func (ci *ColumnInfoType) CloseStorage() (err error) {
	if ci.columnDataChan != nil {
		//close(ci.columnDataChan)
		if ci.Categories != nil {
			for _,category := range ci.Categories{
				category.CloseAnalyzerChannels()
			}
		}
		//err = ci.bitsetStorage.Close()
		//err = ci.hashStorage.Close()

	}
	return err
}


func (ci *ColumnInfoType) RunStorage(ctx context.Context) (errChan chan error) {
	errChan = make(chan error, 1)
	//TODO: check emptyness
	if ci.columnDataChan == nil {
		ci.columnDataChan = make(chan *ColumnDataType,1000)
		go func() {
			funcName:= "ColumnInfoType.RunStorage.gofunc1"
//			var wg sync.WaitGroup
			writtenHashValues := uint64(0)
			writtenBitsetValues := uint64(0)
		outer:
			for {
				select {
				case <-ctx.Done():
					break outer
				case columnData, opened := <-ci.columnDataChan:
					if !opened {
						break outer
					}
					//wg.Add(2)
					func(val *ColumnDataType) {
					//	defer wg.Done()
						offsetBytes := B8.UInt64ToB8(val.LineOffset)
						var key B8.B8Type
						copy(key[:],val.HashValue)
						ci.hashStorage.currentBucket.Put(key[:], offsetBytes)
						writtenHashValues++
						if writtenHashValues >= 100000 {
							writtenHashValues = 0
							ci.hashStorage.currentTxLock.Lock()
							err := ci.hashStorage.currentTx.Commit()
							if err != nil {
								tracelog.Errorf(err,packageName,funcName,"Commit 1000 hash values")
								errChan <- err
								return
							}

							ci.hashStorage.currentTx, err = ci.hashStorage.storage.Begin(true)
							if err != nil {
								tracelog.Errorf(err,packageName,funcName,"a new Tx for 1000 hash values")
								errChan <- err
								return
							}
							ci.hashStorage.currentTxLock.Unlock()
							ci.hashStorage.OpenDefaultBucket()
						}
					}(columnData)
					func(val *ColumnDataType) {
						var err error
						//defer wg.Done()
						funcName := "RunStorage.bitset"
						key := val.dataCategoryKey
						bucket := ci.bitsetStorage.currentTx.Bucket([]byte(key))
						if bucket == nil {
							ci.bitsetStorage.currentBucketLock.Lock()
							bucket = ci.bitsetStorage.currentTx.Bucket([]byte(key))
							if bucket == nil {
								bucket, err = ci.bitsetStorage.currentTx.CreateBucket([]byte(key))
								ci.bitsetStorage.currentBucketLock.Unlock()
								if err != nil {
									tracelog.Errorf(err,packageName,funcName,"create bucket category %v",key)
									errChan <- err
									return
								}
							}
						}
						baseUIntValue, offsetUIntValue := sparsebitset.OffsetBits(val.HashInt)
						baseB8Value := B8.UInt64ToB8(baseUIntValue)
						bitsBytes := bucket.Get(baseB8Value)
						bits, _ := B8.B8ToUInt64(bitsBytes)
						bits = bits | (1 << offsetUIntValue)
						bitsBytes = B8.UInt64ToB8(bits)
						bucket.Put(baseB8Value, bitsBytes)

						writtenBitsetValues++
						if writtenBitsetValues >= 100000 {
							//fmt.Println("bitset1000")
							writtenBitsetValues = 0
							ci.bitsetStorage.currentTxLock.Lock()
							err = ci.bitsetStorage.currentTx.Commit()
							if err != nil {
								tracelog.Errorf(err,packageName,funcName,"Commit 1000 bitset values")
								errChan <- err
								return
							}


							ci.bitsetStorage.currentTx, err = ci.bitsetStorage.storage.Begin(true)
							if err != nil {
								tracelog.Errorf(err,packageName,funcName,"a new Tx for 1000 bitset values")
								errChan <- err
								return
							}
							ci.bitsetStorage.currentTxLock.Unlock()
						}
					}(columnData)
					//wg.Wait()
				}
			}
			close(errChan)
		}()
	} else {
		close(errChan)
	}
	return errChan
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

func (ti *TableInfoType) OpenTank(ctx context.Context, pathToTankDir string, flags int) (err error) {
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
					case <-ctx.Done():
						ti.TankWriter.Flush()
						ti.tankFile.Close()
					}
				}
				tracelog.Completed(packageName, funcName)
			}()
		}
	}

	return
}
