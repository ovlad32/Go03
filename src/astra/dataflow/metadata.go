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
	"github.com/boltdb/bolt"
	"astra/B8"
	"sparsebitset"
)


type boltStorageGroupType struct {
	storageLock sync.Mutex
	storage     *bolt.DB
	currentTx   *bolt.Tx
	rootBucket  *bolt.Bucket
	pathToStorageFile string
	storageName string
	columnId int64
}

func (s boltStorageGroupType) String()  {
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
dataCategoryKey string,
) (err error) {
	funcName := "boltStorageGroupType.Open." + storageName
	tracelog.Started(packageName, funcName)

	s.storageName = storageName
	s.columnId = columnID

	if pathToStorageDir == "" {
		err = fmt.Errorf("Given path to %v storage directory is empty ",storageName)
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
	s.storage, err = bolt.Open(pathToStorageFile, 700, &bolt.Options{InitialMmapSize:16})
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening database for %v storage %v", storageName, pathToStorageFile)
		return err
	}
	s.currentTx, err = s.storage.Begin(true)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening transaction on %v storage %v", storageName, pathToStorageFile)
		return err
	}
	if storageName == "hash" {
		s.rootBucket, err = s.currentTx.CreateBucketIfNotExists([]byte("0"))
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Creating root bucket on%v  storage %v", storageName, pathToStorageFile)
			return err
		}
	}

	return
}
func (s *boltStorageGroupType) Close(ctx context.Context) (err error) {
	funcName := "boltStorageGroupType.Close.delayedClose"
	tracelog.Started(packageName, funcName)
	if s.storage != nil {
		err = s.currentTx.Commit()
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Closing transaction on storage %v", s.pathToStorageFile)
			return
		}

		err = s.storage.Close()
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Closing database for %v storage %v", s.storageName, s.pathToStorageFile)
			return
		}
		s.storage = nil
	}
	tracelog.Completed(packageName, funcName)
	return
}


type ColumnInfoType struct {
	*metadata.ColumnInfoType

	stringAnalysisLock  sync.Mutex
	numericAnalysisLock sync.Mutex

	categoryLock        sync.RWMutex
	Categories          map[string]*DataCategoryType

	hashStorage         *boltStorageGroupType
	bitsetStorage       *boltStorageGroupType
	columnDataChan      chan *ColumnDataType
}

func (ci *ColumnInfoType) CategoryByKey(
	simple *DataCategorySimpleType,
	callBack func(),
	) (	result *DataCategoryType, err error) {
	if ci.Categories == nil {
		ci.categoryLock.Lock()
		if ci.Categories == nil {
			ci.Categories = make(map[string]*DataCategoryType)
			ci.categoryLock.Unlock()
		}
	}

	key := simple.Key()

	ci.categoryLock.Lock()
	if value, found := ci.Categories[key]; !found {
		result = simple.covert()
		ci.Categories[key] = result
		if callBack!=nil {
			err = callBack()
		}
	} else {
		result = value
	}
	ci.categoryLock.Unlock()

	return result, err
}
func(ci *ColumnInfoType) CloseStorage() (err error) {
	if ci.columnDataChan == nil {
		err = ci.bitsetStorage.Close()
		err = ci.hashStorage.Close()
		close(ci.columnDataChan)
	}
	return err
}


func (ci *ColumnInfoType) RunStorage(
	ctx context.Context,
	storagePath string,
	columnId int64,
) (errChan chan error) {
	errChan = make(chan error, 1)

	//TODO: check emptyness
	if ci.columnDataChan == nil {
		ci.columnDataChan = make(chan *ColumnDataType, 1000)
		go func() {
			writtenHashValues := uint64(0);
			offsetBytes := make([]byte, 0, 8)
			outer:
			for {
				select {
				case <-ctx.Done():
					break outer
				case columnData, opened := <-ci.columnDataChan:
					if !opened {
						break outer
					}
					go func() {
						offsetBytes = B8.Clear(offsetBytes)
						B8.UInt64ToBuff(offsetBytes, columnData.LineOffset)
						ci.hashStorage.rootBucket.Put(columnData.HashValue, offsetBytes)
						writtenHashValues++
						if writtenHashValues >= 1000 {
							writtenHashValues = 0
							err := ci.hashStorage.currentTx.Commit();
							if err != nil {
								errChan <-err
								return;
							}
							ci.hashStorage.currentTx, err = ci.hashStorage.storage.Begin(true)
							if err != nil {
								errChan <-err
								return;
							}
						}
					}()
					go func() {
						key := columnData.dataCategoryKey
						bucket, err := ci.bitsetStorage.currentTx.CreateBucketIfNotExists([]byte(key))
						if err != nil {
							errChan <- err
							return
						}

						baseUIntValue, offsetUIntValue := sparsebitset.OffsetBits(columnData.HashInt)
						baseB8Value := B8.UInt64ToB8(baseUIntValue)
						bits, _ := B8.B8ToUInt64(bucket.Get(baseB8Value))
						bits = bits | (1 << offsetUIntValue)
						bucket.Put(baseB8Value, B8.UInt64ToB8(bits))
					}()
				}
			}
			close(errChan)
		}()
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
