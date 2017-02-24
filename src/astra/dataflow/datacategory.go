package dataflow

import (
	"astra/nullable"
	"context"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"math"
	"os"
	"sync"
)

type DataCategoryStore struct {
	store           *bolt.DB
	bucket          *bolt.Bucket
	tx              *bolt.Tx
	pathToStoreDir string
	storeKey        string
	columnDataChan chan *ColumnDataType
	chanLock sync.Mutex

}

func (s *DataCategoryStore) Open(storeKey string, pathToStoreDir string) (err error) {
	funcName := "DataCategoryStore.Open." + storeKey
	tracelog.Started(packageName, funcName)

	s.storeKey = storeKey

	if pathToStoreDir == "" {
		err = fmt.Errorf("Given path to %v store directory is empty ", storeKey)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	err = os.MkdirAll(pathToStoreDir, 700)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Making directories for path %v", pathToStoreDir)
		return err
	}

	s.pathToStoreDir = fmt.Sprintf("%v%c%v",
		pathToStoreDir,
		os.PathSeparator,
		storeKey,
	)

	err = os.MkdirAll(s.pathToStoreDir, 700)

	pathToStoreFile := fmt.Sprintf("%v%c%v.bolt.db",
		pathToStoreDir,
		os.PathSeparator,
		storeKey,
	)
	s.store, err = bolt.Open(pathToStoreFile, 700, &bolt.Options{InitialMmapSize: 16})
	//	s.store.MaxBatchSize = 100000
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening database for %v store %v", storeKey, pathToStoreFile)
		return err
	}

	s.tx, err = s.store.Begin(true)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening transaction on %v store %v", storeKey, pathToStoreFile)
		return err
	}



	s.OpenDefaultBucket()

	return
}

func (s *DataCategoryStore) RunStore(ctx context.Context) (errChan chan error) {
	errChan = make(chan error, 1)
	if s.columnDataChan == nil {
		s.chanLock.Lock()
		if s.columnDataChan == nil {
			s.columnDataChan = make(chan *ColumnDataType,10000)
		}
		s.chanLock.Unlock()
	}

	go func() {

		funcName:= "DataCategoryStore.RunStore.gofunc1"
		_=funcName
		//writtenValues := uint64(0)
		columnBlock:= &ColumnBlockType{}
		_=columnBlock
		outer:
			for {
				select {
				case <-ctx.Done():
					break outer
				case columnData, opened := <-s.columnDataChan:
					if !opened {
						break outer
					}
					_=columnData
				/*	columnBlock.Data = s.bucket.Get(columnData.RawData)
					columnBlock.Append(columnData.Column.Id.Value(), columnData.LineOffset)
					s.bucket.Put(columnData.RawData, columnBlock.Data)
				*/  found := false
					pathToChunk := fmt.Sprintf("%v%c%v",s.pathToStoreDir,os.PathSeparator,columnData.HashInt);
					pathToChunkRenamed := pathToChunk+".r"
					if false {
						if fs, errc := os.Stat(pathToChunk); !os.IsNotExist(errc) {
							os.Remove(pathToChunkRenamed)
							errn := os.Rename(pathToChunk, pathToChunkRenamed)
							if errn != nil {
								errChan <- errn
								tracelog.Error(errn,packageName,funcName)
								break outer
							}
							var f *os.File
							f, errc := os.OpenFile(pathToChunkRenamed, os.O_RDONLY, 0)
							if errc != nil {
								errChan <- errc
								tracelog.Error(errc,packageName,funcName)
								break outer
							}
							columnBlock.Data = make([]byte, fs.Size())
							f.Read(columnBlock.Data)
							f.Close();
							found = true
						}
						//columnBlock.Append(columnData.Column.Id.Value(), columnData.LineOffset)
						f, errc := os.OpenFile(pathToChunk, os.O_CREATE, 755)
						if errc != nil {
							errChan <- errc
							tracelog.Error(errc,packageName,funcName)
							break outer
						}
						f.Write(columnBlock.Data)
						f.Close()
						if found {
							os.Remove(pathToChunkRenamed)
						}
					}
					/*l := len(columnBlock.Data)

					writtenValues++
					if writtenValues >= 50000 {
						writtenValues = 0
						fmt.Println(">",l)
						err := s.tx.Commit()
						if err != nil {
							tracelog.Errorf(err, packageName, funcName, "Commit 1000 hash values")
							errChan <- err
							return
						}

						s.tx, err = s.store.Begin(true)
						if err != nil {
							tracelog.Errorf(err, packageName, funcName, "a new Tx for 1000 hash values")
							errChan <- err
							return
						}
						s.OpenDefaultBucket()
					}*/
				}
			}
		close(errChan)
	}()
	return errChan
}



func(s *DataCategoryStore) OpenDefaultBucket() (err error){
	funcName := "DataCategoryStore.OpenDefaultBucket." + s.storeKey
	tracelog.Started(packageName, funcName)
	bucketName := []byte("0")
	s.bucket, err  = s.tx.CreateBucketIfNotExists(bucketName)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Creating default bucket on %v  store %v", s.storeKey, s.storeKey)
		return err
	}
	tracelog.Completed(packageName,funcName)
	return
}

func (s *DataCategoryStore) Close() (err error) {
	funcName := "DataCategoryStore.Close"
	tracelog.Started(packageName, funcName)
	if s.tx != nil {
		err = s.tx.Commit()
		s.tx = nil
//		fmt.Println("close transaction " + s.storeKey)
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Closing transaction on store %v", s.storeKey)
			return
		}

	}

	if s.store != nil {
		err = s.store.Close()
		s.store = nil
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Closing database for %v store %v", s.storeKey, s.pathToStoreDir)
			return
		}
	}
	tracelog.Completed(packageName, funcName)
	return
}



type DataCategorySimpleType struct {
	ByteLength         int
	IsNumeric          bool
	IsNegative         bool
	FloatingPointScale int
	IsSubHash          bool
	SubHash            uint
}

func (simple *DataCategorySimpleType) Key() (result string) {
	if !simple.IsNumeric {
		result = fmt.Sprintf("C%v", simple.ByteLength)
	} else {
		if simple.FloatingPointScale > 0 {
			if simple.IsNegative {
				result = "M"
			} else {
				result = "F"
			}
			result = result + fmt.Sprintf("%vP%v", simple.ByteLength, simple.FloatingPointScale)
		} else {
			if simple.IsNegative {
				result = "I"
			} else {
				result = "N"
			}
			result = result + fmt.Sprintf("%v", simple.ByteLength)
		}
	}
	if simple.IsSubHash {
		result = result + fmt.Sprintf("H%v", simple.SubHash)
	}
	return
}

func (simple *DataCategorySimpleType) covert() (result *DataCategoryType) {
	result = &DataCategoryType{
		IsNumeric:  nullable.NewNullBool(simple.IsNumeric),
		ByteLength: nullable.NewNullInt64(int64(simple.ByteLength)),
	}
	if simple.IsNumeric {
		result.IsNegative = nullable.NewNullBool(simple.IsNegative)
		result.FloatingPointScale = nullable.NewNullInt64(int64(simple.FloatingPointScale))
	}

	if simple.IsSubHash {
		result.SubHash = nullable.NewNullInt64(int64(simple.SubHash))
	}
	result.stringAnalysisChan = make(chan string,300)
	result.numericAnalysisChan = make(chan float64,300)
	return
}

type DataCategoryType struct {
	//column *metadata.ColumnInfoType
	ByteLength         nullable.NullInt64
	IsNumeric          nullable.NullBool // if array of bytes represents a numeric value
	IsNegative         nullable.NullBool
	FloatingPointScale nullable.NullInt64
	DataCount          nullable.NullInt64
	HashUniqueCount    nullable.NullInt64
	MinStringValue     nullable.NullString
	MaxStringValue     nullable.NullString
	MinNumericValue    nullable.NullFloat64
	MaxNumericValue    nullable.NullFloat64
	NonNullCount       nullable.NullInt64
	SubHash            nullable.NullInt64
	Stats              struct {
		MinStringValue  string
		MaxStringValue  string
		MinNumericValue float64
		MaxNumericValue float64
		NonNullCount    uint64
	}
	stringAnalysisChan  chan string
	numericAnalysisChan chan float64
	analysisChannelsLock sync.Mutex
}

func (dc *DataCategoryType) RunAnalyzer(ctx context.Context) (err error) {
	funcName := "DataCategoryType.RunAnalyzer"
	tracelog.Started(packageName,funcName)
	if dc.stringAnalysisChan == nil{
		dc.analysisChannelsLock.Lock()
		if dc.stringAnalysisChan == nil {
			dc.stringAnalysisChan = make(chan string,300)
			dc.numericAnalysisChan = make(chan float64,300)
		}
		dc.analysisChannelsLock.Unlock()
	}

		go func() {
			funcName1 := "DataCategoryType.RunAnalyzer.gofunc1"
			tracelog.Started(packageName,funcName1)
			outer:
			for {
				select {
				case <-ctx.Done():
					break outer
				case stringValue, opened := <-dc.stringAnalysisChan:
					if !opened {
						break outer
					}
					if dc.NonNullCount.Reference() == nil {
						dc.NonNullCount = nullable.NewNullInt64(int64(0))
					}
					if !dc.MaxStringValue.Valid() {
						dc.MaxStringValue = nullable.NewNullString("")
					}
					if !dc.MinStringValue.Valid() {
						dc.MinStringValue = nullable.NewNullString("")
					}

					dc.Stats.NonNullCount++
					if stringValue > dc.Stats.MaxStringValue {
						dc.Stats.MaxStringValue = stringValue
					}
					if stringValue < dc.Stats.MinStringValue || dc.Stats.MinStringValue == "" {
						dc.Stats.MinStringValue = stringValue
					}
				}
			}
			tracelog.Completed(packageName,funcName1)
		}()

		go func() {
			funcName1 := "DataCategoryType.RunAnalyzer.gofunc2"
			tracelog.Started(packageName,funcName1)
			outer:
			for {
				select {
				case <-ctx.Done():
					break outer
				case floatValue, opened := <-dc.numericAnalysisChan:
					if !opened {
						break outer
					}
					if !dc.MaxNumericValue.Valid() {
						dc.MaxNumericValue = nullable.NewNullFloat64(float64(0))
						dc.Stats.MaxNumericValue = -math.MaxFloat64
					}

					if !dc.MinNumericValue.Valid() {
						dc.MinNumericValue = nullable.NewNullFloat64(float64(0))
						dc.Stats.MinNumericValue = math.MaxFloat64

					}
					if floatValue > dc.Stats.MaxNumericValue {
						dc.Stats.MaxNumericValue = floatValue
					}
					if floatValue < dc.Stats.MinNumericValue {
						dc.Stats.MinNumericValue = floatValue
					}

				}
			}
			tracelog.Completed(packageName,funcName1)
		}()


	tracelog.Completed(packageName,funcName)
	return
}

func (dc *DataCategoryType) CloseAnalyzerChannels() {
	if dc.numericAnalysisChan != nil {
		close(dc.numericAnalysisChan)
	}
	if dc.stringAnalysisChan != nil {
		close(dc.stringAnalysisChan)
	}
	if dc.MinStringValue.Valid() {
		dc.MinStringValue = nullable.NewNullString(dc.Stats.MinStringValue)
		dc.MaxStringValue = nullable.NewNullString(dc.Stats.MaxStringValue)
	}
	if dc.MinNumericValue.Valid() {
		dc.MinNumericValue = nullable.NewNullFloat64(dc.Stats.MinNumericValue)
		dc.MaxNumericValue = nullable.NewNullFloat64(dc.Stats.MaxNumericValue)
	}
}

func (cdc DataCategoryType) Key() (result string) {
	simple := DataCategorySimpleType{
		ByteLength:         int(cdc.ByteLength.Value()),
		IsNumeric:          cdc.IsNumeric.Value(),
		IsNegative:         cdc.IsNegative.Value(),
		FloatingPointScale: int(cdc.FloatingPointScale.Value()),
		IsSubHash:          cdc.SubHash.Valid(),
		SubHash:            uint(cdc.SubHash.Value()),
	}
	return simple.Key()
}

func (cdc DataCategoryType) String() (result string) {
	if !cdc.IsNumeric.Value() {
		result = fmt.Sprintf("char[%v]", cdc.ByteLength.Value())
	} else {
		if cdc.IsNegative.Value() {
			result = "-"
		} else {
			result = "+"
		}
		if cdc.FloatingPointScale.Value() != 0 {
			result = fmt.Sprintf("%vF[%v,%v]",
				result,
				cdc.ByteLength,
				cdc.FloatingPointScale.Value(),
			)
		} else {
			result = fmt.Sprintf("%vI[%v]",
				result,
				cdc.ByteLength,
			)
		}
	}
	if cdc.SubHash.Valid() {
		result = fmt.Sprintf("%v(%v)", result, cdc.SubHash.Value())
	}
	return
}
