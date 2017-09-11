package dataflow

import (
	"astra/nullable"
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/goinggo/tracelog"
	"math"
	"os"
	"sparsebitset"
	"sync"
)

type DataCategorySimpleType struct {
	ByteLength int
	IsNumeric  bool
	IsNegative bool
	IsInteger  bool
}

type stringCategoryKeyStorageType struct {
	codes map[uint16]string
	sync.Mutex
}

var stringCategoryKeyStorage = stringCategoryKeyStorageType{
	codes: make(map[uint16]string),
}

//var stringCategoryKeyCodeCache []string;

func (simple *DataCategorySimpleType) Key() (result string) {
	if !simple.IsNumeric {
		vLen16 := uint16(simple.ByteLength)
		stringCategoryKeyStorage.Lock()
		if code, found := stringCategoryKeyStorage.codes[vLen16]; !found {
			code = fmt.Sprintf("C%v", vLen16)
			stringCategoryKeyStorage.codes[vLen16] = code
			result = code
		} else {
			result = code
		}
		stringCategoryKeyStorage.Unlock()
	} else if simple.IsNegative {
		if simple.IsInteger {
			result = "N"
		} else {
			result = "n"
		}
	} else {
		if simple.IsInteger {
			result = "P"
		} else {
			result = "p"
		}
	}
	return result
}

func (simple *DataCategorySimpleType) NewDataCategory() (result *DataCategoryType) {
	result = &DataCategoryType{
		IsNumeric: nullable.NewNullBool(simple.IsNumeric),
	}
	if simple.IsNumeric {
		result.IsNegative = nullable.NewNullBool(simple.IsNegative)
		result.IsInteger = nullable.NewNullBool(simple.IsInteger)
	} else {
		result.ByteLength = nullable.NewNullInt64(int64(simple.ByteLength))
	}
	result.Stats.MinNumericValue = math.MaxFloat64
	result.Stats.MaxNumericValue = -math.MaxFloat64
	result.Stats.NonNullCount = 0

	return
}

func (simple *DataCategorySimpleType) BinKey() (result []byte) {
	funcName := "DataCategorySimpleType.KeyBin"
	tracelog.Started(packageName, funcName)

	result = make([]byte, 2, 2)

	if simple.IsNumeric {
		result[0] = byte(1 << 7)
		if simple.IsInteger {
			result[0] = result[0] | byte(1<<6)
		}
		if simple.IsNegative {
			result[0] = result[0] | byte(1<<5)
		}
	} else {
		bl := uint16(simple.ByteLength)
		result[1] = byte(bl & 0xFF)
		result[0] = byte((bl >> 8) & 0x7F)
	}
	tracelog.Completed(packageName, funcName)
	return result
}

type DataCategoryType struct {
	Column          *ColumnInfoType
	ByteLength      nullable.NullInt64
	IsNumeric       nullable.NullBool // if array of bytes represents a numeric value
	IsNegative      nullable.NullBool
	IsInteger       nullable.NullBool
	HashUniqueCount nullable.NullInt64
	NonNullCount    nullable.NullInt64
	MinStringValue  nullable.NullString
	MaxStringValue  nullable.NullString
	MinNumericValue nullable.NullFloat64
	MaxNumericValue nullable.NullFloat64
	Stats           struct {
		MinStringValue          string
		MaxStringValue          string
		MinNumericValue         float64
		MaxNumericValue         float64
		NonNullCount            uint64
		ItemCount               uint64
		MovingMean              float64
		MovingStandardDeviation float64
		ContentBitset           *sparsebitset.BitSet
		ContentBitsetCardinality uint64
		HashBitset              *sparsebitset.BitSet
		HashBitsetCardinality uint64
	}
	ItemCount               nullable.NullInt64
	Key                     string
	MovingMean              nullable.NullFloat64
	MovingStandardDeviation nullable.NullFloat64
}
type BitsetFileSuffixType string

var (
	Hash BitsetFileSuffixType = "hash"
	Cont BitsetFileSuffixType = "cont"
)

func (dataCategory DataCategoryType) String() (result string) {
	result = fmt.Sprintf("DataCategory(Key:%v) on %v.%v.", dataCategory.Column.TableInfo, dataCategory.Column, dataCategory.Key)
	return
}

func (dataCategory DataCategoryType) BitsetFileName(suffix BitsetFileSuffixType) (fileName string, err error) {
	funcName := "DataCategoryType.HashBitsetFileName"
	tracelog.Started(packageName, funcName)
	fileName = fmt.Sprintf("%v.%v.%v.bitset",
		dataCategory.Column.Id.String(),
		dataCategory.Key,
		suffix,
	)

	tracelog.Completed(packageName, funcName)

	return fileName, nil
}


func composeBistsetFileFullPath(pathToDir, fileName string) string {
	fullPathFileName := fmt.Sprintf("%v%c%v", pathToDir, os.PathSeparator, fileName)
	return fullPathFileName
}


func (dataCategory DataCategoryType) WriteBitsetToDisk(ctx context.Context, pathToDir string, suffix BitsetFileSuffixType) (err error) {
	funcName := "DataCategoryType.WriteHashBitsetToDisk"

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

	fileName, err := dataCategory.BitsetFileName(suffix)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Reading %v bitset data from %v", suffix, pathToDir)
		return err
	}

	fullPathFileName := composeBistsetFileFullPath(pathToDir, fileName)

	file, err := os.Create(fullPathFileName)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Creating file for %v bitset %v", suffix, fullPathFileName)
		return err
	}

	defer file.Close()

	buffered := bufio.NewWriter(file)

	defer buffered.Flush()
	if suffix == Hash {
		_, err = dataCategory.Stats.HashBitset.WriteTo(ctx, buffered)
	} else {
		_, err = dataCategory.Stats.ContentBitset.WriteTo(ctx, buffered)
	}
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Writing %v bitset data to file %v", suffix, fullPathFileName)
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

	if suffix == Hash {
		_, err = dataCategory.Stats.HashBitset.ReadFrom(ctx, buffered)
		if err == nil {
			dataCategory.Stats.HashBitsetCardinality = dataCategory.Stats.HashBitset.Cardinality()
		}
	} else {
		_, err = dataCategory.Stats.ContentBitset.ReadFrom(ctx, buffered)
		if err == nil {
			dataCategory.Stats.ContentBitsetCardinality = dataCategory.Stats.ContentBitset.Cardinality()
		}
	}
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Reading %v bitset data to file %v", suffix, fullPathFileName)
		return err
	}

	tracelog.Completed(packageName, funcName)

	return err
}

func (dataCategory DataCategoryType) ResetBitset(suffixType BitsetFileSuffixType) {
	if suffixType == Hash {
		if dataCategory.Stats.HashBitset != nil{
			dataCategory.Stats.HashBitset = nil
		}
		dataCategory.Stats.HashBitsetCardinality = 0
	} else {
		if dataCategory.Stats.ContentBitset != nil{
			dataCategory.Stats.ContentBitset = nil
		}
		dataCategory.Stats.ContentBitsetCardinality = 0
	}
}


func (dataCategory *DataCategoryType) UpdateStatistics(runContext context.Context) (err error) {

	if dataCategory.IsNumeric.Value() {
		dataCategory.MaxNumericValue = nullable.NewNullFloat64(dataCategory.Stats.MaxNumericValue)
		dataCategory.MinNumericValue = nullable.NewNullFloat64(dataCategory.Stats.MinNumericValue)
		dataCategory.MaxStringValue = nullable.NullString{}
		dataCategory.MinStringValue = nullable.NullString{}
	} else {
		if len(dataCategory.Stats.MaxStringValue) > VarcharMax {
			dataCategory.Stats.MaxStringValue = dataCategory.Stats.MaxStringValue[:VarcharMax]
		}
		dataCategory.MaxStringValue = nullable.NewNullString(dataCategory.Stats.MaxStringValue)

		if len(dataCategory.Stats.MinStringValue) > VarcharMax {
			dataCategory.Stats.MinStringValue = dataCategory.Stats.MinStringValue[:VarcharMax]
		}
		dataCategory.MinStringValue = nullable.NewNullString(dataCategory.Stats.MinStringValue)
	}
	dataCategory.NonNullCount = nullable.NewNullInt64(int64(dataCategory.Stats.NonNullCount))

	if dataCategory.Stats.HashBitset != nil {
		dataCategory.HashUniqueCount = nullable.NewNullInt64(int64(dataCategory.Stats.HashBitset.Cardinality()))
	}

	bitset := dataCategory.Stats.ContentBitset

	if bitset != nil {
		dataCategory.ItemCount = nullable.NewNullInt64(int64(bitset.Cardinality()))
	}

	if dataCategory.IsNumeric.Value() && bitset != nil {

		var count uint64 = 0
		var meanValue, cumulativeDeviation float64 = 0, 0

		increasingOrder := context.WithValue(runContext, "sort", true)

		var prevValue = uint64(0)
		var gotPrevValue = false
		for value := range bitset.BitChan(increasingOrder) {
			if !gotPrevValue {
				prevValue = value
				gotPrevValue = true
			} else {
				count++
				cumulativeDeviation += float64(value) - float64(prevValue)
				prevValue = value
			}
		}
		if count > 0 {
			var countInDeviation uint64 = 0
			meanValue = cumulativeDeviation / float64(count)
			var totalDeviation = float64(0)

			var prevValue = uint64(0)
			var gotPrevValue = false
			for value := range bitset.BitChan(increasingOrder) {
				if !gotPrevValue {
					prevValue = value
					gotPrevValue = true
				} else {
					countInDeviation++
					totalDeviation = totalDeviation + math.Pow(meanValue-(float64(value)-float64(prevValue)), 2)
					prevValue = value
				}
			}
			if countInDeviation > 1 {
				stdDev := math.Sqrt(totalDeviation / float64(countInDeviation-1))
				dataCategory.MovingStandardDeviation = nullable.NewNullFloat64(stdDev)
			}
			dataCategory.MovingMean = nullable.NewNullFloat64(meanValue)
		}
	}

	return err
}

/*

func(dataCategory DataCategoryType) HashBitsetBucketNameBytes() (result []byte, err error){
	if dataCategory.Key == "" {
		err = fmt.Errorf("Data category Key is empty!")
		return
	}
	if !dataCategory.Column.Id.Valid() {
		err = fmt.Errorf("Column Id is empty!")
		return
	}
	keyLength := len(dataCategory.Key)
	result = make([]byte,binary.MaxVarintLen64 + keyLength)
	actual := binary.PutUvarint(result,uint64(dataCategory.Column.Id.Value()))
	copy(result[actual:],[]byte(dataCategory.Key))
	result = result[:actual+keyLength]
	return
}

type OffsetsType map[uint64][]uint64;
type H8BSType map[byte]*sparsebitset.BitSet;

type H8BuffType struct {
	 ColumnBlockType
	 packetNumber uint64
	 bucket byte
}

type СacheOffsetType struct {
	  packetNumber uint64
      Offsets OffsetsType
}


type DataCategoryStore struct {
	store           *bolt.DB
	bucket          *bolt.Bucket
	tx              *bolt.Tx
	pathToStoreDir string
	storeKey        string
	columnDataChan chan *ColumnDataType
	chanLock sync.Mutex
}




func (s *DataCategoryStore) Open(storeKey string, pathToStoreDir string,channelSize int) (err error) {
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
	//s.store, err = bolt.Open(pathToStoreFile, 700, &bolt.Options{InitialMmapSize: 16})
	//	s.store.MaxBatchSize = 100000
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening database for %v store %v", storeKey, pathToStoreFile)
		return err
	}

	//s.tx, err = s.store.Begin(true)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening transaction on %v store %v", storeKey, pathToStoreFile)
		return err
	}



	s.OpenDefaultBucket()

	if s.columnDataChan == nil {
		s.chanLock.Lock()
		if s.columnDataChan == nil {
			s.columnDataChan = make(chan *ColumnDataType, channelSize)
		}
		s.chanLock.Unlock()
	}
	return
}

func (s *DataCategoryStore) RunStore(runContext context.Context) (errChan chan error) {
	errChan = make(chan error, 1)
	var wgWorker sync.WaitGroup
	var wgDrainer sync.WaitGroup

	workerChannels := make([](chan *ColumnDataType),256);
	drainChan := make(chan*H8BuffType)




	drainToDisk := func(data *H8BuffType) (error) {
		pathToFile := filepath.Join(
			s.pathToStoreDir,
			fmt.Sprintf("%v.%X.cache", data.packetNumber,data.bucket),
		)
		file, err := os.Create(pathToFile)
		if err != nil {
			return err
		}
		defer file.Close()
		gzfile := gzip.NewWriter(file)
		defer gzfile.Close()
		buff := bufio.NewWriter(gzfile)
		defer buff.Flush()
		gobEnc := gob.NewEncoder(buff)
		err = gobEnc.Encode(data.Data)
		defer runtime.GC()
		return err
	}

	worker := func(bucket byte, wc chan*ColumnDataType) {
		countPackets := uint64(0)
		buffer := new (H8BuffType)
		buffer.bucket = bucket
		toDrain := func() {
			countPackets++
			buffer.packetNumber = countPackets
			drainChan <- buffer
			buffer = new (H8BuffType)
		}
		outer:
		for {
			select {
			case <-runContext.Done():
				break outer
			case columnData,open := <-wc:
				if !open {
					fmt.Printf("final Drain %v\n",bucket)
					toDrain()
					break outer
			    }
				buffer.Append(columnData.Column.Id.Value(), columnData.LineOffset)
				if len(buffer.Data) > 1024*256 {
					toDrain()
				}

			}
		}
		wgWorker.Done()
	}

	wgDrainer.Add(1)
	go func() {
		outer:
		for {
			select {
			case <-runContext.Done():
				break outer
			case buffer, open := <-drainChan:
				if !open {
					break outer
				}

				err := drainToDisk(buffer)

				if err != nil {
					select {
					case <-runContext.Done():
					case errChan <- err:
						break outer
					}
				}
			}
		}
		wgDrainer.Done()

	}()


	wgWorker.Add(len(workerChannels))

	for index := range workerChannels {
		workerChannels[index] = make(chan *ColumnDataType,100)
		go worker(
			byte(index),
			workerChannels[index],
		)
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
				case <-runContext.Done():
					break outer
				case columnData, open := <-s.columnDataChan:
					if !open {
						break outer
					}
					workerChannels[columnData.HashValue[7]] <-columnData
				//	go func(index byte, data *ColumnDataType) {
				//		workerChannels[index] <-data
				//	}(columnData.HashValue[7],columnData)
				//
					//_=columnData

				//	columnBlock.Data = s.bucket.Get(columnData.RawData)
				//	columnBlock.Append(columnData.Column.Id.Value(), columnData.LineOffset)
		  	    //	s.bucket.Put(columnData.RawData, columnBlock.Data)


					//





					if false {
						found := false
						pathToChunk := fmt.Sprintf("%v%c%v",s.pathToStoreDir,os.PathSeparator,columnData.HashInt);
						pathToChunkRenamed := pathToChunk+".r"


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
					/ *l := len(columnBlock.Data)

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
					}* /
				}
			}
		for _,wc := range workerChannels {
			close(wc)
		}

		wgWorker.Wait()

		close(drainChan)
		wgDrainer.Wait()
		close(errChan)
	}()
	return errChan
}



func(s *DataCategoryStore) OpenDefaultBucket() (err error){
	funcName := "DataCategoryStore.OpenDefaultBucket." + s.storeKey
	tracelog.Started(packageName, funcName)
	//bucketName := []byte("0")
//	s.bucket, err  = s.tx.CreateBucketIfNotExists(bucketName)
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




type dataTypeAware interface {
	IsNumericDataType() bool
}





func (dc *DataCategoryType) RunAnalyzer(runContext context.Context,analysisChanSize int) (err error) {
	funcName := "DataCategoryType.RunAnalyzer"
	tracelog.Started(packageName,funcName)
	dc.initChans.Do(func(){
		dc.stringAnalysisChan = make(chan string,analysisChanSize)
		dc.numericAnalysisChan = make(chan float64,analysisChanSize)

	})


	dc.drainAnalysisChannels.Add(2)
	go func() {
		funcName1 := "DataCategoryType.RunAnalyzer.gofunc1"
		tracelog.Started(packageName,funcName1)
		outer:
		for {
			select {
			case <-runContext.Done():
				break outer
			case stringValue, open := <-dc.stringAnalysisChan:
				if !open {
					break outer
				}
				if !dc.NonNullCount.Valid()  {
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
		dc.drainAnalysisChannels.Done()
		tracelog.Completed(packageName,funcName1)
	}()

	go func() {
		funcName1 := "DataCategoryType.RunAnalyzer.gofunc2"
		tracelog.Started(packageName,funcName1)
		outer:
		for {
			select {
			case <-runContext.Done():
				break outer
			case floatValue, open := <-dc.numericAnalysisChan:
				if !open {
					break outer
				}
				if !dc.MaxNumericValue.Valid() {
					dc.MaxNumericValue = nullable.NewNullFloat64(float64(0))
					dc.Stats.MaxNumericValue = math.Inf(-1)
				}

				if !dc.MinNumericValue.Valid() {
					dc.MinNumericValue = nullable.NewNullFloat64(float64(0))
					dc.Stats.MinNumericValue = math.Inf(1)

				}
				if floatValue > dc.Stats.MaxNumericValue {
					dc.Stats.MaxNumericValue = floatValue
				}
				if floatValue < dc.Stats.MinNumericValue {
					dc.Stats.MinNumericValue = floatValue
				}
			}
		}
		dc.drainAnalysisChannels.Done()
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
	dc.drainAnalysisChannels.Wait()

	if dc.MinStringValue.Valid() {
		dc.MinStringValue = nullable.NewNullString(dc.Stats.MinStringValue)
		dc.MaxStringValue = nullable.NewNullString(dc.Stats.MaxStringValue)
	}
	if dc.MinNumericValue.Valid() {
		dc.MinNumericValue = nullable.NewNullFloat64(dc.Stats.MinNumericValue)
		dc.MaxNumericValue = nullable.NewNullFloat64(dc.Stats.MaxNumericValue)
	}
	if dc.NonNullCount.Valid() {
		dc.NonNullCount = nullable.NewNullInt64(int64(dc.Stats.NonNullCount))
	}
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
*/
