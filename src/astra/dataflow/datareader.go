package dataflow

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/goinggo/tracelog"
	"io"
	"os"
	"sync"

	"astra/B8"
	"github.com/boltdb/bolt"
	"hash/fnv"
	"io/ioutil"
	"sparsebitset"
	"strconv"
	"strings"
)

type DumpConfigType struct {
	BasePath        string
	TankPath        string
	StoragePath     string
	GZipped         bool
	FieldSeparator  byte
	LineSeparator   byte
	InputBufferSize int
	HashValueLength int
}

type DataReaderType struct {
	Config *DumpConfigType
}

func (dr DataReaderType) ReadSource(ctx context.Context, table *TableInfoType, colDataPool chan *ColumnDataType) (
	outChan chan *ColumnDataType,
	errChan chan error,
) {
	funcName := "DataReaderType.ReadSource"
	tracelog.Startedf(packageName, funcName, "for table %v", table)

	var x0D = []byte{0x0D}
	var wg sync.WaitGroup

	outChan = make(chan *ColumnDataType, 1000)
	errChan = make(chan error, 1)

	writeToTank := func(rowData [][]byte) (result int) {
		columnCount := len(rowData)
		binary.Write(table.TankWriter, binary.LittleEndian, uint16(columnCount)) //
		result = 2
		for _, colData := range rowData {
			colDataLength := len(colData)
			binary.Write(table.TankWriter, binary.LittleEndian, uint16(colDataLength))
			result = result + 2
			table.TankWriter.Write(colData)
			result = result + colDataLength
		}
		return
	}

	splitToColumns := func(LineNumber, LineOffset uint64, column *ColumnInfoType, columnBytes []byte) {
		var hashMethod = fnv.New64()
		defer wg.Done()

		byteLength := len(columnBytes)
		if byteLength == 0 {
			return
		}
		var columnData *ColumnDataType

		select {
		case <-ctx.Done():
			return
		case columnData = <-colDataPool:
		default:
			columnData = new(ColumnDataType)
		}

		if columnData.RawData == nil || cap(columnData.RawData) < byteLength {
			columnData.RawData = make([]byte, 0, byteLength)
		} else {
			columnData.RawData = columnData.RawData[0:0]
		}

		if columnData.HashValue == nil || cap(columnData.HashValue) < 8 {
			columnData.HashValue = make([]byte, 8, 8)
		} else {
			columnData.HashValue = append(columnData.HashValue[0:0], ([]byte{0, 0, 0, 0, 0, 0, 0, 0})...)
		}
		columnData.RawDataLength = byteLength
		columnData.LineNumber = LineNumber
		columnData.LineOffset = LineOffset
		columnData.Column = column

		columnData.RawData = append(columnData.RawData, (columnBytes)...)

		if columnData.RawDataLength > dr.Config.HashValueLength {
			hashMethod.Reset()
			hashMethod.Write(columnData.RawData)
			columnData.HashInt = hashMethod.Sum64()
			B8.UInt64ToBuff(columnData.HashValue, columnData.HashInt)
		} else {
			copy(columnData.HashValue[dr.Config.HashValueLength-byteLength:], columnBytes)
			columnData.HashInt, _ = B8.B8ToUInt64(columnData.HashValue)
		}
		if false || "!CREDIT_BLOCKED" == column.ColumnName.String() {
			fmt.Printf("%v %v %v\n", column.ColumnName.String(), string(columnBytes), columnData.HashValue)
		}
		select {
		case <-ctx.Done():
			return
		case outChan <- columnData:
		}
	}

	readFromDump := func() (err error) {
		funcName := "DataReaderType.ReadSource.readFromDump"

		var tankCancelFunc context.CancelFunc
		var tankContext context.Context

		gzFile, err := os.Open(dr.Config.BasePath + table.PathToFile.Value())
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "for table %v", table)
			return
		}
		defer gzFile.Close()
		file, err := gzip.NewReader(gzFile)
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "for table %v", table)
			return
		}
		defer file.Close()
		bufferedFile := bufio.NewReaderSize(file, dr.Config.InputBufferSize)

		lineNumber := uint64(0)
		lineOffset := uint64(0)
		for {
			line, err := bufferedFile.ReadSlice(dr.Config.LineSeparator)
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			//lineImageLen := len(lineImage)

			//line:= make([]byte, lineImageLen, lineImageLen)
			//copy(line, lineImage)

			line = bytes.TrimSuffix(line, []byte{dr.Config.LineSeparator})
			line = bytes.TrimSuffix(line, x0D)

			metadataColumnCount := len(table.Columns)

			lineColumns := bytes.Split(line, []byte{dr.Config.FieldSeparator})
			lineColumnCount := len(lineColumns)

			if metadataColumnCount != lineColumnCount {
				err = fmt.Errorf("Number of column mismatch in line %v. Expected #%v; Actual #%v",
					lineNumber,
					metadataColumnCount,
					lineColumnCount,
				)
				return err
			}

			lineNumber++
			if lineNumber == 1 {
				tankContext, tankCancelFunc = context.WithCancel(context.Background())
				err = table.OpenTank(tankContext, dr.Config.TankPath, os.O_CREATE)
				if err != nil {
					return err
				}
				defer tankCancelFunc()
			}

			/*result := &RowDataType{
				RawData:    lineColumns,
				LineNumber: lineNumber,
				LineOffset: lineOffset,
				Table:      table,
			}*/

			for columnNumber, columnDataBytes := range lineColumns {
				wg.Add(1)
				go splitToColumns(lineNumber, lineOffset, table.Columns[columnNumber], columnDataBytes)
			}
			wg.Wait()

			/*select {
			case rowDataChan <- result:
			case <-ctx.Done():
				return ctx.Err()
			}*/
			lineOffset = lineOffset + uint64(writeToTank(lineColumns))

		}
		return nil
	}

	go func() {
		err := readFromDump()
		if err != nil {
			errChan <- err
		}

		close(outChan)
		close(errChan)
	}()

	tracelog.Completedf(packageName, funcName, "for table %v", table)
	return outChan, errChan
}

type StorageType struct {
	db     *bolt.DB
	tx     *bolt.Tx
	bucket *bolt.Bucket
}

type StorageColumnBlockType struct {
	Data []byte
}

var offsetPoolSizeLog2 uint = 4

/*
func(s StorageColumnBlockType) ColumnId() []uint64 {
	pointer:=0;
	for {
		columnId := binary.LittleEndian.Uint64(s.data[pointer:pointer+8])
		pointer  += 8;
		watermark := binary.LittleEndian.Uint64(s.data[pointer:pointer+8])
		pointer  += 8;
		pointer = ((watermark>>10)+1)<<10 // (floor(watermark/10)+1)*10

	}
}*/
func NextPagePosition(currentPossition uint64) uint64 {
	return ((currentPossition >> offsetPoolSizeLog2) + 1) << offsetPoolSizeLog2
}

var (
	columnIdPosition    = uint64(0)
	countKeyValPosition = uint64(8)
	keyValStartPosition = uint64(16)
	keyLen              = uint64(8)
	valLen              = uint64(8)
)

func (s *StorageColumnBlockType) Append(columnId uint64, offset uint64) (err error) {
	// columnId1,watermarkToLastOffset[offset1,offset2...offset128]
	base, bitPosition := sparsebitset.OffsetBits(offset)
	/*if s.Data == nil {
		s.Data = make([]byte, keyValStartPosition+keyLen+valLen)
		binary.LittleEndian.PutUint64(s.Data[columnIdPosition:], columnId)
		binary.LittleEndian.PutUint64(s.Data[countKeyValPosition:], uint64(1))
		binary.LittleEndian.PutUint64(s.Data[keyValStartPosition:], base)
		binary.LittleEndian.PutUint64(s.Data[keyValStartPosition+keyLen:], 1<<bitPosition)
	} else {*/
		sourcePosition := uint64(0)
		destPosition := uint64(0)
		dataLen := uint64(len(s.Data))
		columnFound := false
		// making a new buffer as large like the worst scenario: we add a new column
		newBuffer := make([]byte, 0, dataLen+keyValStartPosition+keyLen+valLen)
		for dataLen > 0 && sourcePosition < dataLen {
			storedColumnId := binary.LittleEndian.Uint64(s.Data[sourcePosition+columnIdPosition:])
			keyValCount := binary.LittleEndian.Uint64(s.Data[sourcePosition+countKeyValPosition:])
			bytesToCopy := (keyValStartPosition + keyValCount*(keyLen+valLen))
			newBuffer = append(newBuffer, s.Data[sourcePosition:sourcePosition+bytesToCopy]...)
			if storedColumnId == columnId {
				columnFound = true
				currentKeyValPosition := destPosition + keyValStartPosition
				baseFound := false
				for index := uint64(0); index < keyValCount; index++ {
					storedBase := binary.LittleEndian.Uint64(newBuffer[currentKeyValPosition:])
					currentKeyValPosition += keyLen
					if storedBase == base {
						storedBits := binary.LittleEndian.Uint64(newBuffer[currentKeyValPosition:])
						newBits := storedBits | (1 << bitPosition)
						binary.LittleEndian.PutUint64(newBuffer[currentKeyValPosition:], newBits)
						baseFound = true
						break
					}
					currentKeyValPosition += valLen
				}
				if !baseFound {
					keyValCount += 1
					newBuffer = append(newBuffer, make([]byte, valLen+keyLen)...)
					binary.LittleEndian.PutUint64(newBuffer[destPosition+countKeyValPosition:], keyValCount)
					binary.LittleEndian.PutUint64(newBuffer[destPosition+bytesToCopy:], base)
					destPosition += keyLen
					binary.LittleEndian.PutUint64(newBuffer[destPosition+bytesToCopy:], (1 << bitPosition))
					destPosition += valLen
				}
			}
			sourcePosition += bytesToCopy
			destPosition += bytesToCopy
		}
		if !columnFound {
			newBuffer = append(newBuffer, make([]byte, keyValStartPosition+keyLen+valLen)...)
			binary.LittleEndian.PutUint64(newBuffer[destPosition+columnIdPosition:], columnId)
			binary.LittleEndian.PutUint64(newBuffer[destPosition+countKeyValPosition:], uint64(1))
			binary.LittleEndian.PutUint64(newBuffer[destPosition+keyValStartPosition:], base)
			binary.LittleEndian.PutUint64(newBuffer[destPosition+keyValStartPosition+keyLen:], 1<<bitPosition)
		}
		s.Data = newBuffer
	//}
	ioutil.WriteFile("./block", s.Data, 700)
	return
}

func (dr DataReaderType) StoreByDataCategory(ctx context.Context, columnDataChan chan *ColumnDataType, degree int) (
	outChan chan *ColumnDataType,
	errChan chan error,
) {
	errChan = make(chan error, 1)
	outChan = make(chan *ColumnDataType, 1000)
	var wg sync.WaitGroup
	var storages map[string]*StorageType = make(map[string]*StorageType)

	processColumnData := func() {
	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case columnData, opened := <-columnDataChan:
				if !opened {
					break outer
				}
				if columnData.RawDataLength == 0 {
					continue
				}
				stringValue := string(columnData.RawData)

				floatValue, parseError := strconv.ParseFloat(stringValue, 64)
				simple := &DataCategorySimpleType{
					ByteLength: columnData.RawDataLength,
					IsNumeric:  parseError == nil,
					IsSubHash:  false, //byteLength > da.SubHashByteLengthThreshold
				}

				if simple.IsNumeric {
					//var lengthChanged bool
					if strings.Count(stringValue, ".") == 1 {
						//trimmedValue := strings.TrimLeft(stringValue, "0")
						//lengthChanged = false && (len(stringValue) != len(trimmedValue)) // Stop using it now
						//if lengthChanged {
						//	stringValue = trimmedValue
						//}
						simple.FloatingPointScale = len(stringValue) - (strings.Index(stringValue, ".") + 1)
						//if fpScale != -1 && lengthChanged {
						//	stringValue = strings.TrimRight(fmt.Sprintf("%f", floatValue), "0")
						//	columnData.RawData = []byte(stringValue)
						//	byteLength = len(columnData.RawData)
						//}
					} else {
						simple.FloatingPointScale = 0
					}

					simple.IsNegative = floatValue < float64(0)

					//TODO:REDESIGN THIS!
					//cd.Column.AnalyzeNumericValue(floatValue);
				}
				//TODO:REDESIGN THIS!
				//cd.Column.AnalyzeStringValue(floatValue);
				storageKey := ""
				if columnData.RawDataLength < 8 {
					storageKey = "R"
				} else {
					storageKey = simple.Key()
				}
				var storage *StorageType
				if value, found := storages[storageKey]; !found {
					db, err := bolt.Open("./"+storageKey+".bolt.db", 0700, nil)
					if err != nil {
						errChan <- err
						break outer
					}
					tx, err := db.Begin(true)
					if err != nil {
						errChan <- err
						break outer
					}

					bucket, err := tx.CreateBucketIfNotExists([]byte("0"))
					if err != nil {
						errChan <- err
						break outer
					}

					storage := &StorageType{
						db:     db,
						tx:     tx,
						bucket: bucket,
					}

					storages[storageKey] = storage

				} else {
					storage = value
				}

				if storage.tx == nil {
					tx, err := storage.db.Begin(true)
					if err != nil {
						errChan <- err
						break outer
					}
					storage.tx = tx
					storage.bucket = tx.Bucket([]byte("0"))
				}

				//storageValue := storage.bucket.Get(columnData.RawData)

				dataCategory, err := columnData.Column.CategoryByKey(
					columnData.dataCategoryKey,
					func() (result *DataCategoryType, err error) {
						result = simple.covert()
						count := len(columnData.Column.Categories)
						if count == 0 {
							columnData.Column.hashStorage = new(boltStorageGroupType)
							columnData.Column.bitsetStorage = new(boltStorageGroupType)
							err = columnData.Column.hashStorage.Open(
								"hash",
								dr.Config.StoragePath,
								columnData.Column.Id.Value(),
							)
							if err != nil {
								return
							}
							err = columnData.Column.bitsetStorage.Open(
								"bitset",
								dr.Config.StoragePath,
								columnData.Column.Id.Value(),
							)
							if err != nil {
								return
							}

							//TODO: e3 is a channel
							e3 := columnData.Column.RunStorage(ctx)
							go func() {
								select {
								case <-ctx.Done():
								case err := <-e3:
									if err != nil {
										errChan <- err
									}
								}
							}()
						}
						err = result.RunAnalyzer(ctx)
						if err != nil {
							return
						}
						return
					},
				)

				if err != nil {
					errChan <- err
					break outer
				}

				select {
				case <-ctx.Done():
					break outer
				case dataCategory.stringAnalysisChan <- stringValue:
				}

				if simple.IsNumeric {
					select {
					case <-ctx.Done():
						break outer
					case dataCategory.numericAnalysisChan <- floatValue:
					}
				}

				select {
				case <-ctx.Done():
					break outer
				case columnData.Column.columnDataChan <- columnData:
				}

				select {
				case <-ctx.Done():
					break outer
				case outChan <- columnData:
				}
			}
		}
		wg.Done()
		return
	}

	//degree = 1
	wg.Add(degree)

	for ; degree > 0; degree-- {
		go processColumnData()
	}

	go func() {
		wg.Wait()
		close(outChan)
		close(errChan)
	}()

	return outChan, errChan
}

/*
func(da *DataReaderType) WriteToTank( ctx context.Context, InRowDataChan  chan *RowDataType) (
		outRowDataChan  chan *RowDataType,
		outErrChan chan error,
	) {
	outRowDataChan = make(chan *RowDataType);
	outErrChan = make(chan error,1)
	var wg sync.WaitGroup;
	wg.Add(1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				break;
			case rowData := <-InRowDataChan:
				if rowData.Table.Tank == nil {
					{
						err := os.MkdirAll(da.config.TankPath, 0);
						select {
						case outErrChan <- err:
							break;
						}
					}
					{
						pathToTank := fmt.Sprintf("%v%v%v.tank",
								da.config.TankPath,
								os.PathSeparator,
								rowData.Table.Id.String(),
							)
						if _, err := os.Stat(pathToTank); os.IsNotExist(err) {
							file, err := os.Create(pathToTank)
							if err != nil {
								select {
								case outErrChan <- err:
									break;
								}
							}
							rowData.Table.Tank = file;
						}
					}
					rowData.WriteTo(rowData.Table.Tank)
				}
			}
		}
		wg.Done()
	} ()
	return
}
		for columnIndex := range table.Columns {
			if lineNumber == 1 {
				if columnIndex == 0 {
					statsChannels = make([]ColumnDataChannelType, 0, metadataColumnCount)
					//storeChans = make([]ColumnDataChannelType, 0,metadataColumnCount);

					redumpChannel = make(chan [][]byte,500)
					pathToBinData := "G:/BINDATA/"
					err = os.MkdirAll(pathToBinData, 0);
					redumpFile, err := os.Create(
						fmt.Sprintf("%v/%v.bindata",
							pathToBinData,
							table.Id.Value(),
						));
					defer redumpFile.Close()
					redump = bufio.NewWriterSize(redumpFile,lineImageLen*200);
					goBusy.Add(1)
					go func(colCount int, in chan [][]byte) {
						for columns := range in {
							err = binary.Write(redump, binary.BigEndian, uint16(colCount))
							for _, columnData := range (columns) {
								err = binary.Write(redump, binary.LittleEndian, uint16(len(columnData)))
							}
							for _, columnData := range (columns) {
								_, err = redump.Write(columnData)
							}
						}
						goBusy.Done()
					} (lineColumnCount, redumpChannel)

				}
				statsChan := make(ColumnDataChannelType, 100)
				statsChannels = append(statsChannels, statsChan)
				storeChan := make(ColumnDataChannelType, 100)

				goBusy.Add(2)
				go func(cnin, chout ColumnDataChannelType) {
					for iVal := range cnin {
						da.collectDataStats(iVal)
						//storeChans[index] <- iVal
						chout <- iVal
					}
					goBusy.Done()
					//close(storeChans[index])
					close(chout)
				}(statsChan, storeChan)

				go func(chin ColumnDataChannelType) {
					transactionCount := uint64(0)
					//ticker := uint64(0)
					for iVal := range chin {
						//tracelog.Info(packageName,funcName,"%v,%v",iVal.column,transactionCount)
						iVal.column.bucketLock.Lock()
						da.storeData(iVal)
						iVal.column.bucketLock.Unlock()
						transactionCount++
						if transactionCount > da.TransactionCountLimit {
							//tracelog.Info(packageName,funcName,"Intermediate commit for column %v",iVal.column)
							iVal.column.bucketLock.Lock()
							iVal.column.CloseStorageTransaction(true)
							iVal.column.bucketLock.Unlock()
							transactionCount = 0
						}
					}
					goBusy.Done()
				}(storeChan)
			}

			statsChannels[columnIndex] <- &ColumnDataType{
				column:     table.Columns[columnIndex],
				bValue:     lineColumns[columnIndex],
				lineNumber: lineNumber,
				lineOffset: binDataOffset,
			}
	wg.Done()
	} ()


	return columnDataChan,errChan
}





func (da *DataReaderType) fillColumnStorage(table *metadata.TableInfoType) {
	funcName := "DataReaderType.fillColumnStorage"
	var redump io.Writer

	var x0D = []byte{0x0D}

	//	lineSeparatorArray[0] = da.DumpConfiguration.LineSeparator
	var statsChannels  []ColumnDataChannelType
	var redumpChannel chan [][]byte;
	var goBusy sync.WaitGroup
	tracelog.Startedf(packageName, funcName, "for table %v", table)

	gzfile, err := os.Open(da.config.BasePath + table.PathToFile.Value())
	if err != nil {
		panic(err)
	}
	defer gzfile.Close()

	file, err := gzip.NewReader(gzfile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	rawData := bufio.NewReaderSize(file, da.config.InputBufferSize)
	lineNumber := uint64(0)
	binDataOffset :=uint64(0);

	for {
		lineImage, err := rawData.ReadSlice(da.config.LineSeparator)

		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		lineImageLen := len(lineImage)
		line := make([]byte, lineImageLen, lineImageLen)
		copy(line, lineImage)

		line = bytes.TrimSuffix(line, []byte{da.config.LineSeparator})
		line = bytes.TrimSuffix(line, x0D)
		lineNumber++

		metadataColumnCount := len(table.Columns)

		lineColumns := bytes.Split(line, []byte{da.config.FieldSeparator})
		lineColumnCount := len(lineColumns)
		if metadataColumnCount != lineColumnCount {
			panic(fmt.Sprintf("Number of column mismatch in line %v. Expected #%v; Actual #%v",
				lineNumber,
				metadataColumnCount,
				lineColumnCount,
			))
		}
		// columnCount:uiunt16, array of offsets:columnCount*2bytes+.. data
		if lineNumber == 1 {

		}

		for columnIndex := range table.Columns {
			if lineNumber == 1 {
				if columnIndex == 0 {
					statsChannels = make([]ColumnDataChannelType, 0, metadataColumnCount)
					//storeChans = make([]ColumnDataChannelType, 0,metadataColumnCount);

					redumpChannel = make(chan [][]byte,500)
					pathToBinData := "G:/BINDATA/"
					err = os.MkdirAll(pathToBinData, 0);
					redumpFile, err := os.Create(
						fmt.Sprintf("%v/%v.bindata",
							pathToBinData,
							table.Id.Value(),
						));
					defer redumpFile.Close()
					redump = bufio.NewWriterSize(redumpFile,lineImageLen*200);
					goBusy.Add(1)
					go func(colCount int, in chan [][]byte) {
						for columns := range in {
							err = binary.Write(redump, binary.BigEndian, uint16(colCount))
							for _, columnData := range (columns) {
								err = binary.Write(redump, binary.LittleEndian, uint16(len(columnData)))
							}
							for _, columnData := range (columns) {
								_, err = redump.Write(columnData)
							}
						}
						goBusy.Done()
					} (lineColumnCount, redumpChannel)

				}
				statsChan := make(ColumnDataChannelType, 100)
				statsChannels = append(statsChannels, statsChan)
				storeChan := make(ColumnDataChannelType, 100)

				goBusy.Add(2)
				go func(cnin, chout ColumnDataChannelType) {
					for iVal := range cnin {
						da.collectDataStats(iVal)
						//storeChans[index] <- iVal
						chout <- iVal
					}
					goBusy.Done()
					//close(storeChans[index])
					close(chout)
				}(statsChan, storeChan)

				go func(chin ColumnDataChannelType) {
					transactionCount := uint64(0)
					//ticker := uint64(0)
					for iVal := range chin {
						//tracelog.Info(packageName,funcName,"%v,%v",iVal.column,transactionCount)

						iVal.column.bucketLock.Lock()
						da.storeData(iVal)
						iVal.column.bucketLock.Unlock()
						transactionCount++
						if transactionCount > da.TransactionCountLimit {
							//tracelog.Info(packageName,funcName,"Intermediate commit for column %v",iVal.column)
							iVal.column.bucketLock.Lock()
							iVal.column.CloseStorageTransaction(true)
							iVal.column.bucketLock.Unlock()
							transactionCount = 0
						}
					}
					goBusy.Done()
				}(storeChan)
			}

			statsChannels[columnIndex] <- &ColumnDataType{
				column:     table.Columns[columnIndex],
				bValue:     lineColumns[columnIndex],
				lineNumber: lineNumber,
				lineOffset: binDataOffset,
			}




		}
		redumpChannel <- lineColumns

		binDataOffset += uint64((1+lineColumnCount)*2)
		for _, columnData := range (lineColumns) {
			binDataOffset += uint64(len(columnData))
		}

	}

	if statsChannels != nil {
		for index := range statsChannels {
			close(statsChannels[index])
		}
		close(redumpChannel)
		statsChannels = nil
	}
	goBusy.Wait()
	for index := range table.Columns {
		table.Columns[index].bucketLock.Lock()
		table.Columns[index].CloseStorageTransaction(true)
		table.Columns[index].CloseStorage()
		table.Columns[index].bucketLock.Unlock()
	}
	tracelog.Completedf(packageName, funcName, "for table %v", table)
}


func (da *DataReaderType) storeData(val *ColumnDataType) {
	//var rowNumberBitset []byte = []byte{byte(0)}
	//	funcName := "DataAccessType.storeData"
	bLen := uint16(len(val.bValue))
	if bLen == 0 || val.dataCategory == nil {
		return
	}


	hashFunc := fnv.New64()
	var hashUIntValue uint64
	var hValue []byte
	if bLen > hashLength {
		hashFunc.Reset()
		hashFunc.Write(val.bValue)
		hashUIntValue = hashFunc.Sum64()
		hValue = utils.UInt64ToB8(hashUIntValue)
	} else {
		hValue = make([]byte, hashLength)
		for index := uint16(0); index < bLen; index++ {
			hValue[index] = val.bValue[bLen-index-1]
		}
		hashUIntValue, _ = utils.B8ToUInt64(hValue)
		//fmt.Println("1",val.column,hValue,category)
	}
	//val.column.DataCategories[category] = true
	var err error
	if val.column.CategoriesBucket == nil {
		//tracelog.Info(packageName, funcName, "Lock for column %v",val.column)
		err = val.column.OpenStorage(true)
		if err != nil {
			panic(err)
		}
		err = val.column.OpenCategoriesBucket()
		if err != nil {
			panic(err)
		}

	}
	if val.dataCategory.CategoryBucket == nil {
		_, err = val.dataCategory.OpenBucket(nil)
		if err != nil {
			panic(err)
		}
	}

	if val.dataCategory.CategoryBucket == nil {
		panic("Category bucket has not been created!")
	}

	err = val.dataCategory.OpenHashValuesBucket()
	if err != nil {
		panic(err)
	}


	err = val.dataCategory.OpenBitsetBucket()
	if err != nil {
		panic(err)
	}

	baseUIntValue, offsetUIntValue := sparsebitset.OffsetBits(hashUIntValue)
	baseB8Value := utils.UInt64ToB8(baseUIntValue)
	//offsetB8Value := utils.UInt64ToB8(offsetUIntValue)
	bits, _ := utils.B8ToUInt64(val.dataCategory.BitsetBucket.Get(baseB8Value))
	bits = bits | (1 << offsetUIntValue)
	val.dataCategory.BitsetBucket.Put(baseB8Value, utils.UInt64ToB8(bits))
	//fmt.Println(val.column,string(val.bValue),hValue[:],val.lineNumber,utils.UInt64ToB8(val.lineNumber))
	//HashSourceBucket

	lineOffsetBytes := utils.UInt64ToB8(val.lineOffset);
	//categoryBytes,_ := val.dataCategory.ConvertToBytes();
	//,categoryBytes[:]...
	//val.column.RowsBucket.Put(lineOffsetBytes,append(hValue[:]));

	bitsetBytes := val.dataCategory.HashValuesBucket.Get(hValue[:] )

	if bitsetBytes == nil {
		bitsetBytes = lineOffsetBytes
		val.dataCategory.HashValuesBucket.Put(hValue[:], bitsetBytes)
	} else {

		var buffer *bytes.Buffer
		sb := sparsebitset.New(0)

		if len(bitsetBytes) == 8 {
			prevValue, _ := utils.B8ToUInt64(bitsetBytes)
			buffer = bytes.NewBuffer(make([]byte, 0, 8*3))
			sb.Set(prevValue)
		} else {
			readBuffer := bytes.NewBuffer(bitsetBytes)
			sb.ReadFrom(readBuffer)
			buffer = bytes.NewBuffer(make([]byte,0,len(bitsetBytes)+8))
		}

		sb.Set(val.lineOffset)
		sb.WriteTo(buffer)
		val.dataCategory.HashValuesBucket.Put(hValue[:], buffer.Bytes())
	}

	if hashRowCount,found := utils.B8ToUInt64(val.dataCategory.CategoryBucket.Get(columnInfoCategoryStatsRowCountKey)); !found {
		val.dataCategory.CategoryBucket.Put(
			columnInfoCategoryStatsRowCountKey,
			utils.UInt64ToB8(uint64(1)),
		)
	} else {
		val.dataCategory.CategoryBucket.Put(
			columnInfoCategoryStatsRowCountKey,
			utils.UInt64ToB8(hashRowCount+1),
		)
	}


}
*/

/*
func (dr DataReaderType) ReadSource(ctx context.Context, inPool chan*RowDataType, table *TableInfoType, ) (
	rowDataChan chan *RowDataType,
	errChan chan error,
) {
	funcName := "DataReaderType.ReadSource"
	tracelog.Startedf(packageName, funcName, "for table %v", table)

	var x0D = []byte{0x0D}

	rowDataChan = make(chan *RowDataType)
	errChan = make(chan error, 1)


	writeToTank := func(rowData [][]byte) (result int) {
		columnCount := len(rowData)
		binary.Write(table.TankWriter, binary.LittleEndian, uint16(columnCount)) //
		result = 2
		for _, colData := range rowData {
			colDataLength := len(colData)
			binary.Write(table.TankWriter, binary.LittleEndian, uint16(colDataLength))
			result = result + 2
			table.TankWriter.Write(colData)
			result = result + colDataLength
		}
		return
	}

	readFromDump := func() (err error) {
		funcName := "DataReaderType.ReadSource.readFromDump"

		var tankCancelFunc context.CancelFunc
		var tankContext context.Context

		gzFile, err := os.Open(dr.Config.BasePath + table.PathToFile.Value())
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "for table %v", table)
			return
		}
		defer gzFile.Close()
		file, err := gzip.NewReader(gzFile)
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "for table %v", table)
			return
		}
		defer file.Close()
		bufferedFile := bufio.NewReaderSize(file, dr.Config.InputBufferSize)

		lineNumber := uint64(0)
		lineOffset := uint64(0)
		for {
			lineImage, err := bufferedFile.ReadSlice(dr.Config.LineSeparator)
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			lineImage = bytes.TrimSuffix(lineImage, []byte{dr.Config.LineSeparator})
			lineImage = bytes.TrimSuffix(lineImage, x0D)

			lineImageLen := len(lineImage)

			var result *RowDataType
			select {
			case result = <-inPool:
			case ctx.Done():
				return ctx.Err()
			}

			if result.auxDataBuffer == nil || cap(result.auxDataBuffer)<lineImageLen {
				result.auxDataBuffer = make([]byte,0,lineImageLen)
			} else {
				result.auxDataBuffer = result.auxDataBuffer[0:0]
			}

			lineNumber++
			result.LineNumber = lineNumber
			result.auxDataBuffer = append(result.auxDataBuffer,lineImage...)
			result.RawData = bytes.Split(result.auxDataBuffer, []byte{dr.Config.FieldSeparator})
			result.LineOffset =  lineOffset
			result.Table =   table

			metadataColumnCount := len(table.Columns)
			lineColumnCount := len(result.RawData)

			if metadataColumnCount != lineColumnCount {
				err = fmt.Errorf("Number of column mismatch in line %v. Expected #%v; Actual #%v",
					lineNumber,
					metadataColumnCount,
					lineColumnCount,
				)
				return err
			}


			if lineNumber == 1 {
				tankContext, tankCancelFunc = context.WithCancel(context.Background())
				err = table.OpenTank(tankContext, dr.Config.TankPath, os.O_CREATE)
				if err != nil {
					return err
				}
				defer tankCancelFunc()
			}


			select {
			case rowDataChan <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
			lineOffset = lineOffset + uint64(writeToTank(result.RawData))

		}
		return nil
	}

	go func() {
		err := readFromDump()
		if err != nil {
			errChan <- err
		}
		close(rowDataChan)
		close(errChan)
	}()

	tracelog.Completedf(packageName, funcName, "for table %v", table)
	return rowDataChan, errChan
}
*/
