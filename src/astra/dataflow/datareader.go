package dataflow

import (
	"bytes"
	"hash/fnv"
	"github.com/goinggo/tracelog"
	"io"
	"sync"
	"os"
	"compress/gzip"
	"bufio"
	"fmt"
	"encoding/binary"
	"astra/metadata"
	"context"
)

type DumpConfigType struct {
	BasePath    string
	TankPath    string
	GZipped         bool
	FieldSeparator  byte
	LineSeparator   byte
	InputBufferSize int
}

type DataReaderType struct {
	config *DumpConfigType
}


func (da DataReaderType) ReadSource(ctx context.Context, table *metadata.TableInfoType) (
		rowDataChan chan *RowDataType,
		errChan chan error,
	){
	funcName := "DataReaderType.fillColumnStorage"
	tracelog.Startedf(packageName, funcName, "for table %v", table)
	rowDataChan = make(chan []byte)
	errChan = make(chan error, 1)
	var wg sync.WaitGroup

	gzfile, err := os.Open(da.config.BasePath + table.PathToFile.Value())
	if err != nil {
		errChan <- err
		tracelog.Errorf(packageName, funcName, "for table %v", table)
		return
	}
	file, err := gzip.NewReader(gzfile)
	if err != nil {
		errChan <- err
		tracelog.Errorf(packageName, funcName, "for table %v", table)
		return
	}
	bufferedFile := bufio.NewReaderSize(file, da.config.InputBufferSize)

	wg.Add(1)
	go func() {
		wg.Wait()
		file.Close()
		gzfile.Close()
		close(rowDataChan);
		close(errChan);
	} ()

	go func () {
		var x0D = []byte{0x0D}

		lineNumber := uint64(0);
		for {
			lineImage, err := bufferedFile.ReadSlice(da.config.LineSeparator)
			if err == io.EOF {
				break
			} else if err != nil {
				errChan <- err
				break
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
				errChan <- fmt.Errorf("Number of column mismatch in line %v. Expected #%v; Actual #%v",
					lineNumber,
					metadataColumnCount,
					lineColumnCount,
				)
				break;
			}
			result := &RowDataType{
				Data:lineColumns,
				LineNumber:lineNumber,
				Table:table,
			}
			select {
				case rowDataChan <- result:
				case ctx.Done():
					break;
			}
		}
		wg.Done()
	} ()

	tracelog.Completedf(packageName, funcName, "for table %v", table)
	return rowDataChan, errChan
}



func  (da *DataReaderType) SplitToColumns( ctx context.Context, rowDataChan  chan *RowDataType) (
		columnDataChan chan *ColumnDataType,
		errChan chan error,
	) {
	var wg sync.WaitGroup;
	columnDataChan = make(chan *RowDataType)
	errChan = make(chan error, 1)

	wg.Add(1)
	go func() {
		wg.Wait()
		close(columnDataChan)
		close(errChan)
	}()

	go func() {
		for {
			select {
			case rt := <- rowDataChan:
				for columnNumber, columnDataBytes := range rt.Data {
					columnData := &ColumnDataType{
						LineNumber : rt.LineNumber,
						Column: rt.Table.Columns[columnNumber],
						Data:columnDataBytes,
					}
					select {
						case columnDataChan <- columnData:
						case <-ctx.Done():
							break;
					}
				}
			case <- ctx.Done():
			}
		}
		wg.Done();
	} ()

	return columnDataChan, errChan
}


func(da *DataReaderType) WriteTank( ctx context.Context, InRowDataChan  chan *RowDataType) (
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
							//TODO:HOW TO CLOSE THE FILE?
							if err != nil {
								select {
								case outErrChan <- err:
									break;
								}
							}
							rowData.Table.Tank = file;
						}
					}



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
						/*ticker++
						if ticker > 10000 {
							ticker = 0
							tracelog.Info(packageName,funcName,"10000 for column %v",iVal.column)
						}*/
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



func  (da *DataReaderType) GatherStatistics( rowDataChan  chan *RowDataType)

func (da *DataReaderType) fillColumnStorage(table *metadata.TableInfoType) {
	funcName := "DataReaderType.fillColumnStorage"
	var redump io.Writer

	var x0D = []byte{0x0D}

	//	lineSeparatorArray[0] = da.DumpConfiguration.LineSeparator
	var statsChannels /*,storeChans */ []ColumnDataChannelType
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
						/*ticker++
						if ticker > 10000 {
							ticker = 0
							tracelog.Info(packageName,funcName,"10000 for column %v",iVal.column)
						}*/
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


			/*if table.Columns[columnIndex].ColumnName.String() == "CONTRACT_NUMBER" {
				if len(lineColumns[columnIndex]) != 19 {
					fmt.Printf("%s, %s\n",string(lineColumns[columnIndex]),string(line));
				}
			}*/
			//<-out

			/*out <-columnDataType{
				column:     source.Columns[columnIndex],
				bValue:     lineColumns[columnIndex],
				lineNumber: lineNumber,
			}*/

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
		/*
		err = val.column.OpenRowsBucket()
		if err != nil {
			panic(err)
		}*/
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

	/*newHashValue, err := val.dataCategory.OpenHashBucket(hValue[:])
	if err != nil {
		panic(err)
	}
	if newHashValue {
		(*val.dataCategory.HashUniqueCount.Reference())++
	}

	err = val.dataCategory.OpenHashValuesBucket()
	if err != nil {
		panic(err)
	}*/

	err = val.dataCategory.OpenBitsetBucket()
	if err != nil {
		panic(err)
	}

	/*err = val.dataCategory.OpenHashSourceBucket()
	if err != nil {
		panic(err)
	}
	err = val.dataCategory.OpenHashStatsBucket()
	if err != nil {
		panic(err)
	}
	if val.dataCategory.HashValuesBucket == nil {
		panic("HashValues bucket has not been created!")
	}
	if val.dataCategory.BitsetBucket == nil {
		panic("Bitset bucket has not been created!")
	}
	if val.dataCategory.HashSourceBucket == nil {
		panic("HashSource bucket has not been created!")
	}*/

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