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
	"strings"
	"strconv"
)

type DumpConfigType struct {
	BasePath        string
	TankPath        string
	GZipped         bool
	FieldSeparator  byte
	LineSeparator   byte
	InputBufferSize int
}

type DataReaderType struct {
	Config *DumpConfigType
}

func (dr DataReaderType) ReadSource(ctx context.Context, table *TableInfoType) (
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
			colDataLength := len(rowData)
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

		gzfile, err := os.Open(dr.Config.BasePath + table.PathToFile.Value())
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "for table %v", table)
			return
		}
		defer gzfile.Close()
		file, err := gzip.NewReader(gzfile)
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
			lineImageLen := len(lineImage)
			line := make([]byte, lineImageLen, lineImageLen)
			copy(line, lineImage)

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

			result := &RowDataType{
				RawData:    lineColumns,
				LineNumber: lineNumber,
				LineOffset: lineOffset,
				Table:      table,
			}
			lineOffset = lineOffset + uint64(writeToTank(result.RawData))

			select {
			case rowDataChan <- result:
			case <-ctx.Done():
				return ctx.Err()
			}
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

func (dr *DataReaderType) SplitToColumns(ctx context.Context, rowDataChan chan *RowDataType, degree int) (
	outChan chan *ColumnDataType,
	errChan chan error,
) {
	outChan = make(chan *ColumnDataType)
	errChan = make(chan error, 1)
	var wg sync.WaitGroup
	processRows := func() {
		outer:
		for {
			select {
			case rt,opened := <-rowDataChan:
			if !opened {
				break outer
			}
			for columnNumber, columnDataBytes := range rt.RawData {

				columnData := &ColumnDataType{
					LineNumber: rt.LineNumber,
					LineOffset: rt.LineOffset,
					Column:     &ColumnInfoType{
									ColumnInfoType :rt.Table.Columns[columnNumber],
								},
					RawData:   columnDataBytes,
				}
				select {
				case outChan <- columnData:
				case <-ctx.Done():
					break outer
				}
			}

			case <-ctx.Done():
				break outer
			}
		}
		wg.Done()
		return
	}

	go func() {
		for index := 0;index < degree; index ++ {
			wg.Add(1)
			go processRows()
		}
		wg.Wait()
		close(outChan)
		close(errChan)
	}()

	return outChan, errChan
}
func (dr DataReaderType) Category(ctx context.Context, colDataChan chan *ColumnDataType) (
	outChan *ColumnDataType,
	errChan chan error,
){
	errChan = make(chan error, 1)
	outChan = make(chan *ColumnDataType)
	var wg sync.WaitGroup


	f := func () {
		outer:
		for {
			select {
			case columnData, opened := <- colDataChan:
				if !opened {
					break outer
				}
				if columnData == nil {
					continue
				}




			case <-ctx.Done():
				break outer
			}


		}
	}





	found := val.column.FindDataCategory(
		uint16(byteLength),
		isNumeric,
		isNegative,
		int8(fpScale),
		isSubHash,
		bSubHash,
	)
	if found == nil {
		found = &ColumnDataCategoryStatsType{
			Column:             column,
			ByteLength:         jsnull.NewNullInt64(int64(byteLength)),
			IsNumeric:          jsnull.NewNullBool(isNumeric),
			FloatingPointScale: jsnull.NewNullInt64(int64(fpScale)),
			IsNegative:         jsnull.NewNullBool(isNegative),
			NonNullCount:       jsnull.NewNullInt64(int64(0)),
			HashUniqueCount:    jsnull.NewNullInt64(int64(0)),
			IsSubHash:          jsnull.NewNullBool(isSubHash),
			SubHash:            jsnull.NewNullInt64(int64(bSubHash)),
		}
		//	tracelog.Info(packageName,funcName,"dataCategory %v for column %v[%v] created",found,column,column.Id)
		if column.DataCategories == nil {
			column.DataCategories = make([]*ColumnDataCategoryStatsType, 0, 2)
		}
		column.DataCategories = append(column.DataCategories, found)
	} else {
		//	tracelog.Info(packageName,funcName,"dataCategory %v for column %v[%v] found",found,column,column.Id)
	}
	val.dataCategory = found
	(*found.NonNullCount.Reference())++

	if found.MaxStringValue.Value() < sValue || !found.MaxStringValue.Valid() {
		found.MaxStringValue = jsnull.NewNullString(sValue)
	}

	if found.MinStringValue.Value() > sValue || !found.MinStringValue.Valid() {
		found.MinStringValue = jsnull.NewNullString(sValue)
	}

	if found.IsNumeric.Value() {
		if !found.MaxNumericValue.Valid() {
			found.MaxNumericValue = jsnull.NewNullFloat64(nValue)

		} else if found.MaxNumericValue.Value() < nValue {
			(*found.MaxNumericValue.Reference()) = nValue
		}
		if !column.MinNumericValue.Valid() {
			found.MinNumericValue = jsnull.NewNullFloat64(nValue)
		} else if column.MinNumericValue.Value() > nValue {
			(*found.MinNumericValue.Reference()) = nValue
		}
	}
	//	tracelog.Info(packageName,funcName,"Statistics for line %v of column %v[%v] collected",val.lineNumber,column,column.Id)
	//	tracelog.Completed(packageName,funcName)
	/*
		if column.ColumnName.String() == "CONTRACT_NUMBER" {
			if len(val.bValue) != 19 {
				fmt.Printf("2 %s, %v\n",string(val.bValue),val.lineNumber);
			}
		} */

	return
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
