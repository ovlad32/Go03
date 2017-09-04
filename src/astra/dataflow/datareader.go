package dataflow

import (
	"context"
	"github.com/goinggo/tracelog"
	"sync"
	"time"
	"sparsebitset"
)

type DumpConfigType struct {
	AstraH2Host             string `json:"astra-h2-host"`
	AstraH2Port             string `json:"astra-h2-port"`
	AstraH2Login            string `json:"astra-h2-login"`
	AstraH2Password         string `json:"astra-h2-password"`
	AstraH2Database         string `json:"astra-h2-database"`
	AstraDumpPath           string `json:"astra-dump-path"`
	AstraDataGZip           bool   `json:"astra-data-gzip"`
	AstraColumnSeparator    byte   `json:"astra-column-byte-separator"`
	AstraLineSeparator      byte   `json:"astra-line-byte-separator"`
	BinaryDumpPath          string `json:"binary-dump-path"`
	KVStorePath             string `json:"kv-store-path"`
	AstraReaderBufferSize   int    `json:"astra-reader-buffer-size"`
	TableWorkers            int    `json:"table-workers"`
	CategoryWorkersPerTable int    `json:"category-worker-per-table"`
	CategoryDataChannelSize int    `json:"category-data-channel-size"`
	RawDataChannelSize      int    `json:"raw-data-channel-size"`
	HashValueLength         int
	EmitRawData             bool   `json:"emit-raw-data"`
	EmitHashValues          bool   `json:"emit-hash-data"`
	BuildBinaryDump         bool   `json:"build-binary-dump"`
	SpeedTickTimeSec        int    `json:"speed-tick-time-sec"`
	MemUsageTickTimeSec     int    `json:"memory-usage-tick-time-sec"`
	LogBaseFile             string `json:"log-base-file"`
	LogBaseFileKeepDay      int    `json:"log-base-file-keep-day"`
}

type DataReaderType struct {
	Config         *DumpConfigType
	blockStoreLock sync.RWMutex
//	blockStores    map[string]*DataCategoryStore
}

var nullBuffer []byte = []byte{0, 0, 0, 0, 0, 0, 0}

func (dr DataReaderType) ReadSource(runContext context.Context, table *TableInfoType) (
	outChans []chan *ColumnDataType,
	errChan chan error,
) {
	funcName := "DataReaderType.ReadSource"
	tracelog.Startedf(packageName, funcName, "for table %v", table)

	var lineOffset uint64 = 0

	outChans = make([]chan *ColumnDataType, len(table.Columns))
	for index := range table.Columns {
		outChans[index] = make(chan *ColumnDataType, dr.Config.RawDataChannelSize)
	}
	errChan = make(chan error, 1)


	readFromDump := func() (err error) {
		funcName := "DataReaderType.ReadSource.readFromDump"
		_ = funcName
		var started time.Time;
		var processed uint64
		processRowContent := func(
			ctx context.Context,
			lineNumber uint64,
			rowData [][]byte,
		) (err error) {

			if lineNumber == 1 {
				//table.NewDataDump(dr.Config.AstraDumpPath)
				//table.NewHashDump(dr.Config.AstraDumpPath)
				table.NewBoltDb(dr.Config.AstraDumpPath);
				started = time.Now()
			}
			for columnNumber, column := range table.Columns {

				columnData := column.NewColumnData(rowData[columnNumber])
				if columnData == nil {
					continue;
				}

				columnData.LineNumber = lineNumber
				columnData.LineOffset = lineOffset

				columnData.DefineDataCategory();
				columnData.HashData();
				if columnData.DataCategory.Bitset.BinarySize()>1024*1024*1024 {
					column.FlushBitset(columnData.DataCategory)
					columnData.DataCategory.Bitset = sparsebitset.New(0)
				}
			}
			processed ++;
			if time.Since(started).Minutes() >= 1  {
				tracelog.Info(packageName,funcName,"Processing speed %v lps",processed/60)
				processed = 0
				started = time.Now()
			}
			//fmt.Println(lineNumber)
			//table.CloseBoltDb();
			/*offset, err := rowDataS.WriteToBinaryDump(table.DataDump)
			if err != nil {
				//TODO:
				tracelog.Errorf(err, packageName, funcName, "")
				return err
			}
			lineOffset += offset*/

			return nil
		}

		linesRead, err := table.ReadAstraDump(
			runContext,
			processRowContent,
			&TableDumpConfig{
				Path:            dr.Config.AstraDumpPath,
				GZip:            dr.Config.AstraDataGZip,
				ColumnSeparator: dr.Config.AstraColumnSeparator,
				LineSeparator:   dr.Config.AstraLineSeparator,
				BufferSize:      dr.Config.AstraReaderBufferSize,
			},
		)
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Error in %v on %v", table, linesRead)
		} else {
			tracelog.Info(packageName, funcName, "Table %v, read %v", table, linesRead)
		}
		table.CloseBoltDb();
		//table.HashDump.Close()
		//table.DataDump.Close()

		/*gzFile, err := os.Open(dr.Config.AstraDumpPath + table.PathToFile.Value())
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "for table %v", table)
			return
		}
		defer gzFile.Close()

		bf := bufio.NewReaderSize(gzFile, dr.Config.AstraReaderBufferSize)
		file, err := gzip.NewReader(bf)
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "for table %v", table)
			return
		}
		defer file.Close()
		bufferedFile := bufio.NewReaderSize(file, dr.Config.AstraReaderBufferSize)

		lineNumber := uint64(0)
		lineOffset := uint64(0)
		for {
				line, err := bufferedFile.ReadSlice(dr.Config.AstraLineSeparator)
				if err == io.EOF {
					return nil
				} else if err != nil {
					return err
				}

				lineLength := len(line)

				if line[lineLength-1] == dr.Config.AstraLineSeparator {
					line = line[:lineLength-1]
				}
				if line[lineLength-2] == x0D[0]{
					line = line[:lineLength-2]
				}

				image := make([]byte,len(line))

				copy(image,line)



				metadataColumnCount := len(table.Columns)

				lineColumns := bytes.Split(image, []byte{dr.Config.AstraColumnSeparator})
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
				if lineNumber == 1 && dr.Config.BuildBinaryDump {
					closeContext, closeContextCancelFunc := context.WithCancel(context.Background())
					err = table.OpenBinaryDump(closeContext, dr.Config.BinaryDumpPath, os.O_CREATE)
					if err != nil {
						return err
					}
					defer closeContextCancelFunc()
				}

				if dr.Config.EmitRawData {
					wg.Add(len(lineColumns))
					for columnIndex := range  lineColumns{
						go splitToColumns(lineNumber, lineOffset, columnIndex, &lineColumns[columnIndex])
					}
					wg.Wait()
				}

				lineOffset = lineOffset + uint64(writeToTank(lineColumns))
		//	}
		}*/
		return nil
	}

	go func() {
		err := readFromDump()
		tracelog.Info(packageName, funcName, "4 %v", table)

		if err != nil {
			errChan <- err
		}
		for index := range outChans {
			if outChans[index] != nil {
				close(outChans[index])
			}
		}
		tracelog.Info(packageName, funcName, "Table %v done", table)
		close(errChan)
	}()
	tracelog.Info(packageName, funcName, "5 %v", table)

	tracelog.Completedf(packageName, funcName, "for table %v", table)
	return outChans, errChan
}
/*
type StoreType struct {
	db     *bolt.DB
	tx     *bolt.Tx
	bucket *bolt.Bucket
}

func (dr *DataReaderType) StoreByDataCategory(runContext context.Context, columnDataChans []chan *ColumnDataType, degree int) (
	errChan chan error,
) {
	funcName := "DataReaderType.StoreByDataCategory"
	tracelog.Started(packageName, funcName)
	errChan = make(chan error, 1)
	var speedTickerChan <-chan time.Time
	var memoryStatsTickerChan <-chan time.Time

	var tickerByteCounter int64 = 0

	if dr.Config.SpeedTickTimeSec > 0 {

		speedTickerChan = time.Tick(time.Duration(dr.Config.SpeedTickTimeSec) * time.Second)
		go func() {
			for {
				select {
				case <-runContext.Done():
					return
				case _, open := <-speedTickerChan:
					if !open {
						return
					}
					if tickerByteCounter != 0 {
						tracelog.Info(packageName, funcName, "Processing speed %.2f kb/sec", float64(tickerByteCounter)/float64(1024*dr.Config.SpeedTickTimeSec))
						atomic.AddInt64(&tickerByteCounter, -tickerByteCounter)
					}
				}
			}
		}()

	}

	if dr.Config.MemUsageTickTimeSec > 0 {
		memoryStatsTickerChan = time.Tick(time.Duration(dr.Config.MemUsageTickTimeSec) * time.Second)
		go func() {
			for {
				select {
				case <-runContext.Done():
				case _, open := <-memoryStatsTickerChan:
					if !open {
						return
					}
					vmem, _ := mem.VirtualMemory()
					fmt.Printf("Memory usage %%: %v\n", vmem.UsedPercent)
				}
			}
		}()
	}

	//	threads := uint64(0)

	var wg sync.WaitGroup

/*
	processColumnData := func(columnDataChan chan *ColumnDataType) {
	/
		//threadNo := atomic.AddUint64(&threads,1) +fmt.Sprintf("%v",*threadNo)
		funcName := "DataReaderType.StoreByDataCategory.goFunc1 "
	outer:
		for {
			select {
			case <-runContext.Done():
				break outer
			case columnData, open := <-columnDataChan:
				if !open {
					break outer
				}
				if columnData == nil || columnData.RawDataLength == 0 {
					continue
				}
				stringValue := string(columnData.RawData)
				var floatValue float64 = 0
				var parseError error
				floatValue, parseError = strconv.ParseFloat(strings.Trim(stringValue, " "), 64)

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
				columnData.dataCategoryKey = simple.Key()
				var store *DataCategoryStore

				if dr.Config.EmitHashValues {

					if dr.blockStores == nil {
						dr.blockStoreLock.Lock()
						if dr.blockStores == nil {
							dr.blockStores = make(map[string]*DataCategoryStore)
						}
						dr.blockStoreLock.Unlock()
					}
					dr.blockStoreLock.Lock()
					if value, found := dr.blockStores[columnData.dataCategoryKey]; !found {
						//tracelog.Info(packageName, funcName, "not found for %v", columnData.dataCategoryKey)
						store = &DataCategoryStore{}
						err := store.Open(columnData.dataCategoryKey, dr.Config.KVStorePath, dr.Config.RawDataChannelSize)
						if err != nil {
							errChan <- err
							break outer
						}

						dr.blockStores[columnData.dataCategoryKey] = store
						dr.blockStoreLock.Unlock()
						store.RunStore(runContext)
						//tracelog.Info(packageName, funcName, "Opened Channel for %s/%v", columnData.Column, columnData.dataCategoryKey)

					} else {
						dr.blockStoreLock.Unlock()
						store = value
					}

					select {
					case <-runContext.Done():
						break outer
					case store.columnDataChan <- columnData:
					}
				}

				if dr.Config.SpeedTickTimeSec > 0 {
					atomic.AddInt64(&tickerByteCounter, int64(columnData.RawDataLength))
				}

				dataCategory, err := columnData.Column.CategoryByKey(
					columnData.dataCategoryKey,
					func() (result *DataCategoryType, err error) {
						result = simple.covert()
						if simple.IsNumeric {
							if simple.FloatingPointScale == 0 {
								if !simple.IsNegative {
									if columnData.Column.NumericPositiveBitset == nil {
										columnData.Column.NumericPositiveBitset = sparsebitset.New(0)
										columnData.Column.numericPositiveBitsetChannel = make(chan uint64, dr.Config.CategoryDataChannelSize)
										columnData.Column.drainBitsetChannels.Add(1)
										go func() {
										outer:
											for {
												select {
												case <-runContext.Done():
													break outer
												case value, open := <-columnData.Column.numericPositiveBitsetChannel:
													if !open {
														break outer
													}
													//_=value
													columnData.Column.NumericPositiveBitset.Set(value)
												}
											}
											columnData.Column.drainBitsetChannels.Done()
										}()
									}
								} else {
									if columnData.Column.NumericNegativeBitset == nil {
										columnData.Column.NumericNegativeBitset = sparsebitset.New(0)
										columnData.Column.numericNegativeBitsetChannel = make(chan uint64, dr.Config.CategoryDataChannelSize)
										columnData.Column.drainBitsetChannels.Add(1)
										go func() {
										outer:
											for {
												select {
												case <-runContext.Done():
													break outer
												case value, open := <-columnData.Column.numericNegativeBitsetChannel:
													if !open {
														break outer
													}
													//_=value
													columnData.Column.NumericNegativeBitset.Set(value)
												}
											}
											columnData.Column.drainBitsetChannels.Done()
										}()
									}
								}
							}
						}
						err = result.RunAnalyzer(runContext, dr.Config.CategoryDataChannelSize)
						return
					},
				)

				if err != nil {
					errChan <- err
					break outer
				}

				select {
				case <-runContext.Done():
					break outer
				case dataCategory.stringAnalysisChan <- stringValue:
				}

				if simple.IsNumeric && !math.IsInf(floatValue, -1) && !math.IsInf(floatValue, 1) {
					select {
					case <-runContext.Done():
						break outer
					case dataCategory.numericAnalysisChan <- floatValue:
					}

					if simple.FloatingPointScale == 0 {
						if !simple.IsNegative && columnData.Column.NumericPositiveBitset != nil {
							select {
							case <-runContext.Done():
								break outer
							case columnData.Column.numericPositiveBitsetChannel <- uint64(floatValue):
							}
						}

						if simple.IsNegative && columnData.Column.NumericNegativeBitset != nil {
							select {
							case <-runContext.Done():
								break outer
							case columnData.Column.numericNegativeBitsetChannel <- uint64(-floatValue):
							}
						}
					}
				}

			}
		}

		wg.Done()
		tracelog.Completed(packageName, funcName)
		return
	}

	//degree = 1
	for index := range columnDataChans {
		wg.Add(1)
		go processColumnData(columnDataChans[index])
	}

	go func() {
		funcName := "DataReaderType.StoreByDataCategory.goFunc2"
		tracelog.Started(packageName, funcName)
		wg.Wait()
		close(errChan)
		tracelog.Completed(packageName, funcName)
	}()
	tracelog.Completed(packageName, funcName)
	return errChan
}

func (dr *DataReaderType) CloseStores() {
	if dr.blockStores != nil {
		for _, store := range dr.blockStores {
			if store.columnDataChan != nil {
				close(store.columnDataChan)
			}
		}
	}
}
*/
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
