package metadata

import (
	jsnull "./../jsnull"
	sparsebitset "./../sparsebitset"
	utils "./../utils"
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
//	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"hash/fnv"
	"io"
	"os"
	"strings"
	"sync"
	"github.com/cayleygraph/cayley"
	"sort"
)

const hashLength = 8
const wordSize = uint64(64)

type B9Type [9]byte

//var HashStorage *bolt.DB

var deBruijn = [...]byte{
	0, 1, 56, 2, 57, 49, 28, 3, 61, 58, 42, 50, 38, 29, 17, 4,
	62, 47, 59, 36, 45, 43, 51, 22, 53, 39, 33, 30, 24, 18, 12, 5,
	63, 55, 48, 27, 60, 41, 37, 16, 46, 35, 44, 21, 52, 32, 23, 11,
	54, 26, 40, 15, 34, 20, 31, 10, 25, 14, 19, 9, 13, 8, 7, 6,
}
var (
	// ErrInvalidIndex is answered when an invalid index is given.
	ErrInvalidIndex = errors.New("invalid index given")

	// ErrItemNotFound is answered when a requested item could not be
	// found.
	ErrItemNotFound = errors.New("requested item not found")

	// ErrNilArgument is answered when an unexpected `nil` is
	// encountered as an argument.
	ErrNilArgument = errors.New("nil input given")
)

func trailingZeroes64(v uint64) uint64 {
	return uint64(deBruijn[((v&-v)*0x03f79d71b4ca8b09)>>58])
}

type DataAccessType struct {
	DumpConfiguration          DumpConfigurationType
	ColumnBucketsCache         *utils.Cache
	TransactionCountLimit      uint64
	SubHashByteLengthThreshold int
	Repo *cayley.Handle
}

type ColumnDataType struct {
	column       *ColumnInfoType
	dataCategory *ColumnDataCategoryStatsType
	lineNumber   uint64
	bValue       []byte
}
type ColumnDataChannelType chan *ColumnDataType

/*
func (c columnDataType) buildDataCategory() (result []byte, bLen uint16) {
	result = make([]byte, 3, 5)
	if c.isNumeric {
		result[0] = (1 << 2)
		if c.fpScale != -1 {
			if c.isNegative {
				result[0] = result[0] | (1 << 0)
			}
		} else {
			result[0] = result[0] | (1 << 1)
			if c.isNegative {
				result[0] = result[0] | (1 << 0)
			}
		}
	}
	bLen = uint16(len(c.bValue))
	binary.LittleEndian.PutUint16(result[1:], bLen)
	if c.isNumeric {
		if c.fpScale != -1 {
			result = append(
				result,
				byte(c.fpScale),
			)
		}
	}
	if c.isSubHash {
		result = append(
			result,
			byte(c.bSubHash),
		)
	}
	return
}
*/

func ReportHashStorageContents() {
	/*categoryName := func(k []byte) (result string){
		kLen := len(k)
		//fmt.Println(k)
		if kLen<3 {
			result = fmt.Sprintf("Can not explan category data %v",k)
		} else {
			bLen := binary.LittleEndian.Uint16(k[1:])
			if k[0] == 0 {
				result = fmt.Sprintf("char[%v]",bLen)
				if kLen>3 {
					result = fmt.Sprintf("%v(%v)",result,k[3])
				}
			}  else {
				if (k[0]>>0) & 0x01 > 0 {
					result = "-"
				} else {
					result = "+"
				}
				if (k[0]>>1) & 0x01 == 0 {
					result = fmt.Sprintf("%vF[%v,%v]",result,bLen,k[3])
					if kLen>4 {
						result = fmt.Sprintf("%v(%v)",result,k[4])
					}
				} else {
					result = fmt.Sprintf("%vI[%v]",result,bLen)
					if kLen>3 {
						result = fmt.Sprintf("%v(%v)",result,k[3])
					}
				}
			}
		}
		return
	}


	reportCategories := func (b *bolt.Bucket,totalHashes,totalLines *uint64) {
		b.ForEach(func(k,v []byte) error {
			categoryBucket := b.Bucket(k)
			hashes,_ := utils.B8ToUInt64(categoryBucket.Bucket(statsBucketBytes).Get(hashStatsUnqiueCountBucketBytes))
			var bsWords, lines, hBuckets uint64
			categoryBucket.Bucket(bitsetBucketBytes).ForEach(
				func(ik,iv []byte) error {
					bsWords ++
					return nil
				},
			)
			hashValuesBucket := categoryBucket.Bucket(hashValuesBucketBytes)
			hashValuesBucket.ForEach(
				func(ik,iv []byte) error {
					hBuckets++
					hashValuesBucket.Bucket(ik).ForEach(
						func (iik,_ []byte) error {
							lines ++
							return nil
						})
					return nil
				},
			)

			//- "bitset" (+b)
			//-offset/bit(k/v)
			//- "hashValues" (+b)
			//- hash/value (b)
			fmt.Println(
				fmt.Sprintf("%v: hashes/values %v,  bitset words %v, rows by hashes %v, hashBuckets %v",
					categoryName(k),
					hashes,
					bsWords,
					lines,
					hBuckets ),

			)
			(*totalHashes)+= hashes
			(*totalLines) += lines
			return nil
		})
	}

	reportColumns := func (b *bolt.Bucket) {
		b.ForEach(func(k,v []byte) error {
			//t.Println(k)
			id,_ := utils.B8ToInt64(k)
			col,_ := H2.ColumnInfoById(jsnull.NewNullInt64(id))
			fmt.Println(col)
			var totalHashes,totalLines uint64
			reportCategories(b.Bucket(k),&totalHashes,&totalLines)
			fmt.Printf("Total: hashes/values %v, lines %v\n",totalHashes,totalLines)
			return nil
		})
	}

	reportTables := func (b *bolt.Bucket) {
		b.ForEach(func(k,v []byte) error {
			id,_ := utils.B8ToInt64(k)
			tbl,_ := H2.TableInfoById(jsnull.NewNullInt64(id))
			fmt.Println(id,tbl)
			fmt.Println("-----------------------------------")
			tableBucket := b.Bucket(k)

			tableBucket.ForEach(func(ik,_ []byte) error {
				if bytes.Compare(ik,columnsLabelBucketBytes) == 0 {
					columns := tableBucket.Bucket(ik)
					if columns!= nil {
						reportColumns(columns)
					}
				}
				return nil
			} )




			fmt.Println("-----------------------------------")
			return nil
		})

	}
	tx,err := HashStorage.Begin(false)
	if err != nil {
		panic(err)
	}
	tx.ForEach(func(k []byte, b *bolt.Bucket)(error){
		if bytes.Compare(k,tablesLabelBucketBytes)==0 {
			reportTables(b)
		}
		return nil
	})
	tx.Rollback();*/

}

/*
func(c ColumnInfoType) columnBucketName() (result ColumnBucketNameType) {
	if !c.Id.Valid {
		panic(fmt.Sprintf("Column Id has not been initialized for table %v",c.TableInfo))
	}
	binary.PutUvarint(result[:],uint64(c.Id.Int64))
	return
}*/

func (da *DataAccessType) cleanupColumnStorage(table *TableInfoType) {
	funcName := "DataAccessType.cleanupColumnStorage"
	tracelog.Startedf(packageName, funcName, "for table %v", table)
	for _, column := range table.Columns {

		err := column.OpenStorage(true)
		if err != nil {
			panic(err)
		}

		err = column.CleanStorage()
		if err != nil {
			panic(err)
		}

		err = column.CloseStorageTransaction(true)
		if err != nil {
			panic(err)
		}
		err = column.CloseStorage()
		if err != nil {
			panic(err)
		}
	}
	tracelog.Completedf(packageName, funcName, "for table %v", table)
}

func (da *DataAccessType) updateColumnStats(table *TableInfoType) {
	funcName := "DataAccessType.updateColumnStats"
	tracelog.Startedf(packageName, funcName, "for table %v", table)

	for _, column := range table.Columns {
		err := column.OpenStorage(true)
		if err != nil {
			panic(err)
		}
		err = column.OpenCategoriesBucket()
		if err != nil {
			panic(err)
		}
		var nonNullCount, hashUniqueCount int64
		for _, dc := range column.DataCategories {
			nonNullCount = nonNullCount + dc.NonNullCount.Value()
			hashUniqueCount = hashUniqueCount + dc.HashUniqueCount.Value()

			_, err = dc.OpenBucket(nil)
			if err != nil {
				panic(err)
			}

			err = dc.OpenStatsBucket()
			if err != nil {
				panic(err)
			}
			err = dc.OpenHashValuesBucket()
			if err != nil {
				panic(err)
			}
			//fmt.Println(column)
			/*calcRowCount := func(sourceBucket, statsBucket *bolt.Bucket) {
				cnt := uint64(0)
				sourceBucket.ForEach(func(k, v []byte) error {
					//	vi,_ := utils.B8ToUInt64(k)
					//	fmt.Println("K:", vi,k)
					cnt++
					return nil
				})
				//fmt.Println("KeyN:", cnt)
				statsBucket.Put(columnInfoCategoryStatsRowCountKey, utils.UInt64ToB8(cnt)[:])
			}
			var goBusy sync.WaitGroup
			buckets := make(chan [2]*bolt.Bucket, 10)

			for index := 0; index < 5; index++ {
				goBusy.Add(1)
				go func(chin chan [2]*bolt.Bucket) {
					for b := range chin {
						calcRowCount(b[0], b[1])
					}
					goBusy.Done()
				}(buckets)
			}
			dc.HashValuesBucket.ForEach(func(hashCode, _ []byte) error {
				_, err = dc.OpenHashBucket(hashCode)
				//fmt.Println(hashCode)
				err = dc.OpenHashSourceBucket()
				err = dc.OpenHashStatsBucket()
				//calcRowCount(dc.HashSourceBucket, dc.HashStatsBucket)
				var out [2]*bolt.Bucket;
				out[0] = dc.HashSourceBucket
				out[1] = dc.HashStatsBucket
				buckets <-out
				return nil
			})
			close(buckets)
			goBusy.Wait()*/
			dc.StatsBucket.Put(columnInfoStatsNonNullCountKey, utils.Int64ToB8(dc.NonNullCount.Value())[:])
			dc.StatsBucket.Put(columnInfoStatsHashUniqueCountKey, utils.Int64ToB8(dc.HashUniqueCount.Value())[:])
		}

		//fmt.Println("col:",hashUniqueCount)
		err = column.OpenStatsBucket()
		if err != nil {
			panic(err)
		}
		column.statsBucket.Put(columnInfoStatsCategoryCountKey, utils.Int64ToB8(int64(len(column.DataCategories)))[:])
		column.statsBucket.Put(columnInfoStatsNonNullCountKey, utils.Int64ToB8(nonNullCount)[:])
		column.statsBucket.Put(columnInfoStatsHashUniqueCountKey, utils.Int64ToB8(hashUniqueCount)[:])

		column.CloseStorageTransaction(true)
		column.CloseStorage()

		column.NonNullCount = jsnull.NewNullInt64(nonNullCount)
		column.HashUniqueCount = jsnull.NewNullInt64(hashUniqueCount)
	}
	tracelog.Completedf(packageName, funcName, "for table %v", table)
}

func (da *DataAccessType) fillColumnStorage(table *TableInfoType) {
	funcName := "DataAccessType.fillColumnStorage"

	var x0D = []byte{0x0D}

	//	lineSeparatorArray[0] = da.DumpConfiguration.LineSeparator
	var statsChannels /*,storeChans */ []ColumnDataChannelType
	var goBusy sync.WaitGroup
	tracelog.Startedf(packageName, funcName, "for table %v", table)

	gzfile, err := os.Open(da.DumpConfiguration.DumpBasePath + table.PathToFile.Value())
	if err != nil {
		panic(err)
	}
	defer gzfile.Close()

	file, err := gzip.NewReader(gzfile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	rawData := bufio.NewReaderSize(file, da.DumpConfiguration.InputBufferSize)
	//sLineSeparator := string(da.DumpConfiguration.LineSeparator)
	//sFieldSeparator := string(da.DumpConfiguration.FieldSeparator)

	lineNumber := uint64(0)

	for {
		lineImage, err := rawData.ReadSlice(da.DumpConfiguration.LineSeparator)

		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		lineImage = bytes.TrimSuffix(lineImage, []byte{da.DumpConfiguration.LineSeparator})
		lineImage = bytes.TrimSuffix(lineImage, x0D)
		lineImageLen := len(lineImage)

		line := make([]byte, lineImageLen, lineImageLen)
		copy(line, lineImage)
		//line := lineImage
		lineNumber++

		metadataColumnCount := len(table.Columns)

		lineColumns := bytes.Split(line, []byte{da.DumpConfiguration.FieldSeparator})
		lineColumnCount := len(lineColumns)
		if metadataColumnCount != lineColumnCount {
			panic(fmt.Sprintf("Number of column mismatch in line %v. Expected #%v; Actual #%v",
				lineNumber,
				metadataColumnCount,
				lineColumnCount,
			))
		}

		for columnIndex := range table.Columns {
			if lineNumber == 1 {
				if columnIndex == 0 {
					statsChannels = make([]ColumnDataChannelType, 0, metadataColumnCount)
					//storeChans = make([]ColumnDataChannelType, 0,metadataColumnCount);
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
			}
			//<-out

			/*out <-columnDataType{
				column:     source.Columns[columnIndex],
				bValue:     lineColumns[columnIndex],
				lineNumber: lineNumber,
			}*/
		}
	}

	if statsChannels != nil {
		for index := range statsChannels {
			close(statsChannels[index])
		}
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

func (da *DataAccessType) collectDataStats(val *ColumnDataType) {
	//	funcName := "DataAccessType.CollectDataStats"
	//	tracelog.Started(packageName,funcName)
	column := val.column
	//	tracelog.Info(packageName,funcName,"Collecting statistics for line %v of column %v[%v]...",val.lineNumber,column,column.Id)
	byteLength := len(val.bValue)
	if byteLength == 0 {
		return
	}

	sValue := string(val.bValue)
	var err error
	var nValue float64
	var fpScale int
	var isNegative bool

	isSubHash := byteLength > da.SubHashByteLengthThreshold
	//nValue, err = strconv.ParseFloat(sValue, 64)
	nValue = float64(0)
	isNumeric := err == nil

	if isNumeric {
		var lengthChanged bool
		if strings.Index(sValue, ".") != -1 {
			trimmedValue := strings.TrimRight(sValue, "0")
			lengthChanged = len(sValue) != len(trimmedValue)
			if lengthChanged {
				sValue = trimmedValue
			}
			fpScale = len(sValue) - (strings.Index(sValue, ".") + 1)
			isSubHash = false
		} else {
			fpScale = -1
		}

		isNegative = strings.HasPrefix(sValue, "-")
		(*column.NumericCount.Reference())++
		if !column.MaxNumericValue.Valid() {
			column.MaxNumericValue = jsnull.NewNullFloat64(nValue)
		} else if column.MaxNumericValue.Value() < nValue {
			(*column.MaxNumericValue.Reference()) = nValue
		}

		if !column.MinNumericValue.Valid() {
			column.MinNumericValue = jsnull.NewNullFloat64(nValue)
		} else if column.MinNumericValue.Value() > nValue {
			(*column.MinNumericValue.Reference()) = nValue
		}
		if fpScale != -1 && lengthChanged {
			sValue = strings.TrimRight(fmt.Sprintf("%f", nValue), "0")
			val.bValue = []byte(sValue)
			byteLength = len(val.bValue)
		}
	}
	bSubHash := uint8(0)
	if isSubHash {

		for _, bChar := range val.bValue {
			if bChar > 0 {
				bSubHash = ((uint8(37) * bSubHash) + uint8(bChar)) & 0xff
			}
		}
	}

	if !column.MaxStringValue.Valid() || column.MaxStringValue.Value() < sValue {
		column.MaxStringValue = jsnull.NewNullString(sValue)
	}
	if !column.MinStringValue.Valid() || column.MinStringValue.Value() > sValue {
		column.MinStringValue = jsnull.NewNullString(sValue)
	}

	lValue := int64(len(sValue))
	if !column.MaxStringLength.Valid() {
		column.MaxStringLength = jsnull.NewNullInt64(lValue)
	} else if column.MaxStringLength.Value() < lValue {
		(*column.MaxStringLength.Reference()) = lValue

	}
	if !column.MinStringLength.Valid() {
		column.MinStringLength = jsnull.NewNullInt64(lValue)
	} else if column.MinStringLength.Value() > lValue {
		(*column.MinStringLength.Reference()) = lValue

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
}

func (da *DataAccessType) storeData(val *ColumnDataType) {
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
			hValue[index] = val.bValue[bLen - index - 1]
		}
		hashUIntValue, _ = utils.B8ToUInt64(hValue)
		//fmt.Println("1",val.column,hValue,category)
	}
	//val.column.DataCategories[category] = true
	var err error
	if val.column.categoriesBucket == nil {
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

	newHashValue, err := val.dataCategory.OpenHashBucket(hValue[:])
	if err != nil {
		panic(err)
	}
	if newHashValue {
		(*val.dataCategory.HashUniqueCount.Reference())++
	}

	err = val.dataCategory.OpenHashValuesBucket()
	if err != nil {
		panic(err)
	}

	err = val.dataCategory.OpenBitsetBucket()
	if err != nil {
		panic(err)
	}

	err = val.dataCategory.OpenHashSourceBucket()
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
	}

	baseUIntValue, offsetUIntValue := sparsebitset.OffsetBits(hashUIntValue)
	baseB8Value := utils.UInt64ToB8(baseUIntValue)
	//offsetB8Value := utils.UInt64ToB8(offsetUIntValue)
	bits, _ := utils.B8ToUInt64(val.dataCategory.BitsetBucket.Get(baseB8Value))
	bits = bits | (1 << offsetUIntValue)
	val.dataCategory.BitsetBucket.Put(baseB8Value, utils.UInt64ToB8(bits))
	//fmt.Println(val.column,string(val.bValue),hValue[:],val.lineNumber,utils.UInt64ToB8(val.lineNumber))

	val.dataCategory.HashSourceBucket.Put(
		utils.UInt64ToB8(val.lineNumber),
		//TODO: switch to real file offset to column value instead of lineNumber
		utils.UInt64ToB8(val.lineNumber),
	)


	if hashRowCount,found := utils.B8ToUInt64(val.dataCategory.HashStatsBucket.Get(columnInfoCategoryStatsRowCountKey)); !found {
		val.dataCategory.HashStatsBucket.Put(
			columnInfoCategoryStatsRowCountKey,
			utils.UInt64ToB8(uint64(1)),
		)
	} else {
		val.dataCategory.HashStatsBucket.Put(
			columnInfoCategoryStatsRowCountKey,
			utils.UInt64ToB8(hashRowCount+1),
		)
	}


}




func (da DataAccessType) LoadStorage() {

	var goBusy sync.WaitGroup

	mtd1, err := H2.MetadataById(jsnull.NewNullInt64(10))
	if err != nil {
		panic(err)
	}

	mtd2, err := H2.MetadataById(jsnull.NewNullInt64(11))
	if err != nil {
		panic(err)
	}

	tables, err := H2.TableInfoByMetadata(mtd1)
	if err != nil {
		panic(err)
	}
	tables2, err := H2.TableInfoByMetadata(mtd2)
	if err != nil {
		panic(err)
	}
	//cayley.StartPath(da.Repo,quad.
	tables = append(tables, tables2...)

	{
		tableСhannel := make(TableInfoTypeChannel, 10)
		goProcess := func(chin TableInfoTypeChannel) {
			for ti := range chin {
				da.cleanupColumnStorage(ti)
			}
			goBusy.Done()
		}

		for i := 0; i < 3; i++ {
			goBusy.Add(1)
			go goProcess(tableСhannel)
		}

		for _, tableInfo := range tables {
			//if tableInfo.Id.Value() == int64(268) {
			tableСhannel <- tableInfo
			//}
		}
		close(tableСhannel)
		goBusy.Wait()

	}

	{
		tableСhannel := make(TableInfoTypeChannel, 10)
		goProcess := func(chin TableInfoTypeChannel) {
			for ti := range chin {
				da.fillColumnStorage(ti)
			}
			goBusy.Done()
		}

		err := H2.CreateDataCategoryTable()
		if err != nil {
			panic(err)
		}

		for i := 0; i < 3; i++ {
			goBusy.Add(1)
			go goProcess(tableСhannel)
		}

		for _, tableInfo := range tables {
			//if tableInfo.Id.Value() == int64(268) {
			tableСhannel <- tableInfo
			//}
		}
		close(tableСhannel)
		goBusy.Wait()

		for _, t := range tables {
			for _, c := range t.Columns {
				err = H2.SaveColumnCategory(c)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	{
		tableСhannel := make(TableInfoTypeChannel, 10)
		goProcess := func(chin TableInfoTypeChannel) {
			for ti := range chin {
				da.updateColumnStats(ti)
			}
			goBusy.Done()
		}

		for i := 0; i < 3; i++ {
			goBusy.Add(1)
			go goProcess(tableСhannel)
		}

		for _, tableInfo := range tables {
			//if tableInfo.Id.Value() == int64(268) {
			tableСhannel <- tableInfo
			//}
		}

		close(tableСhannel)
		goBusy.Wait()

	}
}

func (da DataAccessType) MakeColumnPairs(metadata1, metadata2 *MetadataType, stage string) {
//	var emptyValue []byte = make([]byte, 0, 0)
	var pairs []*ColumnPairType = make([]*ColumnPairType,0,100)

	processPair := func(column1, column2 *ColumnInfoType) {
		var pair *ColumnPairType
		var err error
		//column1.bucketLock.Lock()
		column1.categoriesBucket.ForEach(
			func(dataCategory, v []byte) error {
				if column2.categoriesBucket == nil {
					return nil
				}

				//column2.bucketLock.Lock()
				if column2.categoriesBucket.Bucket(dataCategory) != nil {

					dataCategoryCopy := make([]byte, len(dataCategory))
					//fmt.Println("%v",dataCategory)
					copy(dataCategoryCopy, dataCategory)

					dc1 := ColumnDataCategoryStatsType{Column: column1,}
					dc2 := ColumnDataCategoryStatsType{Column: column2}

					dc1.OpenBucket(dataCategoryCopy)
					dc1.OpenBitsetBucket()
					dc1.OpenStatsBucket()
					dc1.OpenHashValuesBucket()

					dc2.OpenBucket(dataCategoryCopy)
					dc2.OpenBitsetBucket()
					dc2.OpenStatsBucket()
					dc2.OpenHashValuesBucket()

					if dc1.BitsetBucket == nil && dc2.BitsetBucket == nil {
						//column2.bucketLock.Unlock()
						return nil
					}
					if pair == nil {
						pair, err = NewColumnPair(column1, column2, dataCategoryCopy)
						if err != nil {
							panic(err)
						}
						err = pair.OpenStorage(true)
						if err != nil {
							panic(err)
						}
						err = pair.OpenCategoriesBucket()
						if err != nil {
							panic(err)
						}
						err = pair.OpenStatsBucket()
						if err != nil {
							panic(err)
						}
					}
					pair.CategoryBucket = nil
					var uqCnt1, uqCnt2 uint64
					if dc1.StatsBucket != nil {
						uqCnt1, _ = utils.B8ToUInt64(dc1.StatsBucket.Get(columnInfoStatsHashUniqueCountKey))
						//fmt.Println(uqCnt1)
					}
					if dc2.StatsBucket != nil {
						uqCnt2, _ = utils.B8ToUInt64(dc2.StatsBucket.Get(columnInfoStatsHashUniqueCountKey))
						//fmt.Println(uqCnt2)
					}
					if uqCnt1 > uqCnt2 {
						dc2, dc1 = dc1, dc2
					}
					var dataCategoryIntersectionCount uint64
					dc1.BitsetBucket.ForEach(
						func(base, offset1 []byte) error {
							offset2 := dc2.BitsetBucket.Get(base)
							if offset2 == nil {
								return nil
							}
							baseUInt, _ := utils.B8ToUInt64(base)

							var offsetIntersection utils.B8Type
							for index := range offset1 {
								offsetIntersection[index] = offset1[index] & offset2[index]
							}
							intersection, _ := utils.B8ToUInt64(offsetIntersection[:])

							prod := baseUInt * wordSize
							rsh := uint64(0)
							prev := uint64(0)
							for {
								w := intersection >> rsh
								if w == 0 {
									break
								}
								result := rsh + trailingZeroes64(w) + prod
								if result != prev {
									if pair.CategoryBucket == nil {
										err = pair.OpenCategoryBucket(dataCategoryCopy)
										if err != nil {
											panic(err)
										}

										err = pair.OpenCategoryHashBucket()
										if err != nil {
											panic(err)
										}
										(*pair.CategoryIntersectionCount.Reference())++
									}

									(*pair.HashIntersectionCount.Reference())++
									hashB8 := utils.UInt64ToB8(result)
									hashBucket,err := pair.CategoryHashBucket.CreateBucketIfNotExists(hashB8)
									if err != nil {
										panic(err)
									}
									dc1.OpenHashBucket(hashB8)
									dc1.OpenHashStatsBucket()
									dc2.OpenHashBucket(hashB8)
									dc2.OpenHashStatsBucket()

									column1RowCountBytes := dc1.HashStatsBucket.Get(columnInfoCategoryStatsRowCountKey)
									column2RowCountBytes := dc2.HashStatsBucket.Get(columnInfoCategoryStatsRowCountKey)

									if dc1.Column.Id.Value() > dc2.Column.Id.Value() {
										column1RowCountBytes, column2RowCountBytes = column2RowCountBytes, column1RowCountBytes
									}
									hashBucket.Put([]byte("column1RowCount"), column1RowCountBytes)
									hashBucket.Put([]byte("column2RowCount"), column2RowCountBytes)

									column1RowCount,_ := utils.B8ToUInt64(column1RowCountBytes)
									column2RowCount,_ := utils.B8ToUInt64(column2RowCountBytes)
									(*pair.column1RowCount.Reference()) += int64(column1RowCount)
									(*pair.column2RowCount.Reference()) += int64(column2RowCount)


									prev = result

								}
								rsh++
							}

							return nil
						},
					)
					if pair.CategoryBucket != nil {
						err = pair.OpenCategoryStatsBucket()
						if err != nil {
							panic(err)
						}

						err = pair.CategoryStatsBucket.Put(columnPairStatsHashUniqueCountKey, utils.UInt64ToB8(dataCategoryIntersectionCount)[:])
						if err != nil {
							panic(err)
						}
					}

				}
				//column2.bucketLock.Unlock()
				return nil
			},
		)
		//column1.bucketLock.Unlock()
		if pair != nil && pair.HashIntersectionCount.Value() > 0 {
			err = pair.OpenStatsBucket()
			if err != nil {
				panic(err)
			}
			err = pair.StatsBucket.Put(columnPairHashUniqueCountKey, utils.Int64ToB8(pair.HashIntersectionCount.Value()))
			if err != nil {
				panic(err)
			}

			err = pair.StatsBucket.Put(columnPairHashCategoryCountKey, utils.Int64ToB8(pair.CategoryIntersectionCount.Value()))
			if err != nil {
				panic(err)
			}

			pair.CloseStorageTransaction(true)
			pair.CloseStorage()
			pairs = append(pairs,pair)
			fmt.Printf("%v <-(%v) %v (%v)-> %v\n", pair.column1, pair.column1RowCount.Value(), pair.HashIntersectionCount.Value(), pair.column2RowCount.Value(), pair.column2)

		}
	}

	var goBusy sync.WaitGroup
	type chinType chan [2]*ColumnInfoType
	var pairChannel = make(chinType, 20)

	for index := 0; index < 5; index++ {
		goBusy.Add(1)
		go func(chin chinType) {
			for pair := range chin {
				processPair(pair[0], pair[1])
			}
			goBusy.Done()
		}(pairChannel)
	}

	tables1, err := H2.TableInfoByMetadata(metadata1)

	if err != nil {
		panic(err)
	}
	tables2, err := H2.TableInfoByMetadata(metadata2)
	if err != nil {
		panic(err)
	}

	for _, table1 := range tables1 {
		for _, column1 := range table1.Columns {
			//			fmt.Printf("-1- %v:\n",column1)
			err = column1.OpenStorage(false)
			if err != nil {
				panic(err)
			}

			column1.OpenCategoriesBucket()
			if column1.categoriesBucket == nil {
				continue
			}
			column1.OpenStatsBucket()
			for _, table2 := range tables2 {
				for _, column2 := range table2.Columns {
					//fmt.Printf("-2- %v\n",column2)
					err = column2.OpenStorage(false)
					if err != nil {
						panic(err)
					}
					column2.OpenCategoriesBucket()
					if column2.categoriesBucket == nil {
						continue
					}
					column2.OpenStatsBucket()
					var pair [2]*ColumnInfoType
					var catCnt1, catCnt2 uint64
					if !column1.CategoryCount.Valid() {
						if column1.statsBucket != nil {
							catCnt1, _ = utils.B8ToUInt64(column1.statsBucket.Get(columnInfoStatsCategoryCountKey))
							//fmt.Println(uqCnt1)
							column1.CategoryCount = jsnull.NewNullInt64(int64(catCnt1))
						}

					} else {
						catCnt1 = uint64(column1.UniqueRowCount.Value())
					}
					if !column2.CategoryCount.Valid() {
						if column2.statsBucket != nil {
							catCnt2, _ = utils.B8ToUInt64(column2.statsBucket.Get(columnInfoStatsCategoryCountKey))
							//fmt.Println(uqCnt2)
							column2.CategoryCount = jsnull.NewNullInt64(int64(catCnt2))
						}
					} else {
						catCnt2 = uint64(column2.CategoryCount.Value())
					}

					if catCnt1 < catCnt2 {
						pair[0] = column1
						pair[1] = column2
					} else {
						pair[0] = column2
						pair[1] = column1
					}

					pairChannel <- pair

				}
			}
		}
	}
	close(pairChannel)
	goBusy.Wait()

	for _, table1 := range tables1 {
		for _, column1 := range table1.Columns {
			if column1.storage != nil {
				column1.CloseStorage()
			}
		}
	}
	for _, table2 := range tables2 {
		for _, column2 := range table2.Columns {
			if column2.storage != nil {
				column2.CloseStorage()
			}
		}
	}

	H2.SaveColumnPairs(pairs);
}

func (da DataAccessType) MakeTablePairs(metadata1, metadata2 *MetadataType) {


}

func (da DataAccessType) MakeTablePairs2(metadata1, metadata2 *MetadataType){
	var emptyValue []byte = make([]byte, 0, 0)

	processPair := func(column1, column2 *ColumnInfoType) {
		var pair *ColumnPairType
		var err error
		var categoryCount uint64
		//column1.bucketLock.Lock()

		column1.categoriesBucket.ForEach(
			func(dataCategory, v []byte) error {
				if column2.categoriesBucket == nil {
					return nil
				}

				//column2.bucketLock.Lock()
				if column2.categoriesBucket.Bucket(dataCategory) != nil {
					categoryCount ++

					dataCategoryCopy := make([]byte, len(dataCategory))
					//fmt.Println("%v",dataCategory)
					copy(dataCategoryCopy, dataCategory)

					dc1 := ColumnDataCategoryStatsType{Column: column1}
					dc2 := ColumnDataCategoryStatsType{Column: column2}

					dc1.OpenBucket(dataCategoryCopy)
					dc1.OpenBitsetBucket()
					dc1.OpenStatsBucket()

					dc2.OpenBucket(dataCategoryCopy)
					dc2.OpenBitsetBucket()
					dc2.OpenStatsBucket()

					if dc1.BitsetBucket == nil && dc2.BitsetBucket == nil {
						//column2.bucketLock.Unlock()
						return nil
					}
					if pair == nil {
						pair, err = NewColumnPair(column1, column2, dataCategoryCopy)
						if err != nil {
							panic(err)
						}
						err = pair.OpenStorage(true)
						if err != nil {
							panic(err)
						}
						err = pair.OpenCategoriesBucket()
						if err != nil {
							panic(err)
						}
						err = pair.OpenStatsBucket()
						if err != nil {
							panic(err)
						}
					}
					pair.CategoryBucket = nil
					var uqCnt1, uqCnt2 uint64
					if dc1.StatsBucket != nil {
						uqCnt1, _ = utils.B8ToUInt64(dc1.StatsBucket.Get(columnInfoStatsHashUniqueCountKey))
						//fmt.Println(uqCnt1)
					}
					if dc2.StatsBucket != nil {
						uqCnt2, _ = utils.B8ToUInt64(dc2.StatsBucket.Get(columnInfoStatsHashUniqueCountKey))
						//fmt.Println(uqCnt2)
					}
					if uqCnt1 > uqCnt2 {
						dc2, dc1 = dc1, dc2
					}
					var dataCategoryIntersectionCount uint64
					dc1.BitsetBucket.ForEach(
						func(base, offset1 []byte) error {
							offset2 := dc2.BitsetBucket.Get(base)
							if offset2 == nil {
								return nil
							}
							baseUInt, _ := utils.B8ToUInt64(base)

							var offsetIntersection utils.B8Type
							for index := range offset1 {
								offsetIntersection[index] = offset1[index] & offset2[index]
							}
							intersection, _ := utils.B8ToUInt64(offsetIntersection[:])

							prod := baseUInt * wordSize
							rsh := uint64(0)
							prev := uint64(0)
							for {
								w := intersection >> rsh
								if w == 0 {
									break
								}
								result := rsh + trailingZeroes64(w) + prod
								if result != prev {
									if pair.CategoryBucket == nil {
										err = pair.OpenCategoryBucket(dataCategoryCopy)
										if err != nil {
											panic(err)
										}
									}

									//dataCategoryIntersectionCount++
									//pair.IntersectionCount++
									hashB8 := utils.UInt64ToB8(result)
									err = pair.CategoryBucket.Put(hashB8, emptyValue)
									if err != nil {
										panic(err)
									}
									prev = result

								}
								rsh++
							}

							return nil
						},
					)
					if pair.CategoryBucket != nil {
						err = pair.OpenCategoryStatsBucket()
						if err != nil {
							panic(err)
						}

						err = pair.CategoryStatsBucket.Put(columnPairStatsHashUniqueCountKey, utils.UInt64ToB8(dataCategoryIntersectionCount)[:])
						if err != nil {
							panic(err)
						}
					}

				}
				//column2.bucketLock.Unlock()
				return nil
			},
		)
		//column1.bucketLock.Unlock()
		if pair != nil && pair.HashIntersectionCount.Value()> 0 {
			err = pair.OpenStatsBucket()
			if err != nil {
				panic(err)
			}
			err = pair.StatsBucket.Put(columnPairHashUniqueCountKey, utils.Int64ToB8(pair.HashIntersectionCount.Value()))
			if err != nil {
				panic(err)
			}

			err = pair.StatsBucket.Put(columnPairHashCategoryCountKey, utils.UInt64ToB8(categoryCount))
			if err != nil {
				panic(err)
			}

			pair.CloseStorageTransaction(true)
			pair.CloseStorage()
			//tracelog.Info("%v <-%v-> %v\n", pair.column1, pair.IntersectionCount, "%v",  pair.column2)

		}

	}

	var goBusy sync.WaitGroup
	type chinType chan [2]*ColumnInfoType
	var pairChannel = make(chinType, 20)

	for index := 0; index < 5; index++ {
		goBusy.Add(1)
		go func(chin chinType) {
			for pair := range chin {
				processPair(pair[0], pair[1])
			}
			goBusy.Done()
		}(pairChannel)
	}

	tables1, err := H2.TableInfoByMetadata(metadata1)

	if err != nil {
		panic(err)
	}
	tables2, err := H2.TableInfoByMetadata(metadata2)
	if err != nil {
		panic(err)
	}

	for _, table1 := range tables1 {
		for _, column1 := range table1.Columns {
			for _, table2 := range tables2 {
				for _, column2 := range table2.Columns {
					pair,_ := NewColumnPair(column1,column2, make([]byte,0))
					err = pair.OpenStorage(false)
					if err != nil {
						panic(err)
					}
					if pair.storage == nil {
						continue
					}
					err = pair.column1.OpenStorage(false)
					if err != nil {
						panic(err)
					}
					err = pair.column2.OpenStorage(false)
					if err != nil {
						panic(err)
					}

					err =  pair.OpenCategoriesBucket();
					if err != nil {
						panic(err)
					}
					pair.CategoriesBucket.ForEach(
						func(category,_[]byte) error {
							pair.OpenCategoryBucket(category)
							err = pair.column1.OpenCategoriesBucket()
							err = pair.column2.OpenCategoriesBucket()

							dc1 := ColumnDataCategoryStatsType{Column: column1}
							dc2 := ColumnDataCategoryStatsType{Column: column2}

							dc1.OpenBucket(category)
							dc2.OpenBucket(category)

							err = pair.OpenCategoryHashBucket()
							pair.CategoryHashBucket.ForEach(
								func(hash,_[]byte) error {
									dc1.OpenHashBucket(hash)
									dc2.OpenHashBucket(hash)
									dc1.HashSourceBucket.ForEach(
										func(lineBytes,_ []byte) error {
											//TODO:
											return nil
										},
									)
									return nil
								},
							)
							return nil
						},
					)
					//fmt.Printf("-2- %v\n",column2)
					/*err = column2.OpenStorage(false)
					if err != nil {
						panic(err)
					}
					column2.OpenCategoriesBucket()
					if column2.categoriesBucket == nil {
						continue
					}
					column2.OpenStatsBucket()
					var pair [2]*ColumnInfoType
					var catCnt1, catCnt2 uint64
					if !column1.CategoryCount.Valid() {
						if column1.statsBucket != nil {
							catCnt1, _ = utils.B8ToUInt64(column1.statsBucket.Get(columnInfoStatsCategoryCountKey))
							//fmt.Println(uqCnt1)
							column1.CategoryCount = jsnull.NewNullInt64(int64(catCnt1))
						}

					} else {
						catCnt1 = uint64(column1.UniqueRowCount.Value())
					}
					if !column2.CategoryCount.Valid() {
						if column2.statsBucket != nil {
							catCnt2, _ = utils.B8ToUInt64(column2.statsBucket.Get(columnInfoStatsCategoryCountKey))
							//fmt.Println(uqCnt2)
							column2.CategoryCount = jsnull.NewNullInt64(int64(catCnt2))
						}
					} else {
						catCnt2 = uint64(column2.CategoryCount.Value())
					}

					if catCnt1 < catCnt2 {
						pair[0] = column1
						pair[1] = column2
					} else {
						pair[0] = column2
						pair[1] = column1
					}

					pairChannel <- pair */

				}
			}
		}
	}
	close(pairChannel)
	goBusy.Wait()

	for _, table1 := range tables1 {
		for _, column1 := range table1.Columns {
			if column1.storage != nil {
				column1.CloseStorage()
			}
		}
	}
	for _, table2 := range tables2 {
		for _, column2 := range table2.Columns {
			if column2.storage != nil {
				column2.CloseStorage()
			}
		}
	}
}

