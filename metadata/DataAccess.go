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
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"hash/fnv"
	"io"
	"os"
	"strings"
	"sync"
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

func (da *DataAccessType) readTableDump(source *TableInfoType) {
	funcName := "DataAccessType.ReadTableDumpData"

	var x0D = []byte{0x0D}

	//	lineSeparatorArray[0] = da.DumpConfiguration.LineSeparator
	var statsChans /*,storeChans */ []ColumnDataChannelType
	var goBusy sync.WaitGroup
	tracelog.Info(packageName, funcName, "Start processing table %v", source)

	gzfile, err := os.Open(da.DumpConfiguration.DumpBasePath + source.PathToFile.Value())
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

		metadataColumnCount := len(source.Columns)

		lineColumns := bytes.Split(line, []byte{da.DumpConfiguration.FieldSeparator})
		lineColumnCount := len(lineColumns)
		if metadataColumnCount != lineColumnCount {
			panic(fmt.Sprintf("Number of column mismatch in line %v. Expected #%v; Actual #%v",
				lineNumber,
				metadataColumnCount,
				lineColumnCount,
			))
		}

		for columnIndex := range source.Columns {
			if lineNumber == 1 {
				if columnIndex == 0 {
					statsChans = make([]ColumnDataChannelType, 0, metadataColumnCount)
					//storeChans = make([]ColumnDataChannelType, 0,metadataColumnCount);
				}
				statsChan := make(ColumnDataChannelType, 100)
				statsChans = append(statsChans, statsChan)
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
			statsChans[columnIndex] <- &ColumnDataType{
				column:     source.Columns[columnIndex],
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

	if statsChans != nil {
		for index := range statsChans {
			close(statsChans[index])
		}
		statsChans = nil
	}
	goBusy.Wait()
	for index := range source.Columns {
		source.Columns[index].bucketLock.Lock()
		source.Columns[index].CloseStorageTransaction(true)
		source.Columns[index].CloseStorage()
		source.Columns[index].bucketLock.Unlock()
	}
	tracelog.Info(packageName, funcName, "Finish processing table %v", source)
}

func (da *DataAccessType) collectDataStats(val *ColumnDataType) {
	//	funcName := "DataAccessType.CollectDataStats"
	//	tracelog.Started(packageName,funcName)
	column := val.column
	//	tracelog.Info(packageName,funcName,"Collecting statistics for line %v of column %v[%v]...",val.lineNumber,column,column.Id)
	byteLength := len(val.bValue)
	if byteLength == 0 {
		if !column.NullCount.Valid() {
			column.NullCount = jsnull.NewNullInt64(1)
		} else {
			(*column.NullCount.Reference())++
		}
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
	/*bSum = 0
	for _,bChar := range val.bValue{
		if bChar>0 {
			res := fStatsMean - float64(bChar)
			bSum = bSum + res*res
		}
	}
	val.bStatsStdv = uint8(math.Sqrt(bSum))
	*/

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

	/*var found *ColumnDataCategoryStatsType
	for _, category := range column.DataCategories {
		if category.ByteLength.Value() != int64(byteLength) {
			continue
		}
		if !category.IsNumeric.Value() && !val.isNumeric {
			found = category
			break
		}
		if category.IsNumeric.Value() == val.isNumeric &&
			int8(category.FloatingPointScale.Value()) == val.fpScale &&
			category.IsNegative.Value() == val.isNegative {
			found = category
			break
		}
	}*/
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

/*
func (da DataAccessType) CollectMinMaxStats(in scm.ChannelType, out scm.ChannelType) {
	for raw := range in {
		switch val := raw.Get().(type) {
		case TableInfoType:
			out <- raw
		case (*ColumnDataType):
			column := val.column
			byteLength := len(val.bValue)
			if byteLength == 0 {
				if !column.NullCount.Valid() {
					column.NullCount = jsnull.NewNullInt64(1)
				} else {
					(*column.NullCount.Reference())++
				}
				continue
			}

			sValue := string(val.bValue)
			var err error
			var nValue     float64
			var fpScale    int
			var isNegative bool

			isSubHash := byteLength > da.SubHashByteLengthThreshold
			nValue =float64(0)
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

				for _,bChar := range val.bValue{
					if bChar>0 {
						bSubHash = ((uint8(37) * bSubHash) + uint8(bChar)) & 0xff;
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

			/ *var found *ColumnDataCategoryStatsType
			for _, category := range column.DataCategories {
				if category.ByteLength.Value() != int64(byteLength) {
					continue
				}
				if !category.IsNumeric.Value() && !val.isNumeric {
					found = category
					break
				}
				if category.IsNumeric.Value() == val.isNumeric &&
					int8(category.FloatingPointScale.Value()) == val.fpScale &&
					category.IsNegative.Value() == val.isNegative {
					found = category
					break
				}
			}* /
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
					Column:             val.column,
					ByteLength:         jsnull.NewNullInt64(int64(byteLength)),
					IsNumeric:          jsnull.NewNullBool(isNumeric),
					FloatingPointScale: jsnull.NewNullInt64(int64(fpScale)),
					IsNegative:         jsnull.NewNullBool(isNegative),
					NonNullCount:       jsnull.NewNullInt64(int64(0)),
					HashUniqueCount:    jsnull.NewNullInt64(int64(0)),
					IsSubHash:jsnull.NewNullBool(isSubHash),
					SubHash:jsnull.NewNullInt64(int64(bSubHash)),
				}
				if column.DataCategories == nil {
					column.DataCategories = make([]*ColumnDataCategoryStatsType, 0, 2)
				}
				column.DataCategories = append(column.DataCategories, found)
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
			out <- scm.NewMessage().Put(val)
		}
	}
	close(out)
}*/

/*
  +hashStorageRoot
   -"tables"  (+)
    -tableId (+)
     -"columns" (+)
      -columnId (b)
       -category (b)
         - "stats" (b)
            - "hashUniqueCount" / int64
         - "bitset" (+b)
            -offset/bit(k/v)
         - "hashValues" (+b)
           - hash/value (b)
            - row#/position (k/v)
*/
func (da *DataAccessType) storeData(val *ColumnDataType) {
	//	funcName := "DataAccessType.storeData"
	bLen := uint16(len(val.bValue))
	if bLen == 0 || val.dataCategory == nil {
		return
	}

	hasher := fnv.New64()
	var hashUIntValue uint64
	var hValue []byte
	if bLen > hashLength {
		hasher.Reset()
		hasher.Write(val.bValue)
		hashUIntValue = hasher.Sum64()
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
	if val.column.categoriesBucket == nil {
		//tracelog.Info(packageName, funcName, "Lock for column %v",val.column)
		err = val.column.OpenStorage(true)
		if err != nil {
			panic(err)
		}
		if val.dataCategory.CategoryBucket == nil {
			err = val.dataCategory.GetOrCreateBucket(nil)
			if err != nil {
				panic(err)
			}
		}
	} else {
		if val.dataCategory.CategoryBucket == nil {
			err = val.dataCategory.GetOrCreateBucket(nil)
			if err != nil {
				panic(err)
			}
		}
	}

	// -"categories"  (+)
	//  -category
	//   "bitset"
	//
	// -"stats" (+)
	// -"columns" (+)
	//  -columnId (b)
	if val.dataCategory.CategoryBucket == nil {
		panic("Category bucket has not been created!")
	}
	if val.dataCategory.HashValuesBucket == nil {
		panic("HashValues bucket has not been created!")
	}
	if val.dataCategory.HashStatsBucket == nil {
		panic("HashStats bucket has not been created!")
	}
	if val.dataCategory.BitsetBucket == nil {
		panic("Bitset bucket has not been created!")
	}

	var hashBucket *bolt.Bucket
	hashBucket = val.dataCategory.HashValuesBucket.Bucket(hValue[:])
	if hashBucket == nil {
		hashBucket, err = val.dataCategory.HashValuesBucket.CreateBucket(hValue[:])
		if err != nil {
			panic(err)
		}
		/*if value, found := utils.B8ToUInt64(
			category.HashStatsBucket.Get(
				hashStatsUnqiueCountBucketBytes,
			),
		); !found {
			category.HashStatsBucket.Put(
				hashStatsUnqiueCountBucketBytes,
				utils.UInt64ToB8(1),
			)
		} else {
			value++
			category.HashStatsBucket.Put(
				hashStatsUnqiueCountBucketBytes,
				utils.UInt64ToB8(value),
			)
		}*/
	}
	//bitsetBucket.Get()

	baseUIntValue, offsetUIntValue := sparsebitset.OffsetBits(hashUIntValue)
	baseB8Value := utils.UInt64ToB8(baseUIntValue)
	//offsetB8Value := utils.UInt64ToB8(offsetUIntValue)
	bits, _ := utils.B8ToUInt64(val.dataCategory.BitsetBucket.Get(baseB8Value))
	bits = bits | (1 << offsetUIntValue)
	val.dataCategory.BitsetBucket.Put(baseB8Value, utils.UInt64ToB8(bits))

	hashBucket.Put(
		utils.UInt64ToB8(val.lineNumber),
		//TODO: switch to real file offset to column value instead of lineNumber
		utils.UInt64ToB8(val.lineNumber),
	)
}

/*
func (da DataAccessType) SplitDataToBuckets(in scm.ChannelType, out scm.ChannelType) {
	funcName := "DataAccess.SplitDataToBuckets"

	//var currentTable *TableInfoType;
	var transactionCount uint64 = 0
	//	var emptyValue []byte = make([]byte, 0)
	hasher := fnv.New64()
	var storageTx *bolt.Tx = nil
	for raw := range in {
		switch val := raw.Get().(type) {
		case TableInfoType:
		case (*ColumnDataType):
			var hashUIntValue uint64
			bLen := uint16(len(val.bValue))
			var hValue []byte
			if bLen > hashLength {
				hasher.Reset()
				hasher.Write(val.bValue)
				hashUIntValue = hasher.Sum64()
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

			if storageTx == nil {
				storageTx, err = HashStorage.Begin(true)
				if err != nil {
					panic(err)

				}
			}
			// -"tables"  (+)
			//  -tableId (+)
			// -"columns" (+)
			//  -columnId (b)

			if val.dataCategory.CategoryBucket == nil {
				err = val.dataCategory.GetOrCreateBucket(
					storageTx,
					nil,
				)
				if err != nil {
					panic(err)
				}
			}

			if val.dataCategory.CategoryBucket == nil {
			      panic("Category bucket has not been created!")
			}
			if val.dataCategory.HashValuesBucket== nil {
				panic("HashValues bucket has not been created!")
			}
			if val.dataCategory.HashStatsBucket == nil {
				panic("HashStats bucket has not been created!")
			}
			if val.dataCategory.BitsetBucket == nil {
				panic("Bitset bucket has not been created!")
			}

			var hashBucket *bolt.Bucket
			hashBucket = val.dataCategory.HashValuesBucket.Bucket(hValue[:])
			if hashBucket == nil {
				hashBucket, err = val.dataCategory.HashValuesBucket.CreateBucket(hValue[:])
				if err != nil {
					panic(err)
				}
				/ *if value, found := utils.B8ToUInt64(
					category.HashStatsBucket.Get(
						hashStatsUnqiueCountBucketBytes,
					),
				); !found {
					category.HashStatsBucket.Put(
						hashStatsUnqiueCountBucketBytes,
						utils.UInt64ToB8(1),
					)
				} else {
					value++
					category.HashStatsBucket.Put(
						hashStatsUnqiueCountBucketBytes,
						utils.UInt64ToB8(value),
					)
				}* /
			}

			//bitsetBucket.Get()

			baseUIntValue, offsetUIntValue := sparsebitset.OffsetBits(hashUIntValue)
			baseB8Value := utils.UInt64ToB8(baseUIntValue)
			//offsetB8Value := utils.UInt64ToB8(offsetUIntValue)
			bits, _ := utils.B8ToUInt64(val.dataCategory.BitsetBucket.Get(baseB8Value))
			bits = bits | (1 << offsetUIntValue)
			val.dataCategory.BitsetBucket.Put(baseB8Value, utils.UInt64ToB8(bits))

			hashBucket.Put(
				utils.UInt64ToB8(val.lineNumber),
				//TODO: switch to real file offset to column value instead of lineNumber
				utils.UInt64ToB8(val.lineNumber),
			)
			transactionCount++
			if transactionCount >= da.TransactionCountLimit {
				tracelog.Info(packageName,funcName,"Сommit %v txs",transactionCount)
				err = storageTx.Commit()
				if err != nil {
					panic(err)
				}
				storageTx = nil
				transactionCount = 0
				val.column.TableInfo.ResetBuckets()
			}
		}
	}
	if storageTx != nil {
		println("final commit")
		err := storageTx.Commit()
		if err != nil {
			panic(err)
		}
		storageTx = nil
	}

	close(out)

}*/


func (da DataAccessType) makeColumnPairs(metadata1,metadata2 MetadataType, stage string) {
	tables1, err := H2.TableInfoByMetadata(metadata1)


	if err != nil {
		panic(err)
	}
	tables2, err := H2.TableInfoByMetadata(metadata2)
	if err != nil {
		panic(err)
	}

	for _,table1 := range tables1 {
		for _,column1 := range table1.Columns {
			err = column1.OpenStorage(false)
			if err!=nil {
				panic(err)
			}
			for _, table2 := range tables2 {
				for _,column2 := range table2.Columns {
					err = column2.OpenStorage(false)
					if err != nil {
						panic(err)
					}

					column1.categoriesBucket.ForEach(
						func(dataCategory,v []byte) error {
							if column2.categoriesBucket.Bucket(dataCategory) != nil {
								dataCategoryCopy := make([]byte,len(dataCategory))
								copy(dataCategoryCopy, dataCategory)
								var pair, err = NewColumnPair(column1, column2, dataCategoryCopy)
								if err != nil {
									panic(err)
								}
								pair.OpenStorage(true)
								pair.OpenCategoriesBucket()
								//TODO: Continue here...
							}
							return nil
						} ,
					)
				}
			}
		}
	}
}
/*
func (da DataAccessType) BuildDataBitsetIntersections() {
	md1 := raw.GetN(0).(*MetadataType)
	md2 := raw.GetN(1).(*MetadataType)

	//fmt.Println(md1)
	//fmt.Println(md2)
	pushPairs(md1,md2,"CHAR")
	pushPairs(md1,md2,"NUMBER")
}

func (da DataAccessType) MakePairs(in, out scm.ChannelType) {
	funcName := "DataAccessType.MakePairs"

	pushPairs := func (metadata1, metadata2 *MetadataType, stage string) {
		tables1, err := H2.TableInfoByMetadata(metadata1)


		if err != nil {
			panic(err)
		}
		tables2, err := H2.TableInfoByMetadata(metadata2)
		if err != nil {
			panic(err)
		}
		for tables1Index := range tables1 {
			for tables2Index := range tables2 {
				// TODO: get number of categories
				pairs := make([]*ColumnPairType, 0, len(tables1[tables1Index].Columns) * len(tables2[tables2Index].Columns))
				tracelog.Info(packageName,funcName,"Open read-only transaction")
				tx, _ := HashStorage.Begin(false);

				for _, column1 := range tables1[tables1Index].Columns {
					column1.TableInfo.ResetBuckets()

					//err = column1.GetOrCreateBucket(tx)

					if err != nil {
						panic(err)
					}
					if column1.ColumnBucket == nil {
						continue
					}

					for _, column2 := range tables2[tables2Index].Columns {
						column2.TableInfo.ResetBuckets()
						if column1.Id.Value() == column2.Id.Value() {
							continue
						}
						//fmt.Println(column1, column2)

						//fmt.Printf("%v,%v\n", column1, column2)
						//err = column2.GetOrCreateBucket(tx)
						if err != nil {
							panic(err)
						}

						if column2.ColumnBucket == nil {
							continue
						}

						column1.ColumnBucket.ForEach(func(dataCategory, value []byte) error {
							if stage == "CHAR" && dataCategory[0] == 0 {
							} else if stage == "NUMBER" && dataCategory[0] != 0 {
							} else {
								return nil
							}

							bucket := column2.ColumnBucket.Bucket(dataCategory)

							if bucket != nil {
								dataCategoryCopy := make([]byte,len(dataCategory))
								copy(dataCategoryCopy, dataCategory)
								var pair, err = NewColumnPair(column1, column2, dataCategoryCopy)
								if err != nil {
									panic(err)
								}
								pairs = append(pairs, pair)
							}

							return nil
						})
					}
				}

				for _,pair := range pairs {
					out <- scm.NewMessageSize(1).Put("PAIR").PutN(0,pair)
					if pair.IntersectionCount>0 {
						if (pair.column2.ColumnName.Value() =="INFORMER_DEAL_ID" &&
							pair.column1.ColumnName.Value() == "CONTRACT_ID") ||
							(pair.column1.ColumnName.Value() =="INFORMER_DEAL_ID" &&
								pair.column2.ColumnName.Value() == "CONTRACT_ID") {
							fmt.Println(pair.column1, pair.column2, pair.dataCategory, pair.IntersectionCount)
						}
					}
				}
				tracelog.Info(packageName,funcName,"Open read-only transaction")

				tx.Rollback()

			}
		}

	}

	for raw := range in {
		fmt.Println(raw.Get())
		switch val := raw.Get().(type) {
		case string:
			if val == "2MD" {
				md1 := raw.GetN(0).(*MetadataType)
				md2 := raw.GetN(1).(*MetadataType)

				//fmt.Println(md1)
				//fmt.Println(md2)
				pushPairs(md1,md2,"CHAR")
				pushPairs(md1,md2,"NUMBER")

			}

		}
	}
	close(out)
}

var  dataIntersectionLabel []byte = []byte("dataIntersection")
//var  dataIntersectionStatsLabel []byte = []byte("stats")
var  dataIntersectionHashLabel []byte = []byte("hash")

/*
func (da DataAccessType) BuildDataBitsets(in, out scm.ChannelType) {
	var storageTx *bolt.Tx
	var err error
	var emptyValue []byte = make([]byte, 0, 0)
	var transactionCount uint64 = 0
	var written sync.WaitGroup
	writeIntersection := func (pair *ColumnPairType,hashChannel chan uint64) {
		defer written.Done()
		for data := range hashChannel{
			if pair.currentTx == nil {
				err = pair.OpenStorage(true)
				if err != nil{
					panic(err)
				}
			}
			err = pair.hashBucket.Put(utils.UInt64ToB8(data)[:],emptyValue)
			if err != nil {
				pair.CloseStorage(false)
				panic(err)

			}
		}
		pair.CloseStorage(true)
		panic(err)
	}
	writeIntersectingHashValue := func (columnPair *ColumnPairType, hValue uint64) {

//		if storageTx == nil {
//			storageTx, err = HashStorage.Begin(true)
//			if err != nil {
//				panic(err)
//			}
//		}
		labelBucket, err := storageTx.CreateBucketIfNotExists(dataIntersectionLabel)
		if err != nil {
			panic(err)
		}

		pairBucket, err := labelBucket.CreateBucketIfNotExists([]byte(columnPair.dataBucketName))
		if err != nil {
			panic(err)
		}

		categoryBucket, err := pairBucket.CreateBucketIfNotExists([]byte(columnPair.dataCategory))
		if err != nil {
			panic(err)
		}

		/ *statsLabelBucket,err := categoryBucket.CreateBucketIfNotExists(dataIntersectionStatsLabel)
		if err != nil {
			panic(err)
		}* /
		hashLabelBucket, err := categoryBucket.CreateBucketIfNotExists(dataIntersectionHashLabel)
		if err != nil {
			panic(err)
		}
		//_ = hashLabelBucket
		//_=emptyValue
		//_= hValue
		hashLabelBucket.Put(utils.UInt64ToB8(hValue)[:], emptyValue)
		transactionCount ++
	}


	pushHashValues := func (bsBucket1,bsBucket2 *bolt.Bucket,
				pair *ColumnPairType,
				) {
		var hashChannel chan uint64;
		bsBucket1.ForEach(func (keyBytes, valueBytes1 []byte) (error) {
			keyUInt,_ := utils.B8ToUInt64(keyBytes)
			valueBytes2 := bsBucket2.Get(keyBytes)
			if valueBytes2 == nil {
				return nil
			}
			var result utils.B8Type
			for index := range valueBytes1 {
				result[index] = valueBytes1[index] & valueBytes2[index]
			}
			intersection,_ := utils.B8ToUInt64(result[:])
			prod := keyUInt * wordSize
			rsh := uint64(0)
			prev := uint64(0)
			for {
				w := intersection >> rsh
				if w == 0 {
					break
				}
				result := rsh + trailingZeroes64(w) + prod
				if result != prev {
					if hashChannel == nil {
						hashChannel = make(chan uint64,100)
						written.Add(1)
						go writeIntersection(pair,hashChannel)
					}
					pair.IntersectionCount++
					hashChannel <- result
					//writeIntersectingHashValue(pair,result)
					prev = result

				}
				rsh++
			}
			return nil
		})
		//out <- scm.NewMessageSize(1).Put("PAIR").PutN(0,pair)
		if hashChannel != nil {
			written.Wait()
		}
	}

	_ = writeIntersectingHashValue
	_=pushHashValues
	_=writeIntersection

	for raw := range in {

		switch val := raw.Get().(type) {
		case string:
			switch val {
			case "PAIR":
				/ *pair := raw.GetN(0).(*ColumnPairType)
				var hashUniqueCount1, hashUniqueCount2 uint64

				vv
				if pair.column1.ColumnBucket == nil {
					err = pair.column1.GetOrCreateBucket(storageTx)
					if err != nil {
						panic(err)
					}
				}
				if pair.column2.ColumnBucket == nil {
					err = pair.column2.GetOrCreateBucket(storageTx)
					if err != nil {
						panic(err)
					}
				}
				cdc1 := &ColumnDataCategoryStatsType{
					Column:pair.column1,
				}
				cdc2 := &ColumnDataCategoryStatsType{
					Column:pair.column2,
				}

				cdc1.GetOrCreateBucket(storageTx, pair.dataCategory)
				cdc2.GetOrCreateBucket(storageTx, pair.dataCategory)
				if cdc1.HashStatsBucket != nil {
					UQKey := cdc1.HashStatsBucket.Get(hashStatsUnqiueCountBucketBytes)
					if UQKey != nil {
						hashUniqueCount1, _ = utils.B8ToUInt64(UQKey)
					}
				}else {
				}
				if cdc2.HashStatsBucket != nil {
					UQKey := cdc2.HashStatsBucket.Get(hashStatsUnqiueCountBucketBytes)
					if UQKey != nil {
						hashUniqueCount2, _ = utils.B8ToUInt64(UQKey)
					}
				}else {
				}

				if hashUniqueCount1 <= hashUniqueCount2 {
					pushHashValues(cdc1.BitsetBucket, cdc2.BitsetBucket, pair);
				} else {
					pushHashValues(cdc2.BitsetBucket, cdc1.BitsetBucket, pair);
				}
				if transactionCount >= da.TransactionCountLimit {
					log.Println("commit... ")
					err = storageTx.Commit()
					if err != nil {
						panic(err)
					}
					pair.column1.TableInfo.ResetBuckets()
					pair.column2.TableInfo.ResetBuckets()
					storageTx = nil
					transactionCount = 0
				}* /
				//out <-scm.NewMessage().Put(pair)
			}
		}
	}

	if storageTx != nil {
		err = storageTx.Commit()
		if err != nil {
			panic(err)
		}
	}

	close(out)
}






*/
func (da DataAccessType) LoadStorage() {
	var goBusy sync.WaitGroup
	tableСhannel := make(TableInfoTypeChannel)
	goProcess := func(chin TableInfoTypeChannel) {
		for ti := range chin {
			da.readTableDump(ti)
		}
		goBusy.Done()
	}
	err := H2.CreateDataCategoryTable()
	if err != nil {
		panic(err)
	}

	mtd1, err := H2.MetadataById(jsnull.NewNullInt64(10))
	if err != nil {
		panic(err)
	}

	mtd2, err := H2.MetadataById(jsnull.NewNullInt64(11))
	if err != nil {
		panic(err)
	}
	for i := 0; i < 3; i++ {
		goBusy.Add(1)
		go goProcess(tableСhannel)
	}

	tables, err := H2.TableInfoByMetadata(mtd1)
	for _, tableInfo := range tables {
		//if tableInfo.Id.Value() == int64(268) {
		tableСhannel <- tableInfo
		//}
	}
	tables, err = H2.TableInfoByMetadata(mtd2)
	for _, tableInfo := range tables {
		//if tableInfo.Id.Value() == int64(291) {
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
