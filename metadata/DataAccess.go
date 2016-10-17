package metadata

import (
	jsnull "./../jsnull"
	scm "./../scm"
	sparsebitset "./../sparsebitset"
	utils "./../utils"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"hash/fnv"
	"io"
	"os"
	"strconv"
	"strings"
	"errors"
	"log"
)

const hashLength = 8
const wordSize = uint64(64)

type B9Type [9]byte

var HashStorage *bolt.DB

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
	DumpConfiguration     DumpConfigurationType
	ColumnBucketsCache *utils.Cache
	TransactionCountLimit uint64
	SubHashByteLengthThreshold int
	SubHashDumpRowCountThreshold int64
}

type columnDataType struct {
	column     *ColumnInfoType
	lineNumber uint64
	bValue     []byte
	nValue     float64
	isNumeric  bool
	fpScale    int8
	isNegative bool
	isSubHash bool
	bSubHash uint8
}


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
		)/*,
			byte(c.bStatsMean),
			byte(c.bStatsMax),
			byte(c.bStatsMin),
		)*/
	}
	return
}


func ReportHashStorageContents() {
	categoryName := func(k []byte) (result string){
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
			hashes,_ := utils.B8ToUInt64(categoryBucket.Bucket(hashStatsName).Get(hashStatsUnqiueCountName))
			var bsWords, lines, hBuckets uint64
			categoryBucket.Bucket(bitsetBucketName).ForEach(
				func(ik,iv []byte) error {
					bsWords ++
					return nil
				},
			)
			hashValuesBucket := categoryBucket.Bucket(hashValuesBucketName)
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
			/*fmt.Println(string(k),k,columnsLabel)
			if columns != nil && bytes.Compare(k,columnsLabel)==0 {
				fmt.Println(string(k))
				reportColumns(columns)
			}*/
			tableBucket.ForEach(func(ik,_ []byte) error {
				if bytes.Compare(ik,columnsLabel) == 0 {
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
		if bytes.Compare(k,tablesLabel)==0 {
			reportTables(b)
		}
		return nil
	})
	tx.Rollback();

}

/*
func(c ColumnInfoType) columnBucketName() (result ColumnBucketNameType) {
	if !c.Id.Valid {
		panic(fmt.Sprintf("Column Id has not been initialized for table %v",c.TableInfo))
	}
	binary.PutUvarint(result[:],uint64(c.Id.Int64))
	return
}*/


func (da DataAccessType) ReadTableDumpData(in scm.ChannelType, out scm.ChannelType) {
	//	var lineSeparatorArray []byte
	//	var fieldSeparatorArray = []
	var x0D = []byte{0x0D}

	//	lineSeparatorArray[0] = da.DumpConfiguration.LineSeparator


	for raw := range in {
		var source *TableInfoType
		switch val := raw.Get().(type) {
		case *TableInfoType:
			source = val
		default:
			panic(fmt.Sprintf("Type is not expected %T", raw.Get()))

		}
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

		metadataColumnCount := len(source.Columns)
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

			lineNumber++

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
				if columnIndex == 0 && lineNumber == 1 {
					out <- scm.NewMessage().Put(source)
				}
				out <- scm.NewMessage().Put(
					columnDataType{
						column:     source.Columns[columnIndex],
						bValue:     lineColumns[columnIndex],
						lineNumber: lineNumber,
					},
				)
			}
		}
	}
	close(out)
}

func (da DataAccessType) CollectMinMaxStats(in scm.ChannelType, out scm.ChannelType) {
	for raw := range in {
		switch val := raw.Get().(type) {
		case TableInfoType:
			out <- raw
		case columnDataType:
			{
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
				val.isSubHash = byteLength > da.SubHashByteLengthThreshold &&
					val.column.TableInfo.RowCount.Value() > da.SubHashDumpRowCountThreshold
				val.nValue, err = strconv.ParseFloat(sValue, 64)

				val.isNumeric = err == nil

				if val.isNumeric {
					var lengthChanged bool
					if strings.Index(sValue, ".") != -1 {
						trimmedValue := strings.TrimRight(sValue, "0")
						lengthChanged = len(sValue) != len(trimmedValue)
						if lengthChanged {
							sValue = trimmedValue
						}
						val.fpScale = int8(len(sValue) - (strings.Index(sValue, ".") + 1))
						val.isSubHash = false
					} else {
						val.fpScale = -1
					}

					val.isNegative = strings.HasPrefix(sValue, "-")
					(*column.NumericCount.Reference())++
					if !column.MaxNumericValue.Valid() {
						column.MaxNumericValue = jsnull.NewNullFloat64(val.nValue)
					} else if column.MaxNumericValue.Value() < val.nValue {
						(*column.MaxNumericValue.Reference()) = val.nValue
					}

					if !column.MinNumericValue.Valid() {
						column.MinNumericValue = jsnull.NewNullFloat64(val.nValue)
					} else if column.MinNumericValue.Value() > val.nValue {
						(*column.MinNumericValue.Reference()) = val.nValue
					}
					if val.fpScale != -1 && lengthChanged {
						sValue = strings.TrimRight(fmt.Sprintf("%f", val.nValue), "0")
						val.bValue = []byte(sValue)
						byteLength = len(val.bValue)
					}
				}
				if val.isSubHash {
					val.bSubHash = 0
					for _,bChar := range val.bValue{
						if bChar>0 {
							val.bSubHash = ((uint8(37) * val.bSubHash) + uint8(bChar)) & 0xff;
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

				var found *ColumnDataCategoryStatsType
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
				}
				if found == nil {
					found = &ColumnDataCategoryStatsType{
						Column:             val.column,
						IsNumeric:          jsnull.NewNullBool(val.isNumeric),
						FloatingPointScale: jsnull.NewNullInt64(int64(val.fpScale)),
						IsNegative:         jsnull.NewNullBool(val.isNegative),
						ByteLength:         jsnull.NewNullInt64(int64(byteLength)),
						NonNullCount:       jsnull.NewNullInt64(int64(0)),
						HashUniqueCount:    jsnull.NewNullInt64(int64(0)),
					}

					if column.DataCategories == nil {
						column.DataCategories = make([]*ColumnDataCategoryStatsType, 0, 2)
					}
					column.DataCategories = append(column.DataCategories, found)

				}

				(*found.NonNullCount.Reference())++
				if found.MaxStringValue.Value() < sValue || !found.MaxStringValue.Valid() {
					found.MaxStringValue = jsnull.NewNullString(sValue)
				}

				if found.MinStringValue.Value() > sValue || !found.MinStringValue.Valid() {
					found.MinStringValue = jsnull.NewNullString(sValue)
				}

				if found.IsNumeric.Value() {
					if !found.MaxNumericValue.Valid() {
						found.MaxNumericValue = jsnull.NewNullFloat64(val.nValue)

					} else if found.MaxNumericValue.Value() < val.nValue {
						(*found.MaxNumericValue.Reference()) = val.nValue
					}
					if !column.MinNumericValue.Valid() {
						found.MinNumericValue = jsnull.NewNullFloat64(val.nValue)
					} else if column.MinNumericValue.Value() > val.nValue {
						(*found.MinNumericValue.Reference()) = val.nValue
					}
				}
				out <- scm.NewMessage().Put(val)
			}
		}
	}
	close(out)
}

/*
  +hashStorageRoot
   -"tables"  (+)
    -tableId (+)
     -"columns" (+)
      -columnId (b)
       -category (b)
         - "hashStats" (b)
            - "uniqueCount" / int64
         - "bitset" (+b)
            -offset/bit(k/v)
         - "hashValues" (+b)
           - hash/value (b)
            - row#/position (k/v)
*/


type columnSupplementaryBucketsType struct {
	bitsetBucket *bolt.Bucket
	hashValuesBucket *bolt.Bucket
	hashStatsBucket *bolt.Bucket
	dataCategoryBucket *bolt.Bucket
}

var tablesLabel = []byte("tables")
var columnsLabel = []byte("columns")

var bitsetBucketName = []byte("bitset")
var hashValuesBucketName = []byte("hashValues")

var hashStatsName = []byte("hashStats")
var hashStatsUnqiueCountName = []byte("uniqueCount")

func (da DataAccessType) tableBucket(tx *bolt.Tx, table *TableInfoType) (result *bolt.Bucket, err error) {
	if table == nil {
		err = TableInfoNotInitialized
		return
	}
	if !table.Id.Valid() {
		err = TableIdNotInitialized
		return
	}
	tableBucketName := utils.Int64ToB8(table.Id.Value())
	tablesLabelBucket := tx.Bucket(tablesLabel)

	if tablesLabelBucket == nil {
		if tx.Writable() {
			tablesLabelBucket, err = tx.CreateBucket(tablesLabel)
			if err != nil {
				return
			}
			if tablesLabelBucket == nil {
				err = errors.New("Could not create predefined buclet \"tables\". Got empty value")
				return
			} else {
				log.Println("Bucket \"tables\" created")
			}
		} else {
			err = errors.New("Predefined bucket \"tables\" does not exist")
			return
		}
	}

	result = tablesLabelBucket.Bucket(tableBucketName[:])
	if result == nil {
		if tx.Writable() {
			result, err = tablesLabelBucket.CreateBucket(tableBucketName[:])
			if err != nil {
				return
			}
			if result == nil {
				err = errors.New(fmt.Sprintf("Could not create buclet for table id %v. Got empty value",table.Id))
				return
			} else {
				log.Println(fmt.Sprintf("Bucket for table id %v created",table.Id))
			}
		} else {
			//			err = errors.New(fmt.Sprintf("Bucket for table id %v does not exist",table.Id))
			return
		}
	}

	return result, nil
}

func (da DataAccessType)columnBucket(tx *bolt.Tx, column *ColumnInfoType) (tableBucketResult, columnBucketResult *bolt.Bucket, err error) {
	if column == nil {
		err = ColumnInfoNotInitialized
		return
	}
	if !column.Id.Valid() {
		err = ColumnIdNotInitialized
		return
	}

	tableBucketResult, err = da.tableBucket(tx, column.TableInfo)
	if err != nil {
		return
	}
	if tableBucketResult == nil {
		return
	}

	columnBucketName := utils.Int64ToB8(column.Id.Value())
	columnsLabelBucket := tableBucketResult.Bucket(columnsLabel)
	if columnsLabelBucket == nil {
		if tx.Writable() {
			columnsLabelBucket, err = tableBucketResult.CreateBucket(columnsLabel)
			if err != nil {
				return
			}
			if columnsLabelBucket == nil {
				err = errors.New("Could not create predefined buclet \"columns\". Got empty value")
				return
			} else {
				log.Println("Bucket \"columns\" created")
			}
		} else {
			err = errors.New(fmt.Sprintf("Predefined bucket \"columns\" does not exist for table id %v ",column.TableInfo.Id))
			return
		}
	}

	columnBucketResult = columnsLabelBucket.Bucket(columnBucketName[:])
	if columnBucketResult == nil  {
		if tx.Writable() {
			columnBucketResult, err = columnsLabelBucket.CreateBucket(columnBucketName[:])
			if err != nil {
				return
			}
			if columnBucketResult == nil {
				err = errors.New(fmt.Sprintf("Could not create bucket for column id %v. Got empty value",column.Id))
				return
			} else {
				log.Println(fmt.Sprintf("Bucket for column id %v created",column.Id))
			}
		} else {
			//err = errors.New(fmt.Sprintf("Bucket for column id %v does not exist",column.Id))
			return
		}

	}
	return
}


type columnBucketsType struct {
	loaded bool
	columnBucket *bolt.Bucket
	supplementaryBucketCache *utils.Cache
}

func (da *DataAccessType) GetOrCreateColumnBuckets(tx *bolt.Tx, column *ColumnInfoType) (result *columnBucketsType){
	if raw, found := da.ColumnBucketsCache.Get(column.Id.Value()); !found {
		_, bucket, err := da.columnBucket(tx, column)
		if err != nil {
			panic(err)
		}
		result = &columnBucketsType{
			supplementaryBucketCache:utils.New(50),
			columnBucket: bucket,
		}
		da.ColumnBucketsCache.Add(column.Id.Value(),result)
		log.Println(fmt.Sprintf("append to cache bucket for column id %v",column.Id.Value()))
	} else {
		result = raw.(*columnBucketsType)
		result.loaded = true
	}
	return
}

func (cbs *columnBucketsType) GetOrCreateByCategory(dataCategory []byte) (result *columnSupplementaryBucketsType){
	if cbs.loaded {
		if raw,found := cbs.supplementaryBucketCache.Get(string(dataCategory)); found{
			result = raw.(*columnSupplementaryBucketsType)
		}
	}
	var appendToCache bool = false
	if result == nil{
		result = &columnSupplementaryBucketsType {}
		appendToCache = true
	}

	if result.dataCategoryBucket == nil{
		categoryBucket := cbs.columnBucket.Bucket(dataCategory)
		if categoryBucket != nil {
			result.dataCategoryBucket = categoryBucket
			result.bitsetBucket = categoryBucket.Bucket(bitsetBucketName)
			result.hashValuesBucket = categoryBucket.Bucket(hashValuesBucketName)
			result.hashStatsBucket = categoryBucket.Bucket(hashStatsName)
		} else {
			var err error
			categoryBucket, err = cbs.columnBucket.CreateBucket(dataCategory)
			if err != nil {
				panic(err)
			}
			result.dataCategoryBucket = categoryBucket
			result.bitsetBucket, err = categoryBucket.CreateBucket(bitsetBucketName)
			if err != nil {
				panic(err)
			}
			result.hashValuesBucket, err = categoryBucket.CreateBucket(hashValuesBucketName)
			if err != nil {
				panic(err)
			}
			result.hashStatsBucket, err = categoryBucket.CreateBucket(hashStatsName)
			if err != nil {
				panic(err)
			}
		}
	}
	if appendToCache {
		cbs.supplementaryBucketCache.Add(string(dataCategory), result)
	}
	return
}

func (da DataAccessType) SplitDataToBuckets(in scm.ChannelType, out scm.ChannelType) {
	//var currentTable *TableInfoType;
	var transactionCount uint64 = 0
	//	var emptyValue []byte = make([]byte, 0)
	hasher := fnv.New64()
	var storageTx *bolt.Tx = nil




	for raw := range in {
		switch val := raw.Get().(type) {
		case TableInfoType:
			/*if currentTable != nil {
				//		makeColumnBuckets()
			}
			currentTable = &val;*/
		case columnDataType:
			var hashUIntValue uint64
			category, bLen := val.buildDataCategory()
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
			columnBuckets := da.GetOrCreateColumnBuckets(storageTx,val.column)
			supplementary := columnBuckets.GetOrCreateByCategory(category[:])


			var hashBucket *bolt.Bucket
			hashBucket = supplementary.hashValuesBucket.Bucket(hValue[:])
			if hashBucket == nil {
				hashBucket, err = supplementary.hashValuesBucket.CreateBucket(hValue[:])
				if err != nil {
					panic(err)
				}
				if value, found := utils.B8ToUInt64(
					supplementary.hashStatsBucket.Get(
						hashStatsUnqiueCountName,
					),
				); !found {
					supplementary.hashStatsBucket.Put(
						hashStatsUnqiueCountName,
						utils.UInt64ToB8(1),
					)
				} else {
					value++
					supplementary.hashStatsBucket.Put(
						hashStatsUnqiueCountName,
						utils.UInt64ToB8(value),
					)
				}
			}

			//bitsetBucket.Get()

			baseUIntValue, offsetUIntValue := sparsebitset.OffsetBits(hashUIntValue)
			baseB8Value := utils.UInt64ToB8(baseUIntValue)
			//offsetB8Value := utils.UInt64ToB8(offsetUIntValue)
			bits, _ := utils.B8ToUInt64(supplementary.bitsetBucket.Get(baseB8Value))
			bits = bits | (1 << offsetUIntValue)
			supplementary.bitsetBucket.Put(baseB8Value, utils.UInt64ToB8(bits))

			hashBucket.Put(
				utils.UInt64ToB8(val.lineNumber),
				//TODO: switch to real file offset to column value instead of lineNumber
				utils.UInt64ToB8(val.lineNumber),
			)
			transactionCount++
			if transactionCount >= da.TransactionCountLimit {
				log.Println("commit... ")
				err = storageTx.Commit()
				if err != nil {
					panic(err)
				}
				storageTx = nil
				transactionCount = 0
				log.Println("Clear buckets cache...")
				da.ColumnBucketsCache.Clear()
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

}

type columnPairType struct{
	dataCategory []byte
	column1 *ColumnInfoType
	column2 *ColumnInfoType
	IntersectionCount uint64
	dataBucketName []byte
}

func (da DataAccessType) MakePairs(in, out scm.ChannelType) {

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
				pairs := make([]*columnPairType,0, len(tables1[tables1Index].Columns)*len(tables2[tables2Index].Columns))

				for _, column1 := range tables1[tables1Index].Columns {
					for _, column2 := range tables2[tables2Index].Columns {
						if column1.Id.Value() == column2.Id.Value() {
							continue
						}
						//fmt.Println(column1, column2)

						HashStorage.View(func(tx *bolt.Tx) error {
							_, bucket1, err := da.columnBucket(tx, column1)
							if err != nil {
								panic(err)
							}
							if bucket1 == nil {
								return nil
							}
							_, bucket2, err := da.columnBucket(tx, column2)
							if err != nil {
								panic(err)
							}
							if bucket2 == nil {
								return nil
							}

							bucket1.ForEach(func(dataCategory, value []byte) error {
								if stage == "CHAR" && dataCategory[0] == 0 {
								} else if stage == "NUMBER" && dataCategory[0] != 0 {
								} else {
									return nil
								}

								bucket := bucket2.Bucket(dataCategory)

								if bucket != nil {
									//fmt.Println("!",column1, column2, dataCategory, bucket)

									var pair = &columnPairType{}

									if column1.Id.Value() < column2.Id.Value() {
										pair.column1 = column1
										pair.column2 = column2
									} else {
										pair.column2 = column1
										pair.column1 = column2
									}

									pair.dataBucketName = make([]byte,8*2,8*2)
									b81 := utils.Int64ToB8(pair.column1.Id.Value())
									b82 := utils.Int64ToB8(pair.column2.Id.Value())
									copy(pair.dataBucketName,b81)
									copy(pair.dataBucketName[8:],b82)
									//fmt.Println(b81,b82)
									//fmt.Println(pair.dataBucketName)
									//fmt.Println()
									pair.dataCategory = dataCategory
									pairs = append(pairs, pair)
								}

								return nil
							})
							//fmt.Println("!")
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


func (da DataAccessType) BuildDataBitsets(in, out scm.ChannelType) {
	var storageTx *bolt.Tx
	var err error
	var emptyValue []byte = make([]byte, 0, 0)
	var transactionCount uint64 = 0

	writeIntersectingHashValue := func (columnPair *columnPairType, hValue uint64) {

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

		/*statsLabelBucket,err := categoryBucket.CreateBucketIfNotExists(dataIntersectionStatsLabel)
		if err != nil {
			panic(err)
		}*/
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
				pair *columnPairType,
				) {
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
					pair.IntersectionCount++
					writeIntersectingHashValue(pair,result)
					prev = result

				}
				rsh++
			}
			return nil
		})
		//out <- scm.NewMessageSize(1).Put("PAIR").PutN(0,pair)
	}

	for raw := range in {
		if storageTx == nil {
			storageTx, err = HashStorage.Begin(true)
			if err != nil {
				panic(err)
			}
			log.Println("open..")
		}
		switch val := raw.Get().(type) {
		case string:
			switch val {
			case "PAIR":
				pair := raw.GetN(0).(*columnPairType)
				var hashUniqueCount1, hashUniqueCount2 uint64
				buckets1 := da.GetOrCreateColumnBuckets(storageTx, pair.column1)
				dcBucket1 := buckets1.columnBucket.Bucket(pair.dataCategory);
				bsBucket1 := dcBucket1.Bucket(bitsetBucketName)
				statsBucket1 := dcBucket1.Bucket(hashStatsName)
				if statsBucket1 != nil {
					uqCount := statsBucket1.Get(hashStatsUnqiueCountName)
					if uqCount != nil {
						hashUniqueCount1, _ = utils.B8ToUInt64(uqCount)
					}
				}

				buckets2 := da.GetOrCreateColumnBuckets(storageTx, pair.column2)
				dcBucket2 := buckets2.columnBucket.Bucket(pair.dataCategory);
				bsBucket2 := dcBucket2.Bucket(bitsetBucketName)
				statsBucket2 := dcBucket2.Bucket(hashStatsName)
				if statsBucket2 != nil {
					uqCount := statsBucket2.Get(hashStatsUnqiueCountName)
					if uqCount != nil {
						hashUniqueCount2, _ = utils.B8ToUInt64(uqCount)
					}
				}
				//fmt.Println(pair.column1,pair.column2)
				if hashUniqueCount1 <= hashUniqueCount2 {
					pushHashValues(bsBucket1, bsBucket2, pair);
				} else {
					pushHashValues(bsBucket2, bsBucket1, pair);
				}
				if transactionCount >= da.TransactionCountLimit {
					log.Println("commit... ")
					err = storageTx.Commit()
					if err != nil {
						panic(err)
					}
					da.ColumnBucketsCache.Clear()
					storageTx = nil
					transactionCount = 0
				}
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







