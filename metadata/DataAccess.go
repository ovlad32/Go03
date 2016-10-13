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

}

type columnDataType struct {
	column     *ColumnInfoType
	lineNumber uint64
	bValue     []byte
	nValue     float64
	isNumeric  bool
	fpScale    int8
	isNegative bool
	bStatsMean uint8
	bStatsStdv uint8
	bStatsMin uint8
	bStatsMax uint8
}



func (c columnDataType) buildDataCategory() (result []byte, bLen uint16) {
	result = make([]byte, 3, 4)
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
	} else {
		result = append(
			result,
			byte(c.bStatsMean),
		)/*,
			byte(c.bStatsMean),
			byte(c.bStatsMax),
			byte(c.bStatsMin),
		)*/
	}
	return
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
				} else {
					val.bStatsMax = 1
					val.bStatsMin = 0xFF
					var bSum float64 = 0
					var bCount int = 0
					for _,bChar := range val.bValue{
						if bChar>0 {
							bSum = bSum + float64(bChar)
							bCount ++
						}
					}
					fStatsMean := bSum/float64(bCount)
					val.bStatsMean = uint8(fStatsMean)

					/*bSum = 0
					for _,bChar := range val.bValue{
						if bChar>0 {
							res := fStatsMean - float64(bChar)
							bSum = bSum + res*res
						}
					}
					val.bStatsStdv = uint8(math.Sqrt(bSum))
					*/
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
        - partition#(byte) (b)
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
	if result == nil{
		result = &columnSupplementaryBucketsType {}
		cbs.supplementaryBucketCache.Add(string(dataCategory), result)
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
			colunmBuckets := da.GetOrCreateColumnBuckets(storageTx,val.column)
			supplementary := colunmBuckets.GetOrCreateByCategory(category[:])


			var hashBucket *bolt.Bucket
			hashBucket = supplementary.hashValuesBucket.Bucket(hValue[:])
			if hashBucket == nil {
				hashBucket, err = supplementary.hashValuesBucket.CreateBucket(hValue[:])
				if err != nil {
					panic(err)
				}
				supplementary.hashStatsBucket.Put(
					hashStatsUnqiueCountName,
					utils.UInt64ToB8(1),
				)
			} else {

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
					//fmt.Println(hValue,value)
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
			bits = bits | 1<<offsetUIntValue
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
	dataBucketName string
}

func (da DataAccessType) MakePairs(in, out scm.ChannelType) {
	for raw := range in {
		fmt.Println(raw.Get())
		switch val := raw.Get().(type) {
		case string:
			if val == "2MD" {
				md1 := raw.GetN(0).(*MetadataType)
				md2 := raw.GetN(1).(*MetadataType)
				tables1, err := H2.TableInfoByMetadata(md1)


				if err != nil {
					panic(err)
				}
				tables2, err := H2.TableInfoByMetadata(md2)
				if err != nil {
					panic(err)
				}
				fmt.Println(md1)
				fmt.Println(md2)
				HashStorage.View(func(tx *bolt.Tx) error {
					for tables1Index := range tables1 {
						for tables2Index := range tables2 {
							for _, column1 := range tables1[tables1Index].Columns {
								for _, column2 := range tables2[tables2Index].Columns {
									if column1.Id.Value() == column2.Id.Value() {
										continue
									}
									//fmt.Println(column1, column2)

									_, bucket1, err := da.columnBucket(tx, column1)
									if err != nil {
										panic(err)
									}
									if bucket1 == nil {
										continue
									}
									_, bucket2, err := da.columnBucket(tx, column2)
									if err != nil {
										panic(err)
									}
									if bucket2 == nil {
										continue
									}

									bucket1.ForEach(func(dataCategory, value []byte) error {
										bucket := bucket2.Bucket(dataCategory)

										if bucket != nil {
											//fmt.Println(column1, column2, key, bucket)

											var pair = &columnPairType{}

											if column1.Id.Value() < column2.Id.Value() {
												pair.column1 = column1
												pair.column2 = column2
											} else {
												pair.column2 = column1
												pair.column1 = column2
											}

											pair.dataBucketName = fmt.Sprintf("%v-%v",pair.column1.Id.Value(),pair.column2.Id.Value())
											pair.dataCategory = dataCategory

											out <- scm.NewMessage().Put(pair)
										}

										return nil
									})
								}
							}
						}
					}
					return nil
				})

			}

		}
	}
	close(out)
}

func (da DataAccessType) NarrowPairCategories(in, out scm.ChannelType) {
	var storageTx *bolt.Tx
	var err error
	for raw := range in {
		switch val := raw.Get().(type) {
		case *columnPairType:
				fmt.Println(val.column1,val.column2,val.dataCategory)
				if storageTx == nil {
					storageTx,err = HashStorage.Begin(false)
					if err != nil {
						panic(err)
					}

				}

				buckets1 := da.GetOrCreateColumnBuckets(storageTx, val.column1)
				dcBucket1 := buckets1.columnBucket.Bucket(val.dataCategory);
				bsBucket1 := dcBucket1.Bucket(bitsetBucketName)

				buckets2 := da.GetOrCreateColumnBuckets(storageTx, val.column1)
				dcBucket2 := buckets2.columnBucket.Bucket(val.dataCategory);
				bsBucket2 := dcBucket2.Bucket(bitsetBucketName)

				bsBucket1.ForEach(func (keyBytes,valueBytes1 []byte) (error) {
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
							val.IntersectionCount++
							out <- scm.NewMessageSize(2).Put("DH").PutN(0,val).PutN(result)
							prev = result
						}
						rsh++
					}
					out <- scm.NewMessageSize(2).Put("PAIR").PutN(0,val).PutN(result)
					return nil
				})
			}
		}

	if storageTx != nil {
		storageTx.Rollback()
	}

	close(out)
}

var  dataIntersectionLabel []byte = []byte("dataIntersection")
var  dataIntersectionStatsLabel []byte = []byte("stats")


func (da DataAccessType) WriteDataBitset(in, out scm.ChannelType) {
	var storateTx *bolt.Tx
	var err error
	for raw := range in {
		if storateTx == nil{
			storateTx,err = HashStorage.Begin(true)
			if err != nil {
				panic(err)
			}
		}
		switch sVal := raw.Get().(type) {
		case string:
			switch sVal {
			case "DH":
				var columnPair = raw.GetN(0).(*columnPairType)
				var hValue uint64 = raw.GetN(0).(uint64)

				labelBucket,err := storateTx.CreateBucketIfNotExists(dataIntersectionLabel)
				if err != nil {
					panic(err)
				}

				pairBucket,err := labelBucket.CreateBucketIfNotExists([]byte(columnPair.dataBucketName))
				if err != nil {
					panic(err)
				}

				categoryBucket, err := pairBucket.CreateBucketIfNotExists([]byte(columnPair.dataCategory))
				if err != nil {
					panic(err)
				}

				hValueBucket,err := categoryBucket.CreateBucketIfNotExists(hValue)
				if err != nil {
					panic(err)
				}

				_ = hValueBucket


			case "PAIR":


			}
		}
