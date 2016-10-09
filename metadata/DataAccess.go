package metadata

import (
	scm "./../scm"
	utils "./../utils"
	sparsebitset "./../sparsebitset"
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
)

const hashLength = 8

type B5Type [5]byte

var HashStorage *bolt.DB

func tableBucket(tx *bolt.Tx, table *TableInfoType) (result *bolt.Bucket, err error) {
	var tablesLabel = []byte("tables")
	tableBucketName  := utils.Int64ToB8(table.Id.Int64)
	tablesLabelBucket := tx.Bucket(tablesLabel)
	if tablesLabelBucket == nil {
		tablesLabelBucket, err = tx.CreateBucket(tablesLabel)
		if err != nil {
			return
		}
	}

	result = tablesLabelBucket.Bucket(tableBucketName[:])
	if result == nil {
		result, err = tablesLabelBucket.CreateBucket(tableBucketName[:])
		if err != nil {
			return
		}
	}
	return result, nil
}

func columnBucket(tx *bolt.Tx, column *ColumnInfoType) (tableBucketResult, columnBucketResult *bolt.Bucket, err error) {
	tableBucketResult,err = tableBucket(tx,column.TableInfo)
	if err != nil {
		return
	}
	var columnsLabel = []byte("columns")
	columnBucketName := utils.Int64ToB8(column.Id.Int64)
	columnsLabelBucket := tableBucketResult.Bucket(columnsLabel)
	if columnsLabelBucket == nil {
		columnsLabelBucket, err = tableBucketResult.CreateBucket(columnsLabel)
		if err != nil {
			return
		}
	}

	columnBucketResult = columnsLabelBucket.Bucket(columnBucketName[:])
	if columnBucketResult == nil {
		columnBucketResult, err = columnsLabelBucket.CreateBucket(columnBucketName[:])
		if err != nil {
			return
		}
	}
	return
}


type DataAccessType struct {
	DumpConfiguration DumpConfigurationType
	TransactionCountLimit uint64
}

type columnDataType struct {
	column     *ColumnInfoType
	lineNumber uint64
	bValue     []byte
	nValue     float64
	isNumeric  bool
	isFloat    bool
	isNegative bool
}

func (c columnDataType) buildDataCategory() (result B5Type, bLen uint64) {
	if c.isNumeric {
		result[0] = 1 << 3
		if c.isFloat {
			if c.isNegative {
				result[0] = result[0] | 1<<0
			}
		} else {
			result[0] = result[0] | 1<<1
			if c.isNegative {
				result[0] = result[0] | 1<<0
			}
		}
	}
	bLen = uint64(len(c.bValue))
	binary.PutUvarint(result[1:], bLen)
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
	var s0d = []byte{0x0D}

	//	lineSeparatorArray[0] = da.DumpConfiguration.LineSeparator

	for raw := range in {
		var source *TableInfoType
		switch val := raw.Get().(type) {
		case *TableInfoType:
			source = val
		default:
			panic(fmt.Sprintf("Type is not expected %T", raw.Get()))
		}
		gzfile, err := os.Open(da.DumpConfiguration.DumpBasePath + source.PathToFile.String)
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
			//line, err := rawData.ReadString(byte(da.DumpConfiguration.LineSeparator))
			line, err := rawData.ReadSlice(da.DumpConfiguration.LineSeparator)
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}
			lineNumber++

			line = bytes.TrimSuffix(line, []byte{da.DumpConfiguration.LineSeparator})
			line = bytes.TrimSuffix(line, s0d)

			//            fmt.Print(line)
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

				if len(val.bValue) == 0 {
					continue
				}

				sValue := string(val.bValue)
				if column.MaxStringValue.String < sValue {
					column.MaxStringValue.String = sValue
					column.MaxStringValue.Valid = true
				}
				if column.MinStringValue.String > sValue {
					column.MinStringValue.String = sValue
					column.MinStringValue.Valid = true
				}

				lValue := int64(len(sValue))
				if column.MaxStringLength.Int64 < lValue {
					column.MaxStringLength.Int64 = lValue
					column.MaxStringLength.Valid = true
				}
				if column.MinStringLength.Int64 > lValue {
					column.MinStringLength.Int64 = lValue
					column.MinStringLength.Valid = true
				}
				var err error
				val.nValue, err = strconv.ParseFloat(sValue, 64)

				val.isNumeric = err == nil

				if val.isNumeric {
					val.isFloat = strings.Contains(sValue, ".")
					val.isNegative = strings.HasPrefix(sValue, "-")
					column.NumericCount.Int64++
					if column.MaxNumericValue.Float64 < val.nValue {
						column.MaxNumericValue.Float64 = val.nValue
						column.MaxNumericValue.Valid = true
					}
					if column.MinNumericValue.Float64 > val.nValue {
						column.MinNumericValue.Float64 = val.nValue
						column.MinNumericValue.Valid = true
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
			var hashUIntValue uint64;
			category, bLen := val.buildDataCategory()
			var hValue []byte
			if bLen > hashLength {
				hasher.Reset()
				hasher.Write(val.bValue)
				hashUIntValue = hasher.Sum64()
				hValue = utils.UInt64ToB8(hashUIntValue)
			} else {
				hValue = make([]byte,hashLength)
				for index := uint64(0); index < bLen; index++ {
					hValue[index] = val.bValue[bLen-index-1]
				}
				hashUIntValue,_ = utils.B8ToUInt64(hValue)
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
			_, columnBucket, err := columnBucket(storageTx, val.column)
			if err != nil {
				panic(err)
			}

			var bitsetBucketName = []byte("bitset")
			var hashValuesBucketName = []byte("hashValues")

			var hashStatsName = []byte("hashStats")
			var hashStatsUnqiueCountName = []byte("uniqueCount")

			var dataCategoryBucket *bolt.Bucket
			var bitsetBucket *bolt.Bucket
			var hashValuesBucket *bolt.Bucket
			var hashStatsBucket *bolt.Bucket
			_ = bitsetBucket
			dataCategoryBucket = columnBucket.Bucket(category[:])
			if dataCategoryBucket == nil {
				dataCategoryBucket, err = columnBucket.CreateBucket(category[:])
				if err != nil {
					panic(err)
				}
				bitsetBucket, err = dataCategoryBucket.CreateBucket(bitsetBucketName)
				if err != nil {
					panic(err)
				}
				hashValuesBucket, err = dataCategoryBucket.CreateBucket(hashValuesBucketName)
				if err != nil {
					panic(err)
				}
				hashStatsBucket, err = dataCategoryBucket.CreateBucket(hashStatsName)
				if err != nil {
					panic(err)
				}
			} else {
				bitsetBucket = dataCategoryBucket.Bucket(bitsetBucketName)
				hashValuesBucket = dataCategoryBucket.Bucket(hashValuesBucketName)
				hashStatsBucket = dataCategoryBucket.Bucket(hashStatsName)
			}

			var hashBucket *bolt.Bucket
			hashBucket = hashValuesBucket.Bucket(hValue[:])
			if hashBucket == nil {
				hashBucket, err = hashValuesBucket.CreateBucket(hValue[:])
				if err != nil {
					panic(err)
				}
				hashStatsBucket.Put(
					hashStatsUnqiueCountName,
					utils.UInt64ToB8(1),
				)
			} else {

				if value, found := utils.B8ToUInt64(
					hashStatsBucket.Get(
						hashStatsUnqiueCountName,
					),
				); !found {
					hashStatsBucket.Put(
						hashStatsUnqiueCountName,
						utils.UInt64ToB8(1),
					)
				} else {
					value++
					//fmt.Println(hValue,value)
					hashStatsBucket.Put(
						hashStatsUnqiueCountName,
						utils.UInt64ToB8(value),
					)
				}
			}

			//bitsetBucket.Get()


			baseUIntValue,offsetUIntValue := sparsebitset.OffsetBits(hashUIntValue)
			baseB8Value := utils.UInt64ToB8(baseUIntValue)
			//offsetB8Value := utils.UInt64ToB8(offsetUIntValue)
			bits,_ := utils.B8ToUInt64(bitsetBucket.Get(baseB8Value))
			bits = bits | 1 << offsetUIntValue
			bitsetBucket.Put(baseB8Value,utils.UInt64ToB8(bits))

			hashBucket.Put(
				utils.UInt64ToB8(val.lineNumber),
				//TODO: switch to real file offset to column value instead of lineNumber
				utils.UInt64ToB8(val.lineNumber),
			)

			transactionCount++
			if transactionCount >= da.TransactionCountLimit {
				println("commit 1")
				err = storageTx.Commit()
				if err != nil {
					panic(err)
				}
				storageTx = nil
				transactionCount = 0
			}
		}
	}
	if storageTx != nil {
		println("commit 2")
		err := storageTx.Commit()
		if err != nil {
			panic(err)
		}
		storageTx = nil
	}

	close(out)

}


func (da DataAccessType) MakePairs(in,out scm.ChannelType) {
	for raw := range in {
		fmt.Println(raw.Get())
		switch val := raw.Get().(type) {
		case string:
			if val == "2MD" {
				md1 := raw.GetN(0).(*MetadataType)
				md2 := raw.GetN(1).(*MetadataType)
				tables1, err := H2.TableInfoByMetadata(md1)
				fmt.Println(md1)
				fmt.Println(md2)

				if err != nil {
					panic(err)
				}
				tables2, err := H2.TableInfoByMetadata(md2)
				if err != nil {
					panic(err)
				}
				for tables1Index := range tables1 {
					for tables2Index := range tables2 {
						for _,column1 := range tables1[tables1Index].Columns {
							for _,column2 := range tables1[tables2Index].Columns {
								if column1.Id.Int64 == column2.Id.Int64 {
									continue
								}
								out<-scm.NewMessageSize(2).
									Put("2CL").
									PutN(0,column1).
									PutN(1,column2)
							}
						}
					}
				}
			}

		}
	}
	close(out)
}

func (da DataAccessType) NarrowPairCategories(in,out scm.ChannelType) {
	for raw := range in {
		switch val := raw.Get().(type) {
		case string:
			if val == "2CL" {
				cl1 := raw.GetN(0).(*ColumnInfoType)
				cl2 := raw.GetN(1).(*ColumnInfoType)
				HashStorage.View(func(tx *bolt.Tx) error{
					_,c1Bucket,_:= columnBucket(tx,cl1)
					if c1Bucket == nil {
						return 	nil
					}
					_,c2Bucket,_ := columnBucket(tx,cl2)
					if c2Bucket == nil {
						return nil
					}

					c1Bucket.ForEach(func(key,value []byte) error {
						bucket := c2Bucket.Bucket(key)
						if bucket != nil {
							fmt.Println(cl1,cl2,key)
						}
						return nil
					})
					return nil
				})

			}

		}
	}
	close(out)
}
