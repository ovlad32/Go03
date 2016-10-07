package metadata

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	scm "./../scm"
	"strconv"
	"strings"
	"github.com/boltdb/bolt"
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"github.com/constabulary/gb/testdata/src/c"
)

const hashLength = 8
type ColumnBucketNameType [8]byte
type TableBucketNameType [8]byte
type TypedColumnBucketType [5]byte

var HashStorage *bolt.DB
var columns = []byte("columns")

func tableBucket(tx * bolt.Tx, table *TableInfoType) (result *bolt.Bucket,err error) {
	var tableBucketName TableBucketNameType
	var tables = []byte("tables")
	binary.BigEndian.PutUint64(tableBucketName[:],table.Id.Int64)
	tablesBucket := tx.Bucket(tables)
	if tablesBucket == nil {
		tablesBucket,err = tx.CreateBucket(tables)
		if err != nil {
			return
		}
	}

	result = tablesBucket.Bucket(tableBucketName[:])
	if result == nil {
		result,err = tablesBucket.CreateBucket(tableBucketName[:])
		if err != nil {
			return
		}
	}
	return result, nil
}

func columnBucket(tableIdBucket * bolt.Bucket, column *TableInfoType) (result *bolt.Bucket,err error) {
	var columnBucketName ColumnBucketNameType
	var columns = []byte("columns")
	binary.BigEndian.PutUint64(columnBucketName[:],column.Id.Int64)
	columnsBucket := tableIdBucket.Bucket(columns)
	if columnsBucket == nil {
		columnsBucket,err = tableIdBucket.CreateBucket(columns)
		if err != nil {
			return
		}
	}

	result = columnsBucket.Bucket(columnBucketName[:])
	if result == nil {
		result,err = columnsBucket.CreateBucket(columnBucketName[:])
		if err != nil {
			return
		}
	}
	return result, nil
}

type DataAccessType struct {
	DumpConfiguration DumpConfigurationType
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




func (c columnDataType) buildDataCategory() (result TypedColumnBucketType, bLen uint64) {
	if c.isNumeric {
		result[0] = 1 << 3
		if c.isFloat {
			if c.isNegative {
				result[0] = result[0] | 1 << 0
			}
		} else {
			result[0] = result[0] | 1 << 1
			if c.isNegative {
				result[0] = result[0]| 1 << 0
			}
		}
	}
	bLen = uint64(len(c.bValue))
	binary.PutUvarint(result[1:],bLen)
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
	var s0d  = []byte{0x0D}

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
        - partition(byte) (b)
         - "bitset" (+b)
           -offset/bit(k/v)
         - "hash" (+b)
           - hash/value (b)
            - row#/position (k/v)
*/

func (da DataAccessType) SplitDataToBuckets(in scm.ChannelType, out scm.ChannelType) {
	var currentTable *TableInfoType;
	const transactionLimit = 10000;
	var transactionLength int = 0;
	var emptyValue []byte = make([]byte,0)
	hasher := fnv.New64()
	var storageTx *bolt.Tx = nil;


	for raw := range in {
		switch val := raw.Get().(type) {
		case TableInfoType:
			if currentTable != nil {
				//		makeColumnBuckets()
			}
			currentTable = &val;
		case columnDataType:
			category, bLen := val.buildDataCategory()
			hValue := make([]byte, hashLength)
			if bLen > hashLength {
				hasher.Reset();
				hasher.Write(val.bValue)
				binary.BigEndian.PutUint64(hValue, hasher.Sum64())
			} else {
				for index := uint64(0); index < bLen; index++ {
					hValue[index] = val.bValue[bLen - index - 1]
				}
			}
			//val.column.DataCategories[category] = true
			var err error

			if storageTx == nil {
				storageTx, err = HashStorage.Begin(true)
				if err != nil {
					panic(err)

				}
			}
			tableBucket,err  := tableBucket(storageTx,val.column.TableInfo)
			if err != nil {
				panic(err)
			}
			columnBucket,err  := columnBucket(tableBucket,val.column)
			if err != nil {
				panic(err)
			}


			var dataCategoryBucket *bolt.Bucket
			dataCategoryBucket = columnBucket.Bucket(category[:])
			if dataCategoryBucket == nil {
				dataCategoryBucket, err = columnBucket.CreateBucket(category[:])
				if err != nil {
					panic(err)
				}
			}


			value := dataCategoryBucket.Get(hValue[:])
			if value == nil {
				dataCategoryBucket.Put(hValue[:], emptyValue)
			}

			partition := hValue[0]

			var partitionBucket *bolt.Bucket
			partitionBucket = dataCategoryBucket.Bucket([]byte{partition})
			if partitionBucket == nil {
				partitionBucket, err = dataCategoryBucket.CreateBucket([]byte{partition})
				if err != nil {
					panic(err)
				}
			}

			var hValueBucket *bolt.Bucket
			hValueBucket = partitionBucket.Bucket(hValue[:])
			if hValueBucket == nil {
				hValueBucket, err = partitionBucket.CreateBucket(hValue[:])
				if err != nil {
					panic(err)
				}
			}
			bRow := make([]byte, 8)
			bDumpOffset := make([]byte, 8)
			binary.BigEndian.PutUint64(bRow, val.lineNumber)
			//TODO: switch to real offset instead of lineNumber
			binary.BigEndian.PutUint64(bDumpOffset, val.lineNumber)
			hValueBucket.Put(bRow, bDumpOffset)

			transactionLength ++
			if transactionLength >= transactionLimit {
				//println("commit 1")
				err = storageTx.Commit();
				if err != nil {
					panic(err)
				}
				storageTx = nil;
				transactionLength = 0
			}
		}
	}
	if storageTx != nil {
		println("commit 2")
		err := storageTx.Commit();
		if err != nil {
			panic(err)
		}
		storageTx = nil;
	}

	close(out)

}
/*
func (da DataAccessType) makePairs(in,out scm.ChannelType) {
	for raw := range in {
		switch val := raw.Get().(type) {
		case string:
			if val == "2MD" {
				md1 := raw.GetN(0).(MetadataType)
				md2 := raw.GetN(1).(MetadataType)
				tables1, err := H2.TableInfoByMetadata(md1)
				if err != nil {

				}
				tables2, err := H2.TableInfoByMetadata(md2)
				if err != nil {

				}
				for tables1Index := range tables1 {
					for tables2Index := range tables2 {

					}
				}
			}

		}
	}
	close(out)

}
*/