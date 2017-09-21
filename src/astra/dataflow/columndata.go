package dataflow

import (
	"astra/B8"
	"fmt"
	"github.com/goinggo/tracelog"
	"hash/fnv"
	"io/ioutil"
	"math"
	"os"
	"sparsebitset"
	"strconv"
	"strings"
)

type ColumnDataType struct {
	Column        *ColumnInfoType
	DataCategory  *DataCategoryType
	LineNumber    uint64
	RawData       []byte
	RawDataLength int
	HashData      uint64
}

func (c *ColumnInfoType) NewColumnData(rawData []byte) (columnData *ColumnDataType) {
	rawDataLength := len(rawData)
	if rawDataLength > 0 {
		columnData = &ColumnDataType{
			RawData:       rawData,
			RawDataLength: rawDataLength,
			Column:        c,
		}
	}
	return
}

func (columnData *ColumnDataType) DiscoverDataCategory() (simpleCategory *DataCategorySimpleType, err error) {

	stringValue := strings.Trim(string(columnData.RawData), " ")

	var floatValue, truncatedFloatValue float64 = 0, 0
	var parseError error
	simpleCategory = &DataCategorySimpleType{ByteLength: columnData.RawDataLength}

	if len(stringValue) > 0 {
		floatValue, parseError = strconv.ParseFloat(stringValue, 64)
		if simpleCategory.IsNumeric = parseError == nil; simpleCategory.IsNumeric {
			columnData.HashData = math.Float64bits(floatValue)
			truncatedFloatValue = math.Trunc(floatValue)
			simpleCategory.IsInteger = truncatedFloatValue == floatValue
			simpleCategory.IsNegative = floatValue < float64(0)
		}
	}

	dataCategoryKey := simpleCategory.Key()

	columnData.DataCategory, err = columnData.Column.CategoryByKey(
		dataCategoryKey,
		func() (result *DataCategoryType, err error) {
			result = simpleCategory.NewDataCategory()
			result.Key = dataCategoryKey
			result.Column = columnData.Column
			result.Stats.HashBitset = sparsebitset.New(0)
			return
		},
	)

	columnData.DataCategory.Stats.NonNullCount++

	if simpleCategory.IsNumeric {
		if columnData.DataCategory.Stats.MaxNumericValue < floatValue {
			columnData.DataCategory.Stats.MaxNumericValue = floatValue
		}
		if columnData.DataCategory.Stats.MinNumericValue > floatValue {
			columnData.DataCategory.Stats.MinNumericValue = floatValue
		}
		if simpleCategory.IsInteger {
			if columnData.DataCategory.Stats.ItemBitset == nil {
				columnData.DataCategory.Stats.ItemBitset = sparsebitset.New(0)
			}
			if simpleCategory.IsNegative {
				columnData.DataCategory.Stats.ItemBitset.Set(uint64(-truncatedFloatValue))
			} else {
				columnData.DataCategory.Stats.ItemBitset.Set(uint64(truncatedFloatValue))
			}
		}
	} else {
		if columnData.DataCategory.Stats.ItemBitset == nil {
			columnData.DataCategory.Stats.ItemBitset = sparsebitset.New(0)
		}
		for _, charValue := range stringValue {
			columnData.DataCategory.Stats.ItemBitset.Set(uint64(charValue))
		}
		if columnData.DataCategory.Stats.MaxStringValue == "" ||
			columnData.DataCategory.Stats.MaxStringValue < stringValue {
			columnData.DataCategory.Stats.MaxStringValue = stringValue
		}
		if columnData.DataCategory.Stats.MinStringValue == "" ||
			columnData.DataCategory.Stats.MinStringValue > stringValue {
			columnData.DataCategory.Stats.MinStringValue = stringValue
		}
	}
	return simpleCategory, nil

}

func (columnData *ColumnDataType) Encode() (err error) {
	if columnData.RawDataLength > 0 {
		if !columnData.DataCategory.IsNumeric.Value() {
			if columnData.RawDataLength > B8.HashLength {
				var hashMethod = fnv.New64()
				hashMethod.Write(columnData.RawData)
				columnData.HashData = hashMethod.Sum64()
			} else {
				columnData.HashData = 0
				for dPos, dByte := range columnData.RawData {
					columnData.HashData = columnData.HashData | uint64(dByte<<(uint64(dPos)))
				}
			}
		}
	}

	columnData.DataCategory.Stats.HashBitset.Set(columnData.HashData)

	return
}

func (c ColumnInfoType) IndexFileExists(baseDir string) (result bool, err error) {
	funcName := "ColumnInfoType.IndexFileExists"
	tracelog.Started(packageName, funcName)

	fileMask := fmt.Sprintf("%v%v%v.*.bitset", baseDir, os.PathSeparator, c.Id.Value())

	files, err := ioutil.ReadDir(fileMask)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Cannot list files with mask %v", fileMask)
		return false, err
	}

	for _, f := range files {
		if !f.IsDir() {
			return true, nil
		}
	}
	tracelog.Completed(packageName, funcName)
	return false, nil
}

/*
func (column *ColumnInfoType) FlushBitset(dataCategory *DataCategoryType) (err error) {
	funcName := "ColumnDataType.WriteHashData"


	bucketBytes,err  := column.BucketNameBytes(dataCategory.Key)
	if err != nil{
		tracelog.Errorf(err,packageName,funcName,
			"Creating a BoltDB Bitset Bucket Name for table/category %v/%v ",
			column.TableInfo.Id.Value(),
			dataCategory.Key,
		)
		return  err
	}


	currentTx, err := column.TableInfo.bitSetStorage.Begin(true);
	if err != nil {
		tracelog.Errorf(err,packageName,funcName,"Opening a BoltDB transaction for table %v ",column.TableInfo.Id.Value())
		return  err
	}

	bucket  := currentTx.Bucket(bucketBytes)

	bucketAlreadyCreated := bucket != nil
	if !bucketAlreadyCreated  {
		bucket,err = currentTx.CreateBucketIfNotExists(bucketBytes)
		if err != nil{
			tracelog.Errorf(err,packageName,funcName,"Creating a BoltDB Bitset Bucket for table %v ",column.TableInfo.Id.Value())
			return  err
		}
	}


	bsKvChan := dataCategory.Stats.HashBitset.KvChan(context.WithValue(context.Background(),"sort",true))
	for tuple := range(bsKvChan) {
		keyBytes := make([]byte, binary.MaxVarintLen64)
		actual := binary.PutUvarint(keyBytes, tuple[0])
		keyBytes = keyBytes[:actual]

		valueBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, tuple[1])
		if bucketAlreadyCreated {
			prevValueBytes := bucket.Get(keyBytes)
			if prevValueBytes != nil {
				for prevByteIndex, prevByteValue := range (prevValueBytes) {
					valueBytes[prevByteIndex] = valueBytes[prevByteIndex] | prevByteValue
				}
			}
		}
		err = bucket.Put(keyBytes, valueBytes)
		if err != nil{
			tracelog.Errorf(err,packageName,funcName,"Writing a hash code into BoltDB for table %v ",column.TableInfo.Id.Value())
			return  err
		}
	}
	err = currentTx.Commit();
	if err != nil{
		tracelog.Errorf(err,packageName,funcName,"Committing BS data into BoltDB for table %v ",column.TableInfo.Id.Value())
		return  err
	}
	tracelog.Info(packageName,funcName,"BS data of column %v.%v.%v/%v has been persisted",column.TableInfo.SchemaName,column.TableInfo.TableName,column.ColumnName,dataCategory.Key)
   return
}*/
