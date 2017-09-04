package dataflow

import (
	"sparsebitset"
	"hash/fnv"
	"astra/B8"
	"strconv"
	"math"
	"strings"
	"encoding/binary"
	"github.com/goinggo/tracelog"
	"fmt"
	"context"
)

type ColumnDataType struct {
	Column       *ColumnInfoType
	DataCategory *DataCategoryType
	LineNumber   uint64
	LineOffset   uint64
	RawData      []byte
	RawDataLength int
	HashInt      uint64
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

func (columnData *ColumnDataType) DefineDataCategory() (simpleCategory *DataCategorySimpleType, err error) {
	//funcName := "ColumnDataType.DefineDataCategory"
//	tracelog.Completed(packageName, funcName)

	stringValue := strings.Trim(string(columnData.RawData), " ")

	var floatValue, truncatedFloatValue float64 = 0, 0
	var parseError error
	simpleCategory = &DataCategorySimpleType{ByteLength: columnData.RawDataLength}

	if len(stringValue) > 0 {
		floatValue, parseError = strconv.ParseFloat(stringValue, 64)
		if simpleCategory.IsNumeric = parseError == nil; simpleCategory.IsNumeric {
			columnData.HashInt = math.Float64bits(floatValue);
			truncatedFloatValue = math.Trunc(floatValue)
			simpleCategory.IsInteger = truncatedFloatValue == floatValue
			simpleCategory.IsNegative = floatValue < float64(0)
		}
	}

	dataCategoryKey := simpleCategory.Key()

	columnData.DataCategory, err = columnData.Column.CategoryByKey(
		dataCategoryKey,
		func() (result *DataCategoryType, err error) {
			result = simpleCategory.CovertToNullable()
			result.Key = dataCategoryKey;
			result.Stats.HashBitset = sparsebitset.New(0)
			return
		},
	)

	columnData.DataCategory.Stats.NonNullCount ++;

	if simpleCategory.IsNumeric {
		if columnData.DataCategory.Stats.MaxNumericValue < floatValue {
			columnData.DataCategory.Stats.MaxNumericValue = floatValue
		}
		if columnData.DataCategory.Stats.MinNumericValue > floatValue {
			columnData.DataCategory.Stats.MinNumericValue = floatValue
		}
		if simpleCategory.IsInteger {
			if columnData.DataCategory.Stats.IntegerBitset == nil {
				columnData.DataCategory.Stats.IntegerBitset = sparsebitset.New(0)
				}
				if simpleCategory.IsNegative {
					columnData.DataCategory.Stats.IntegerBitset.Set(uint64(-truncatedFloatValue));
				} else {
					columnData.DataCategory.Stats.IntegerBitset.Set(uint64(truncatedFloatValue));
				}
		}
	} else {
		if columnData.DataCategory.Stats.MaxStringValue < stringValue {
			columnData.DataCategory.Stats.MaxStringValue = stringValue
		}
		if columnData.DataCategory.Stats.MinStringValue > stringValue {
			columnData.DataCategory.Stats.MinStringValue = stringValue
		}
	}
	//tracelog.Completed(packageName, funcName)
	return simpleCategory, nil

}

func (columnData *ColumnDataType) HashData() (err error) {
	if columnData.RawDataLength > 0 {
		if !columnData.DataCategory.IsNumeric.Value() {
			if columnData.RawDataLength > B8.HashLength {
				var hashMethod = fnv.New64()
				hashMethod.Write(columnData.RawData)
				columnData.HashInt = hashMethod.Sum64()
			} else {
				columnData.HashInt = 0
				for dPos,dByte := range columnData.RawData {
					columnData.HashInt  = columnData.HashInt | uint64(dByte << (uint64(dPos)))
				}
			}
		}
	}

	columnData.DataCategory.Stats.HashBitset.Set(columnData.HashInt)

	return
}

func(column ColumnInfoType) BucketNameBytes(dataCategoryKey string) (result []byte, err error){
	if dataCategoryKey == "" {
		err = fmt.Errorf("Data category Key is empty!")
		return
	}
	if !column.Id.Valid() {
		err = fmt.Errorf("Column Id is empty!")
		return
	}
	keyLength := len( dataCategoryKey)
	result = make([]byte,binary.MaxVarintLen64 + keyLength)
	actual := binary.PutUvarint(result,uint64(column.Id.Value()))
	copy(result[actual:],[]byte( dataCategoryKey))
	result = result[:actual+keyLength]
	return
}


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

	bucket,err := currentTx.CreateBucketIfNotExists(bucketBytes)
	if err != nil{
		tracelog.Errorf(err,packageName,funcName,"Creating a BoltDB Bitset Bucket for table %v ",column.TableInfo.Id.Value())
		return  err
	}

	bsKvChan := dataCategory.Stats.HashBitset.KvChan(context.WithValue(context.Background(),"sort",true))
	for tuple := range(bsKvChan) {
		fval := math.Float64frombits(tuple[0])
		tracelog.Info(packageName,funcName,"%v:%v,%v",tuple[0],tuple[1],fval )
		keyBytes := make([]byte, binary.MaxVarintLen64)
		actual := binary.PutUvarint(keyBytes, tuple[0])
		keyBytes = keyBytes[:actual]

		valueBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(valueBytes, tuple[1])
		prevValueBytes := bucket.Get(keyBytes)
		if prevValueBytes != nil {
			for prevByteIndex, prevByteValue := range (prevValueBytes) {
				valueBytes[prevByteIndex] = valueBytes[prevByteIndex] | prevByteValue
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
}