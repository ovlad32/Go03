package dataflow

import (
	"sparsebitset"
	"hash/fnv"
	"astra/B8"
	"strconv"
	"math"
	"strings"
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

func NewColumnData(column *ColumnInfoType,rawData []byte) (columnData *ColumnDataType) {
	rawDataLength := len(rawData)
	if rawDataLength > 0 {
		columnData = &ColumnDataType{
			RawData:       rawData,
			RawDataLength: rawDataLength,
			Column:        column,
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
			return
		},
	)
	//columnData.DataCategory = simpleCategory.CovertToNullable()

	columnData.DataCategory.Stats.NonNullCount ++;

	if simpleCategory.IsNumeric {
		if columnData.DataCategory.Stats.MaxNumericValue < floatValue {
			columnData.DataCategory.Stats.MaxNumericValue = floatValue
		}
		if columnData.DataCategory.Stats.MinNumericValue > floatValue {
			columnData.DataCategory.Stats.MinNumericValue = floatValue
		}
		if simpleCategory.IsInteger {
			if !simpleCategory.IsNegative {
				if columnData.Column.NumericPositiveBitset == nil {
					columnData.Column.NumericPositiveBitset = sparsebitset.New(0)
				}
				columnData.Column.NumericPositiveBitset.Set(uint64(truncatedFloatValue));
			} else {
				if columnData.Column.NumericNegativeBitset == nil {
					columnData.Column.NumericNegativeBitset = sparsebitset.New(0)
				}
				columnData.Column.NumericNegativeBitset.Set(uint64(-truncatedFloatValue))
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
	return
}
