package dataflow

import (
	"strconv"
	"strings"
	"context"
)

type ColumnDataType struct {
	Column       *ColumnInfoType
	dataCategoryKey  string
	dataCategory *DataCategoryType
	LineNumber   uint64
	LineOffset   uint64
	RawData      []byte
	RawDataLength int
	HashValue    []byte
}

type RowDataType struct{
	Table *TableInfoType
	LineNumber   uint64
	LineOffset   uint64
	//auxDataBuffer []byte
	RawData      [][]byte
}

func(cd *ColumnDataType) StoreByDataCategory(ctx context.Context,storagePath string) {
	if cd.RawDataLength == 0 {
		return
	}
	stringValue := string(cd.RawData)

	floatValue, err := strconv.ParseFloat(stringValue, 64)
	simple := &DataCategorySimpleType{
		ByteLength: cd.RawDataLength,
		IsNumeric : err == nil,
		IsSubHash : false , //byteLength > da.SubHashByteLengthThreshold
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

		simple.IsNegative = floatValue<float64(0)

		//TODO:REDESIGN THIS!
		//cd.Column.AnalyzeNumericValue(floatValue);
	}
	//TODO:REDESIGN THIS!
	//cd.Column.AnalyzeStringValue(floatValue);

	simple.SubHash = uint(0)
	if simple.IsSubHash {
		for _, bChar := range cd.RawData {
			if bChar > 0 {
				simple.SubHash = uint((uint8(37*simple.SubHash) + uint8(bChar)) & 0xff)
			}
		}
	}


	dataCategory,newOne := cd.Column.CategoryByKey(ctx, simple)
	if newOne {
		dataCategory.RunAnalyzer(ctx)
		dataCategory.RunStorage(ctx, storagePath, cd.Column.Id.Value())
	}
	_=dataCategory
	select {
	case <-ctx.Done():
		return
		case dataCategory.stringAnalysisChan <- stringValue:
	}

	if simple.IsNumeric{
		select {
		case <-ctx.Done():
			return
			case dataCategory.numericAnalysisChan <-floatValue:
		}
	}

	/*select {
	case dataCategory.columnDataChan <- cd:
	case <-ctx.Done():
		return
	}*/

}

