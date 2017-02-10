package dataflow

import (
	"strconv"
	"strings"
)

type ColumnDataType struct {
	Column       *ColumnInfoType
	dataCategory *DataCategoryType
	LineNumber   uint64
	LineOffset   uint64
	RawData       []byte
}

type RowDataType struct{
	Table *TableInfoType
	LineNumber   uint64
	LineOffset   uint64
	RawData      [][]byte
}

func(dc *ColumnDataType) AnalyzeDataCategory() {
	byteLength := len(dc.RawData)
	if byteLength == 0 {
		return
	}
	stringValue := string(dc.RawData)

	floatValue, err := strconv.ParseFloat(stringValue, 64)
	simple := &DataCategorySimpleType{
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
		dc.Column.AnalyzeNumericValue(floatValue);


	}
	simple.SubHash = uint(0)
	if simple.IsSubHash {
		for _, bChar := range dc.RawData {
			if bChar > 0 {
				simple.SubHash = uint((uint8(37*simple.SubHash) + uint8(bChar)) & 0xff)
			}
		}
	}
	dc.Column.AnalyzeStringValue(stringValue)

	dataCategory := dc.Column.CategoryByKey(simple)
	if simple.IsNumeric{
		dataCategory.AnalyzeNumericValue(floatValue)
	}
	dataCategory.AnalyzeStringValue(stringValue)

}

