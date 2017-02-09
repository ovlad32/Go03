package dataflow

import (
	"astra/metadata"
	"io"
	"encoding/binary"
	"strconv"
	"strings"
	"astra/nullable"
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
	var err error
	var floatValue float64
	var fpScale int
	var isNegative bool

	stringValue := string(dc.RawData)

	floatValue, err = strconv.ParseFloat(stringValue, 64)
	isNumeric := err == nil

	isSubHash := false //byteLength > da.SubHashByteLengthThreshold

	if isNumeric {
		//var lengthChanged bool
		if strings.Count(stringValue, ".") == 1 {
			//trimmedValue := strings.TrimLeft(stringValue, "0")
			//lengthChanged = false && (len(stringValue) != len(trimmedValue)) // Stop using it now
			//if lengthChanged {
			//	stringValue = trimmedValue
			//}
			fpScale = len(stringValue) - (strings.Index(stringValue, ".") + 1)
			isSubHash = false
			//if fpScale != -1 && lengthChanged {
			//	stringValue = strings.TrimRight(fmt.Sprintf("%f", floatValue), "0")
			//	columnData.RawData = []byte(stringValue)
			//	byteLength = len(columnData.RawData)
			//}
		} else {
			fpScale = -1
		}

		isNegative = strings.HasPrefix(stringValue, "-")
		dc.Column.AnalyzeNumericValue(floatValue);


	}
	bSubHash := uint8(0)
	if isSubHash {
		for _, bChar := range dc.RawData {
			if bChar > 0 {
				bSubHash = ((uint8(37) * bSubHash) + uint8(bChar)) & 0xff
			}
		}
	}

	dc.Column.AnalyzeStringValue(stringValue)
	if isNumeric {
		dc.dataCategory = &DataCategoryType{
			IsNumeric:nullable.NewNullBool(false),
			ByteLength:nullable.NewNullInt64(int64(len)),
		}
		dc.dataCategory.AnalyzeNumericValue(floatValue)
	} else {
		dc.dataCategory = &DataCategoryType{
			IsNumeric:nullable.NewNullBool(true),
			ByteLength:nullable.NewNullInt64(int64(len)),
			IsNegative:nullable.NewNullBool(isNegative),
			FloatingPointScale:nullable.NewNullInt64(fpScale),
		}
	}

	dc.dataCategory.AnalyzeStringValue(stringValue)




}

func NumericCategory(len int,negative bool,fpScale int) *DataCategoryType {
	return
}

}

func (ti *RowDataType) WriteTo(writer io.Writer) (result int){
	//writer.Write([]byte{0xBF}); //SPARE

	columnCount := len(ti.RawData)
	binary.Write(writer,binary.LittleEndian,uint16(columnCount)) //
	result = 2
	for _, data := range ti.RawData {
		lenData := len(data)
		binary.Write(writer, binary.LittleEndian, uint16(lenData))
		result = result + 2
		writer.Write(data)
		result = result + lenData
	}
	return
}


/*func (ti *RowDataType) ReadFrom(reader io.Reader){
	columnCount := uint16(0)
	binary.Read(reader,binary.LittleEndian,&columnCount)

	for _, data := range ti.Data {
		binary.Write(writer, binary.LittleEndian, uint16(len(data)))
		writer.Write(data)
	}
}
*/