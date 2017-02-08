package dataflow

import (
	"astra/metadata"
	"io"
	"encoding/binary"
)

type ColumnDataType struct {
	Column       *metadata.ColumnInfoType
	dataCategory *DataCategoryType
	LineNumber   uint64
	LineOffset   uint64
	Data       []byte
}
type RowDataType struct{
	Table *TableInfoType
	LineNumber   uint64
	Data      [][]byte
}


func (ti *RowDataType) WriteTo(writer io.Writer){
	//writer.Write([]byte{0xBF}); //SPARE
	columnCount := len(ti.Data)
	binary.Write(writer,binary.LittleEndian,uint16(columnCount)) //
	for _, data := range ti.Data {
		binary.Write(writer, binary.LittleEndian, uint16(len(data)))
		writer.Write(data)
	}
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