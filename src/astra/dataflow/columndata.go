package dataflow

import "astra/metadata"

type ColumnDataType struct {
	Column       *metadata.ColumnInfoType
	dataCategory *DataCategoryType
	LineNumber   uint64
	LineOffset   uint64
	Data       []byte
}
type RowDataType struct{
	Table *metadata.TableInfoType
	LineNumber   uint64
	Data      [][]byte
}
//  |assdf sdfsdf sdfsdf
