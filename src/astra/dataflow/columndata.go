package dataflow

import "astra/metadata"

type ColumnDataType struct {
	Column       *metadata.ColumnInfoType
	dataCategory *DataCategoryType
	lineNumber   uint64
	lineOffset   uint64
	data       []byte
}
type RowDataType struct{
	Table *metadata.TableInfoType
	lineNumber   uint64
	data      [][]byte
}
//  |assdf sdfsdf sdfsdf
