package dataflow


type ColumnDataType struct {
	Column       *ColumnInfoType
	dataCategory *DataCategoryType
	lineNumber   uint64
	lineOffset   uint64
	data       []byte
}
