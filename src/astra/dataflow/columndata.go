package dataflow


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


