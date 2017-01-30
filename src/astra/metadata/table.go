package metadata

import (
	"database/sql"
	"fmt"
)

type TableInfoType struct {
	Id                sql.NullInt64  `json:"tale-id"`
	MetadataId        sql.NullInt64  `json:"metadata-id"`
	DatabaseName      sql.NullString `json:"database-name"`
	SchemaName        sql.NullString `json:"schema-name"`
	TableName         sql.NullString `json:"table-name"`
	RowCount          sql.NullInt64  `json:"row-count"`
	Dumped            sql.NullString `json:"data-dumped"`
	Indexed           sql.NullString `json:"data-indexed"`
	PathToFile        sql.NullString `json:"path-to-file"`
	PathToDataDir     sql.NullString `json:"path-to-data-dir"`
	Metadata          *MetadataType
	Columns           []*ColumnInfoType `json:"columns"`
}



func (t TableInfoType) СheckId() {

	if !t.Id.Valid() {
		panic("Table Id has not been initialized!")
	}
}

func (t TableInfoType) СheckTableName() {
	if !t.TableName.Valid() {
		panic("Table Name has not been  initialized!")
	}

	if t.TableName.Value() == "" {
		panic("Table Name is empty!")
	}
}

func (t TableInfoType) CheckMetadata() {
	if t.Metadata == nil {
		panic("Metadata reference has not been  initialized!")
	}
}

func (t TableInfoType) СheckColumns() {
	t.СheckTableName()
	if len(t.Columns) == 0 {
		panic(fmt.Sprintf("Table %v does not have columns", t))
	}
}

func (t TableInfoType) String() string {
	var result string

	if t.SchemaName.Value() != "" {
		result = t.SchemaName.Value() + "."
	}

	if t.TableName.Value() == "" {
		result = result + "Table name has not been defined"
	} else {
		result = result + t.TableName.Value()
	}
	return result
}

func (t TableInfoType) СheckDumpFileName() {
	if !t.PathToFile.Valid() {
		panic("Dump File Name has not been initialized!")
	}
	if t.PathToFile.Value() == "" {
		panic("Dump File Name is empty!")
	}
}