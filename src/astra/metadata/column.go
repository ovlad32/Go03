package metadata

import (
	"fmt"
	"database/sql"
)

type ColumnInfoType struct {
	Id               sql.NullInt64  `json:"column-id"`
	TableInfoId      sql.NullInt64  `json:"table-id"`
	ColumnName       sql.NullString `json:"column-name"`
	Position         sql.NullInt64  `json:"column-position"`
	DataType         sql.NullString `json:"data-type"`
	DataPrecision    sql.NullInt64  `json:"numeric-precision"`
	DataScale        sql.NullInt64  `json:"numeric-scale"`
	DataLength       sql.NullInt64  `json:"byte-length"`
	CharLength       sql.NullInt64  `json:"character-length"`
	Nullable         sql.NullString `json:"nullable"`
	RealDataType     sql.NullString `json:"java-data-type"`
	MinStringValue   sql.NullString `json:"min-string-value"`
	MaxStringValue   sql.NullString `json:"max-string-value"`
	CategoryCount    sql.NullInt64
	HashUniqueCount  sql.NullInt64
	UniqueRowCount   sql.NullInt64
	TotalRowCount    sql.NullInt64
	MinStringLength  sql.NullInt64
	MaxStringLength  sql.NullInt64
	IsAllNumeric     sql.NullString
	IsAllInteger     sql.NullString
	MinNumericValue  sql.NullFloat64
	MaxNumericValue  sql.NullFloat64
	NonNullCount     sql.NullInt64
	DistinctCount    sql.NullInt64
	TableInfo        *TableInfoType
}


func (c ColumnInfoType) String() string {
	var result string
	if c.TableInfo != nil {
		return fmt.Sprintf("%v.%v", c.TableInfo.String(), c.ColumnName.String())
	} else {
		return fmt.Sprintf("%v", c.ColumnName.String())
	}
	return result
}


func (c ColumnInfoType) СheckId() {
	if !c.Id.Valid() {
		panic("Column Id has not been initialized!")
	}
}

func (c ColumnInfoType) СheckTableInfo() {
	if c.TableInfo == nil {
		panic("Table reference has not been initialized!")
	}
}

