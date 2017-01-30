package metadata

import (
	"fmt"
	"errors"
	"database/sql"
	"github.com/goinggo/tracelog"
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



func (c ColumnInfoType) СheckId() error {
	var funcName = "ColumnInfoType.СheckId"
	tracelog.Started(packageName, funcName)

	if !c.Id.Valid() {
		err := errors.New("Column Id has not been initialized!")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	tracelog.Completed(packageName, funcName)
	return nil
}

func (c ColumnInfoType) СheckTableInfo() error {
	var funcName = "ColumnInfoType.CheckMetadata"
	tracelog.Started(packageName, funcName)
	if c.TableInfo == nil {
		err := errors.New("Table reference has not been initialized!")
		tracelog.Error(err, packageName, funcName)
		return err
	}
	tracelog.Completed(packageName, funcName)
	return

}

func (c ColumnInfoType) String() string {
	var funcName = "ColumnInfoType.String"
	tracelog.Started(packageName, funcName)

	var result string
	if c.TableInfo != nil {
		return fmt.Sprintf("%v.%v", c.TableInfo.String(), c.ColumnName.String())
	} else {
		return fmt.Sprintf("%v", c.ColumnName.String())
	}
	return result
}


func (c ColumnInfoType) GoString() string {
	var funcName = "ColumnInfoType.GoString"
	tracelog.Started(packageName, funcName)

	var result = "ColumnInfoType["

	if c.TableInfo != nil {
		result = result + "TableInfo=" + c.TableInfo.GoString() + "; "
	}

	if c.ColumnName.Value() != "" {
		result = result + "ColumnName=" + c.ColumnName + "; "
	}
	if c.Id.Valid {
		result = result + "Id=" + fmt.Sprintf("%v", c.Id.Int64) + "; "
	}
	result = result + "]"
	tracelog.Completed(packageName, funcName)
	return result
}