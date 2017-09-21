package metadata

import (
	"astra/nullable"
	"errors"
	"fmt"
	"github.com/goinggo/tracelog"
)

type ColumnInfoType struct {
	Id              nullable.NullInt64  `json:"column-id"`
	TableInfoId     nullable.NullInt64  `json:"table-id"`
	ColumnName      nullable.NullString `json:"column-name"`
	Position        nullable.NullInt64  `json:"column-position"`
	DataType        nullable.NullString `json:"data-type"`
	DataPrecision   nullable.NullInt64  `json:"numeric-precision"`
	DataScale       nullable.NullInt64  `json:"numeric-scale"`
	DataLength      nullable.NullInt64  `json:"byte-length"`
	CharLength      nullable.NullInt64  `json:"character-length"`
	Nullable        nullable.NullString `json:"astra.nullable"`
	RealDataType    nullable.NullString `json:"java-data-type"`
	MinStringValue  nullable.NullString `json:"min-string-value"`
	MaxStringValue  nullable.NullString `json:"max-string-value"`
	CategoryCount   nullable.NullInt64
	HashUniqueCount nullable.NullInt64
	UniqueRowCount  nullable.NullInt64
	TotalRowCount   nullable.NullInt64
	MinStringLength nullable.NullInt64
	MaxStringLength nullable.NullInt64
	//IsAllNumeric     nullable.NullString
	//IsAllInteger     nullable.NullString
	NumericCount    nullable.NullInt64
	MinNumericValue nullable.NullFloat64
	MaxNumericValue nullable.NullFloat64
	NonNullCount    nullable.NullInt64
	DistinctCount   nullable.NullInt64
	TableInfo       *TableInfoType
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

func (c ColumnInfoType) СheckTableInfo() (err error) {
	var funcName = "ColumnInfoType.CheckMetadata"
	tracelog.Started(packageName, funcName)
	if c.TableInfo == nil {
		err = errors.New("Table reference has not been initialized!")
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
		result = result + "ColumnName=" + c.ColumnName.Value() + "; "
	}
	if c.Id.Valid() {
		result = result + "Id=" + fmt.Sprintf("%v", c.Id.Value()) + "; "
	}
	result = result + "]"
	tracelog.Completed(packageName, funcName)
	return result
}
