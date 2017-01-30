package metadata

import (
	"database/sql"
	"fmt"
	"github.com/goinggo/tracelog"
	"errors"
)

type TableInfoType struct {
	Id            sql.NullInt64  `json:"tale-id"`
	MetadataId    sql.NullInt64  `json:"metadata-id"`
	DatabaseName  sql.NullString `json:"database-name"`
	SchemaName    sql.NullString `json:"schema-name"`
	TableName     sql.NullString `json:"table-name"`
	RowCount      sql.NullInt64  `json:"row-count"`
	Dumped        sql.NullString `json:"data-dumped"`
	Indexed       sql.NullString `json:"data-indexed"`
	PathToFile    sql.NullString `json:"path-to-file"`
	PathToDataDir sql.NullString `json:"path-to-data-dir"`
	Metadata      *MetadataType
	Columns       []*ColumnInfoType `json:"columns"`
}

func (t TableInfoType) СheckId() error {
	var funcName = "TableInfoType.СheckId"
	tracelog.Started(packageName, funcName)

	if !t.Id.Valid() {
		err := errors.New("Table Id has not been initialized!")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	tracelog.Completed(packageName, funcName)
	return nil
}

func (t TableInfoType) СheckTableName() error {
	var funcName = "TableInfoType.СheckTableName"
	tracelog.Started(packageName, funcName)

	if !t.TableName.Valid() {
		err := errors.New("Table name has not been initialized!")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	if t.TableName.Value() == "" {
		err := errors.New("Table name is empty!")
		tracelog.Error(err, packageName, funcName)
		return err
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (t TableInfoType) CheckMetadata() error {
	var funcName = "TableInfoType.CheckMetadata"
	tracelog.Started(packageName, funcName)
	if t.Metadata == nil {
		err := errors.New("Metadata reference has not been initialized!")
		tracelog.Error(err, packageName, funcName)
		return err
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (t TableInfoType) СheckColumnListExistence() error {
	var funcName = "TableInfoType.СheckColumnListExistence"
	tracelog.Started(packageName, funcName)

	var errorMessage string
	if t.Columns == nil {
		errorMessage = "Column list has not been inititalized!"
	} else if len(t.Columns) == 0 {
		errorMessage = "Column list is empty!"
	}
	if len(errorMessage) > 0 {
		if err := t.СheckId(); err != nil {
			err = fmt.Errorf("Table %v. %v", t, errorMessage)
			tracelog.Error(err, packageName, funcName)
			return err
		} else {
			err = errors.New(errorMessage)
			tracelog.Error(err, packageName, funcName)
			return err
		}
	}
	tracelog.Completed(packageName, funcName)
	return
}


func (t TableInfoType) СheckDumpFileName() error{
	var funcName = "TableInfoType.СheckDumpFileName"
	tracelog.Started(packageName, funcName)

	if !t.PathToFile.Valid() {
		err:= errors.New("Dump filename has not been initialized!")
		tracelog.Error(err,packageName,funcName)
		return err
	}
	if t.PathToFile.Value() == "" {
		err:= errors.New("Dump filename is empty!")
		tracelog.Error(err,packageName,funcName)
		return err
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (t TableInfoType) String() string {
	var funcName = "TableInfoType.String"
	tracelog.Started(packageName, funcName)

	var result = ""

	if t.SchemaName.Value() != "" {
		result = result +t.SchemaName + "."
	}

	if t.TableName.Value() != "" {
		result = result + t.TableName
	}
	if false && t.Id.Valid {
		result = result + " (Id=" + fmt.Sprintf("%v", t.Id.Int64) + ")"
	}
	tracelog.Completed(packageName, funcName)
	return result
}


func (t TableInfoType) GoString() string {
	var funcName = "TableInfoType.GoString"
	tracelog.Started(packageName, funcName)

	var result = "TableInfo["

	if t.SchemaName.Value() != "" {
		result = result + "SchemaName=" + t.SchemaName + "; "
	}

	if t.TableName.Value() != "" {
		result = result + "TableName=" + t.TableName + "; "
	}
	if t.Id.Valid {
		result = result + "Id=" + fmt.Sprintf("%v", t.Id.Int64) + "; "
	}
	result = result + "]"
	tracelog.Completed(packageName, funcName)
	return result
}