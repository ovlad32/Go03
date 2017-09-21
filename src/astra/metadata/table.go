package metadata

import (
	"astra/nullable"
	"errors"
	"fmt"
	"github.com/goinggo/tracelog"
)

type TableInfoType struct {
	Id            nullable.NullInt64  `json:"tale-id"`
	MetadataId    nullable.NullInt64  `json:"metadata-id"`
	DatabaseName  nullable.NullString `json:"database-name"`
	SchemaName    nullable.NullString `json:"schema-name"`
	TableName     nullable.NullString `json:"table-name"`
	RowCount      nullable.NullInt64  `json:"row-count"`
	Dumped        nullable.NullString `json:"data-dumped"`
	Indexed       nullable.NullString `json:"data-indexed"`
	PathToFile    nullable.NullString `json:"path-to-file"`
	PathToDataDir nullable.NullString `json:"path-to-data-dir"`
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

func (t TableInfoType) СheckTableName() (err error) {
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

func (t TableInfoType) CheckMetadata() (err error) {
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

func (t TableInfoType) СheckColumnListExistence() (err error) {
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

func (t TableInfoType) СheckDumpFileName() (err error) {
	var funcName = "TableInfoType.СheckDumpFileName"
	tracelog.Started(packageName, funcName)

	if !t.PathToFile.Valid() {
		err := errors.New("Dump filename has not been initialized!")
		tracelog.Error(err, packageName, funcName)
		return err
	}
	if t.PathToFile.Value() == "" {
		err := errors.New("Dump filename is empty!")
		tracelog.Error(err, packageName, funcName)
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
		result = result + t.SchemaName.Value() + "."
	}

	if t.TableName.Value() != "" {
		result = result + t.TableName.Value()
	}
	if false && t.Id.Valid() {
		result = result + " (Id=" + fmt.Sprintf("%v", t.Id.Value()) + ")"
	}
	tracelog.Completed(packageName, funcName)
	return result
}

func (t TableInfoType) GoString() string {
	var funcName = "TableInfoType.GoString"
	tracelog.Started(packageName, funcName)

	var result = "TableInfo["

	if t.SchemaName.Value() != "" {
		result = result + "SchemaName=" + t.SchemaName.Value() + "; "
	}

	if t.TableName.Value() != "" {
		result = result + "TableName=" + t.TableName.Value() + "; "
	}
	if t.Id.Valid() {
		result = result + "Id=" + fmt.Sprintf("%v", t.Id.Value()) + "; "
	}
	result = result + "]"
	tracelog.Completed(packageName, funcName)
	return result
}
