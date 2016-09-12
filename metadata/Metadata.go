package metadata

import (
	"database/sql"
	"fmt"
	_ "github.com/fajran/go-monetdb"

)


var idb *sql.DB
func IDB() *sql.DB {
	if idb ==  nil {
		var err error
		idb,err = sql.Open("monetdb","monetdb:monetdb@localhost:52000/test")
		if err != nil {
			panic(err)
		}
	}
	return idb
}
type DatabaseConfig struct{
	Id sql.NullInt64
	DataSourceAlias sql.NullString
	ServerHost sql.NullString
	ServerPort int
	DatabaseName sql.NullString
	ServerType sql.NullString
	UserName sql.NullString
	Password sql.NullString
	Driver sql.NullString
	ServerTypeCustom01 sql.NullString
	ServerTypeCustom02 sql.NullString
	ServerTypeCustom03 sql.NullString
}

type Metadata struct{
	Id sql.NullInt64
	DBConfig *DatabaseConfig
	InfoTaken sql.NullString
}
type TableInfo struct {
	Id sql.NullInt64
	Metadata *Metadata
	TableName sql.NullString
	Schema sql.NullString
	DumpFileName sql.NullString
	Columns []ColumnInfo
}
type ColumnInfo struct {
	Id sql.NullInt64
	TableInfo *TableInfo
	ColumnName sql.NullString
	Position sql.NullInt64
	DBDataType sql.NullString
	DBPrecision sql.NullInt64
	DBScale sql.NullInt64
	DBLength sql.NullInt64
	DBCharLength sql.NullInt64
	Nullable sql.NullString
	RealDataType sql.NullString
	MinStringValue sql.NullString
	MaxStringValue sql.NullString
	MinStringLength sql.NullInt64
	MaxStringLength sql.NullInt64
	MinNumericValue sql.NullString
	MaxNumericValue sql.NullString
	NullCount sql.NullInt64
	DistinctCount sql.NullInt64
	HashUnqiqueCount sql.NullInt64
	HashCategories map[string] bool
}

type DumpConfig struct{


}

func(t TableInfo) СheckId(){

	if !t.Id.Valid {
		panic("Table Id is not initialized!")
	}
}

func(t TableInfo) СheckTableName(){
	if !t.TableName.Valid {
		panic("Table Name is not initialized!")
	}

	if t.TableName.String == "" {
		panic("Table Name is empty!")
	}
}

func(t TableInfo) МheckMetadata() {
	if t.Metadata == nil  {
		panic("Metadata reference is not initialized!")
	}
}

func(t TableInfo) СheckColumns() {
	t.СheckTableName()
	if len(t.Columns) == 0 {
		panic(fmt.Sprintf("Table %v does not have columns",t))
	}
}

func(t TableInfo) String() (string) {
	var result string

	if t.Schema.String != "" {
		result = t.Schema.String + "."
	}

	if t.TableName.String== "" {
		result += "Table name is not defined"
	}else{
		result += t.TableName.String
	}

	return result
}

func(c ColumnInfo) СheckId(){
	if !c.Id.Valid {
		panic("Column Id is not initialized!")
	}
}


func(t TableInfo) СheckDumpFileName(){
	if !t.DumpFileName.Valid {
		panic("Dump File Name is not initialized!")
	}
	if t.DumpFileName.String == "" {
		panic("Dump File Name is empty!")
	}
}


func(c ColumnInfo) СheckTableInfo(){
	if c.TableInfo == nil  {
		panic("Table reference is not initialized!")
	}
}


func(c ColumnInfo) String() (string) {
	var result string

	if c.TableInfo != nil {
		return fmt.Sprintf("%v.%v",c.TableInfo,c.ColumnName.String)
	} else {
		return fmt.Sprintf("%%v",c.TableInfo,c.ColumnName.String)
	}
	return result
}
