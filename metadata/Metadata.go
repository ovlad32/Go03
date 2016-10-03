package metadata

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

type H2Type struct {
	Login          string
	Password       string
	hiddenPassword string
	DatabaseName   string
	Host           string
	Port           string
	IDb            *sql.DB
}

var H2 H2Type

func (h2 *H2Type) InitDb() (idb *sql.DB) {
	if idb == nil {
		var err error
		idb, err = sql.Open("postgres",
			fmt.Sprintf("user=%v password=%v dbname=%v host=%v port=%v sslmode=disable",
				h2.Login, h2.Password, h2.Port, h2.DatabaseName, h2.Host, h2.Port),
		)
		//idb, err = sql.Open("monetdb", "monetdb:monetdb@localhost:52000/test")
		if err != nil {
			panic(err)
		}
		h2.hiddenPassword, h2.Password = h2.Password, ""
	}
	return idb
}

type DatabaseConfigType struct {
	Id            sql.NullInt64  `json:"database-config-id"`
	ServerHost    sql.NullString `json:"server-host"`
	ServerPort    sql.NullInt64  `json:"server-port"`
	DatabaseAlias sql.NullString `json:"database-alias"`
	DatabaseName  sql.NullString `json:"database-name"`
	DatabaseGroup sql.NullString `json:"database-group"`
	ServerType    sql.NullString `json:"server-type"`
	UserName      sql.NullString `json:"user-name"`
	Password      sql.NullString `json:"user-password"`
	TargetSchema  sql.NullString `json:"target-schema-name"`
	//Driver             sql.NullString `json:"jdbcDriverClass"`
	//ServerTypeCustom01 sql.NullString
	//ServerTypeCustom02 sql.NullString
	//ServerTypeCustom03 sql.NullString
}

func (h2 H2Type) databaseConfig(whereFunc func() string) (result []*DatabaseConfigType, err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]*DatabaseConfigType, 0)
	query := "SELECT " +
		" ID" +
		" ,DATABASE_NAME" +
		" ,DB_GROUP" +
		" ,NAME" +
		" ,HOST" +
		" ,PORT" +
		" ,TARGET" +
		" ,SCHEMA" +
		" ,USERNAME" +
		" ,PASSWORD " +
		" FROM DATABASE_CONFIG "

	if whereFunc != nil {
		query = query + whereFunc()
	}
	query = query + " ORDER BY NAME"
	rws, err := tx.Query(query)
	if err != nil {
		return
	}

	for rws.Next() {
		var row DatabaseConfigType
		err = rws.Scan(
			&row.Id,
			&row.DatabaseName,
			&row.DatabaseGroup,
			&row.DatabaseAlias,
			&row.ServerHost,
			&row.ServerPort,
			&row.ServerType,
			&row.TargetSchema,
			&row.UserName,
			&row.Password,
		)
		if err != nil {
			return
		}
		result = append(result, &row)
	}
	return
}

func (h2 H2Type) DatabaseConfigAll() (result []*DatabaseConfigType, err error) {
	return h2.databaseConfig(nil)
}

func (h2 H2Type) DatabaseConfigById(Id sql.NullInt64) (result *DatabaseConfigType, err error) {
	whereFunc := func() string {
		if Id.Valid {
			return fmt.Sprintf(" WHERE ID = %v", Id.Int64)
		}
		return ""
	}

	res, err := h2.databaseConfig(whereFunc)

	if len(res) > 0 {
		result = res[0]
	}
	return
}

type Metadata struct {
	Id        sql.NullInt64
	DBConfig  *DatabaseConfigType
	InfoTaken sql.NullString
}

//metadataId sql.NullInt64
func (h2 H2Type) tableInfo(whereFunc func() string) (result []*TableInfoType, err error) {

	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]*TableInfoType, 0)

	query := "SELECT " +
		" ID" +
		" ,DATABASE_NAME" +
		" ,SCHEMA_NAME" +
		" ,NAME" +
		" ,ROW_COUNT" +
		" ,DUMPED" +
		" ,PATH_TO_FILE" +
		" ,PATH_TO_DATA_DIR" +
		" ,METADATA_ID" +
		" FROM TABLE_INFO "

	if whereFunc != nil {
		query = query + whereFunc()
	}
	query = query + " ORDER BY NAME"

	rws, err := tx.Query(query)
	if err != nil {
		return
	}

	for rws.Next() {
		var row TableInfoType
		err = rws.Scan(
			&row.Id,
			&row.DatabaseName,
			&row.SchemaName,
			&row.TableName,
			&row.RowCount,
			&row.Dumped,
			&row.PathToFile,
			&row.PathToDataDir,
			&row.MetadataId,
		)
		if err != nil {
			return
		}
		result = append(result, &row)
	}
	return
}

func (h2 H2Type) TableInfoByMetadata(metadata *Metadata) (result []*TableInfoType, err error) {
	whereFunc := func() string {
		if metadata != nil && metadata.Id.Valid {
			return fmt.Sprintf(" WHERE METADATA_ID = %v", metadata.Id.Int64)
		}
		return ""
	}
	return h2.tableInfo(whereFunc)
}

func (h2 H2Type) TableInfoById(Id sql.NullInt64) (result *TableInfoType, err error) {
	whereFunc := func() string {
		if Id.Valid {
			return fmt.Sprintf(" WHERE ID = %v", Id.Int64)
		}
		return ""
	}
	res, err := h2.tableInfo(whereFunc)

	if len(res) > 0 {
		result = res[0]
	}
	return
}

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
	Metadata      *Metadata
	Columns       []ColumnInfoType `json:"columns"`
}

func (h2 H2Type) columnInfo(whereFunc func() string) (result []*ColumnInfoType, err error) {

	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]*ColumnInfoType, 0)

	query := "SELECT " +
		" ID" +
		" ,NAME" +
		" ,DATA_TYPE" +
		" ,REAL_TYPE" +
		" ,CHAR_LENGTH" +
		" ,DATA_PRECISION" +
		" ,DATA_SCALE" +
		" ,POSITION" +
		" ,TOTAL_ROW_COUNT" +
		" ,UNIQUE_ROW_COUNT" +
		" ,HASH_UNIQUE_COUNT" +
		" FROM COLUMN_INFO "

	if whereFunc != nil {
		query = query + whereFunc()
	}

	query = query + " ORDER BY POSITION"
	rws, err := tx.Query(query)
	if err != nil {
		return
	}

	for rws.Next() {
		var row ColumnInfoType
		err = rws.Scan(
			&row.Id,
			&row.ColumnName,
			&row.DataType,
			&row.RealDataType,
			&row.CharLength,
			&row.DataPrecision,
			&row.DataScale,
			&row.Position,
			&row.TotalRowCount,
			&row.UniqueRowCount,
			&row.HashUniqueCount,
		)
		if err != nil {
			return
		}
		result = append(result, &row)
	}
	return
}

func (h2 H2Type) ColumnInfoByTable(tableInfo *TableInfoType) (result []*ColumnInfoType, err error) {
	whereFunc := func() string {
		if tableInfo != nil && tableInfo.Id.Valid {
			return fmt.Sprintf(" WHERE TABLE_INFO_ID = %v", tableInfo.Id.Int64)
		}
		return ""
	}
	return h2.columnInfo(whereFunc)
}

func (h2 H2Type) ColumnInfoById(Id sql.NullInt64) (result *ColumnInfoType, err error) {
	whereFunc := func() string {
		if Id.Valid {
			return fmt.Sprintf(" WHERE ID = %v", Id.Int64)
		}
		return ""
	}
	res, err := h2.columnInfo(whereFunc)

	if len(res) > 0 {
		result = res[0]
	}
	return
}

type ColumnInfoType struct {
	Id              sql.NullInt64  `json:"column-id"`
	TableInfoId     sql.NullInt64  `json:"table-id"`
	ColumnName      sql.NullString `json:"column-name"`
	Position        sql.NullInt64  `json:"column-position"`
	DataType        sql.NullString `json:"data-type"`
	DataPrecision   sql.NullInt64  `json:"numeric-precision"`
	DataScale       sql.NullInt64  `json:"numeric-scale"`
	DataLength      sql.NullInt64  `json:"byte-length"`
	CharLength      sql.NullInt64  `json:"character-length"`
	Nullable        sql.NullString `json:"nullable"`
	RealDataType    sql.NullString `json:"java-data-type"`
	MinStringValue  sql.NullString `json:"min-string-value"`
	MaxStringValue  sql.NullString `json:"max-string-value"`
	HashUniqueCount sql.NullInt64
	UniqueRowCount  sql.NullInt64
	TotalRowCount   sql.NullInt64
	MinStringLength sql.NullInt64
	MaxStringLength sql.NullInt64
	IsNumeric       sql.NullString
	IsInteger       sql.NullString
	MinIntegerValue sql.NullInt64
	MaxIntegerValue sql.NullInt64
	MinFloatValue   sql.NullFloat64
	MaxFloatValue   sql.NullFloat64
	NullCount       sql.NullInt64
	DistinctCount   sql.NullInt64
	TableInfo       *TableInfoType
	HashCategories  map[string]bool
	NumericCount    sql.NullInt64
}

type DumpConfigurationType struct {
	IsZipped        bool
	FieldSeparator  byte
	LineSeparator   byte
	InputBufferSize int
}

func (t TableInfoType) СheckId() {

	if !t.Id.Valid {
		panic("Table Id is not initialized!")
	}
}

func (t TableInfoType) СheckTableName() {
	if !t.TableName.Valid {
		panic("Table Name is not initialized!")
	}

	if t.TableName.String == "" {
		panic("Table Name is empty!")
	}
}

func (t TableInfoType) МheckMetadata() {
	if t.Metadata == nil {
		panic("Metadata reference is not initialized!")
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

	if t.SchemaName.String != "" {
		result = t.SchemaName.String + "."
	}

	if t.TableName.String == "" {
		result += "Table name is not defined"
	} else {
		result += t.TableName.String
	}

	return result
}

func (t TableInfoType) СheckDumpFileName() {
	if !t.PathToFile.Valid {
		panic("Dump File Name is not initialized!")
	}
	if t.PathToFile.String == "" {
		panic("Dump File Name is empty!")
	}
}

func (c ColumnInfoType) СheckId() {
	if !c.Id.Valid {
		panic("Column Id is not initialized!")
	}
}
func (c ColumnInfoType) СheckTableInfo() {
	if c.TableInfo == nil {
		panic("Table reference is not initialized!")
	}
}

func (c ColumnInfoType) String() string {
	var result string

	if c.TableInfo != nil {
		return fmt.Sprintf("%v.%v", c.TableInfo, c.ColumnName.String)
	} else {
		return fmt.Sprintf("%%v", c.TableInfo, c.ColumnName.String)
	}
	return result
}
