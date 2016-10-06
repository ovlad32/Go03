package metadata

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	jsnull "./../jsnull"
)

var H2 H2Type

func (h2 *H2Type) InitDb() (idb *sql.DB) {
	if idb == nil {
		var err error
		idb, err = sql.Open("postgres",
			fmt.Sprintf("user=%v password=%v dbname=%v host=%v port=%v sslmode=disable",
				h2.Login, h2.Password, h2.DatabaseName, h2.Host, h2.Port),
		)
		//idb, err = sql.Open("monetdb", "monetdb:monetdb@localhost:52000/test")
		if err != nil {
			panic(err)
		}
		h2.hiddenPassword, h2.Password = h2.Password, ""
		h2.IDb = idb
	}
	//fmt.Println(idb)
	return idb
}

func (h2 H2Type) databaseConfig(whereFunc func() string) (result []DatabaseConfigType, err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]DatabaseConfigType, 0)
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
		result = append(result, row)
	}
	return
}


func (h2 H2Type) DatabaseConfigAll() (result []DatabaseConfigType, err error) {
	return h2.databaseConfig(nil)
}

func (h2 H2Type) DatabaseConfigById(Id sql.NullInt64) (result DatabaseConfigType, err error) {
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

func (h2 H2Type) metadata(whereFunc func() string) (result []MetadataType, err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]MetadataType, 0)
	query := "SELECT " +
		" ID" +
		" ,INDEX" +
		" ,INDEXED" +
		" ,VERSION" +
		" ,DATABASE_CONFIG_ID" +
		" ,INDEXED_KEYS" +
		" FROM METADATA "

	if whereFunc != nil {
		query = query + whereFunc()
	}
	query = query + " ORDER ID"
	rws, err := tx.Query(query)
	if err != nil {
		return
	}

	for rws.Next() {
		var row MetadataType
		err = rws.Scan(
			&row.Id,
			&row.Index,
			&row.Indexed,
			&row.Version,
			&row.DatabaseConfigId,
			&row.IndexedKeys,
		)
		if err != nil {
			return
		}
		result = append(result, row)
	}
	return
}

func (h2 H2Type) HighestDatabaseConfigVersion(DatabaseConfigId jsnull.NullInt64) (result jsnull.NullInt64, err error) {

	if !DatabaseConfigId.Valid {
		err = errors.New("DatabaseConfigId is invalid")
		return
	}

	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	err = tx.QueryRow(fmt.Sprintf("SELECT MAX(VERSION) FROM METADATA WHERE DATABASE_CONFIG_ID = %v", DatabaseConfigId.Int64)).Scan(result)
	return
}

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

func (h2 H2Type) TableInfoByMetadata(metadata *MetadataType) (result []*TableInfoType, err error) {
	whereFunc := func() string {
		if metadata != nil && metadata.Id.Valid {
			return fmt.Sprintf(" WHERE METADATA_ID = %v", metadata.Id.Int64)
		}
		return ""
	}
	return h2.tableInfo(whereFunc)
}

func (h2 H2Type) TableInfoById(Id jsnull.NullInt64) (result *TableInfoType, err error) {
	whereFunc := func() string {
		if Id.Valid {
			return fmt.Sprintf(" WHERE ID = %v", Id.Int64)
		}
		return ""
	}
	res, err := h2.tableInfo(whereFunc)
	if err == nil {
		_,err = h2.ColumnInfoByTable(res[0])
	}
	if len(res) > 0 {
		result = res[0]
	}
	return
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
	result, err = h2.columnInfo(whereFunc)
	if err == nil {
		for index := range result {
			result[index].TableInfo = tableInfo
		}
	}
	tableInfo.Columns = result
	return
}

func (h2 H2Type) ColumnInfoById(Id jsnull.NullInt64) (result *ColumnInfoType, err error) {
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

type H2Type struct {
	Login          string
	Password       string
	hiddenPassword string
	DatabaseName   string
	Host           string
	Port           string
	IDb            *sql.DB
}

type DumpConfigurationType struct {
	DumpBasePath    string
	IsZipped        bool
	FieldSeparator  byte
	LineSeparator   byte
	InputBufferSize int
}

type DatabaseConfigType struct {
	Id            jsnull.NullInt64  `json:"database-config-id"`
	ServerHost    jsnull.NullString `json:"server-host"`
	ServerPort    jsnull.NullInt64  `json:"server-port"`
	DatabaseAlias jsnull.NullString `json:"database-alias"`
	DatabaseName  jsnull.NullString `json:"database-name"`
	DatabaseGroup jsnull.NullString `json:"database-group"`
	ServerType    jsnull.NullString `json:"server-type"`
	UserName      jsnull.NullString `json:"user-name"`
	Password      jsnull.NullString `json:"user-password"`
	TargetSchema  jsnull.NullString `json:"target-schema-name"`
	//Driver             jsnull.NullString `json:"jdbcDriverClass"`
	//ServerTypeCustom01 jsnull.NullString
	//ServerTypeCustom02 jsnull.NullString
	//ServerTypeCustom03 jsnull.NullString
}

type MetadataType struct {
	Id               jsnull.NullInt64
	Index            jsnull.NullString
	Indexed          jsnull.NullString
	Version          jsnull.NullInt64
	DatabaseConfigId jsnull.NullInt64
	IndexedKeys      jsnull.NullString
	DBConfig         *DatabaseConfigType
}

type TableInfoType struct {
	Id            jsnull.NullInt64  `json:"tale-id"`
	MetadataId    jsnull.NullInt64  `json:"metadata-id"`
	DatabaseName  jsnull.NullString `json:"database-name"`
	SchemaName    jsnull.NullString `json:"schema-name"`
	TableName     jsnull.NullString `json:"table-name"`
	RowCount      jsnull.NullInt64  `json:"row-count"`
	Dumped        jsnull.NullString `json:"data-dumped"`
	Indexed       jsnull.NullString `json:"data-indexed"`
	PathToFile    jsnull.NullString `json:"path-to-file"`
	PathToDataDir jsnull.NullString `json:"path-to-data-dir"`
	Metadata      *MetadataType
	Columns       []*ColumnInfoType `json:"columns"`
}

type ColumnInfoType struct {
	Id              jsnull.NullInt64  `json:"column-id"`
	TableInfoId     jsnull.NullInt64  `json:"table-id"`
	ColumnName      jsnull.NullString `json:"column-name"`
	Position        jsnull.NullInt64  `json:"column-position"`
	DataType        jsnull.NullString `json:"data-type"`
	DataPrecision   jsnull.NullInt64  `json:"numeric-precision"`
	DataScale       jsnull.NullInt64  `json:"numeric-scale"`
	DataLength      jsnull.NullInt64  `json:"byte-length"`
	CharLength      jsnull.NullInt64  `json:"character-length"`
	Nullable        jsnull.NullString `json:"nullable"`
	RealDataType    jsnull.NullString `json:"java-data-type"`
	MinStringValue  jsnull.NullString `json:"min-string-value"`
	MaxStringValue  jsnull.NullString `json:"max-string-value"`
	HashUniqueCount jsnull.NullInt64
	UniqueRowCount  jsnull.NullInt64
	TotalRowCount   jsnull.NullInt64
	MinStringLength jsnull.NullInt64
	MaxStringLength jsnull.NullInt64
	IsAllNumeric    jsnull.NullString
	IsAllInteger    jsnull.NullString
	MinNumericValue jsnull.NullFloat64
	MaxNumericValue jsnull.NullFloat64
	NullCount       jsnull.NullInt64
	DistinctCount   jsnull.NullInt64
	TableInfo       *TableInfoType
	DataCategories  map[string]bool
	NumericCount    jsnull.NullInt64
}


