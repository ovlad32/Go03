package metadata

import (
	jsnull "../src/util/jsnull"
	utils "../src/astra/utils"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	//_ "github.com/lxn/go-pgsql"
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"os"
	"strings"
)

var H2 H2Type

var (
	DatabaseInfoNotInitialized = errors.New("Database reference is not initialized")
	DatabaseIdNotInitialized   = errors.New("Database ID is not initialized")
	MetadataInfoNotInitialized = errors.New("Metadata reference is not initialized")
	MetadataIdNotInitialized   = errors.New("Metadata ID is not initialized")
	TableInfoNotInitialized    = errors.New("Table reference is not initialized")
	ColumnInfoNotInitialized   = errors.New("Column reference is not initialized")
	TableIdNotInitialized      = errors.New("Table ID is not initialized")
	ColumnIdNotInitialized     = errors.New("Column ID is not initialized")
)

const packageName = "metadata"

var tablesLabelBucketBytes = []byte("tables")
var columnsLabelBucketBytes = []byte("columns")

var hashStatsUnqiueCountBucketBytes = []byte("hashUniqueCount")

func (h2 *H2Type) InitDb() (idb *sql.DB) {
	if idb == nil {
		var err error
		idb, err = sql.Open("postgres",
			//fmt.Sprintf("user=%v password=%v dbname=%v host=%v port=%v  sslmode=disable",
			fmt.Sprintf("user=%v password=%v dbname=%v host=%v port=%v  timeout=10 sslmode=disable",

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

func (h2 H2Type) DatabaseConfigById(Id jsnull.NullInt64) (result DatabaseConfigType, err error) {
	whereFunc := func() string {
		if Id.Valid() {
			return fmt.Sprintf(" WHERE ID = %v", Id)
		}
		return ""
	}

	res, err := h2.databaseConfig(whereFunc)

	if err == nil && len(res) > 0 {
		result = res[0]
	}
	return
}

func (h2 H2Type) metadata(whereFunc func() string) (result []*MetadataType, err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]*MetadataType, 0)
	query := "SELECT " +
		" ID" +
		" ,INDEX" +
		" ,VERSION" +
		" ,DATABASE_CONFIG_ID" +
		" FROM METADATA "

	if whereFunc != nil {
		query = query + whereFunc()
	}
	query = query + " ORDER BY ID"
	rws, err := tx.Query(query)
	if err != nil {
		return
	}

	for rws.Next() {
		var row MetadataType
		err = rws.Scan(
			&row.Id,
			&row.Index,
			&row.Version,
			&row.DatabaseConfigId,
		)
		if err != nil {
			return
		}
		result = append(result, &row)
	}
	return
}

func (h2 H2Type) HighestDatabaseConfigVersion(DatabaseConfigId jsnull.NullInt64) (result jsnull.NullInt64, err error) {

	if !DatabaseConfigId.Valid() {
		err = errors.New("DatabaseConfigId is not valid")
		return
	}

	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	err = tx.QueryRow(fmt.Sprintf("SELECT MAX(VERSION) FROM METADATA WHERE DATABASE_CONFIG_ID = %v", DatabaseConfigId)).Scan(result)
	return
}

func (h2 H2Type) LastMetadata(DatabaseConfigId jsnull.NullInt64) (result *MetadataType, err error) {
	version, err := h2.HighestDatabaseConfigVersion(DatabaseConfigId)

	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	results, err := h2.metadata(func() string {
		return fmt.Sprintf(" WHERE DATABASE_CONFIG_ID = %v and VERSION = %v ",
			DatabaseConfigId,
			version,
		)
	})
	if err == nil && len(results) > 0 {
		result = results[0]
	}
	return
}
func (h2 H2Type) MetadataById(MetadataId jsnull.NullInt64) (result *MetadataType, err error) {
	if !MetadataId.Valid() {
		err = errors.New("MetadataId is not valid")
		return
	}
	results, err := h2.metadata(func() string {
		return fmt.Sprintf(" WHERE ID = %v", MetadataId)
	})
	if err == nil && len(results) > 0 {
		result = results[0]
	}
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
		if metadata != nil && metadata.Id.Valid() {
			return fmt.Sprintf(" WHERE METADATA_ID = %v and DUMPED=true", metadata.Id)
		}
		return ""
	}
	result, err = h2.tableInfo(whereFunc)
	if err != nil {
		return
	}
	for tableIndex := range result {
		result[tableIndex].Metadata = metadata
		_, err = h2.ColumnInfoByTable(result[tableIndex])
		if err != nil {
			return
		}
	}
	return
}

func (h2 H2Type) TableInfoById(Id jsnull.NullInt64) (result *TableInfoType, err error) {
	whereFunc := func() string {
		if Id.Valid() {
			return fmt.Sprintf(" WHERE ID = %v", Id)
		}
		return ""
	}
	res, err := h2.tableInfo(whereFunc)
	if err == nil {
		_, err = h2.ColumnInfoByTable(res[0])
	}
	if err == nil && len(res) > 0 {
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
		" ,TABLE_INFO_ID" +
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
			&row.TableInfoId,
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
		if tableInfo != nil && tableInfo.Id.Valid() {

			return fmt.Sprintf(" WHERE TABLE_INFO_ID = %v", tableInfo.Id)
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
		if Id.Valid() {
			return fmt.Sprintf(" WHERE ID = %v", Id)
		}
		return ""
	}
	res, err := h2.columnInfo(whereFunc)

	if err == nil && len(res) > 0 {
		result = res[0]
	}
	return
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
	Id                jsnull.NullInt64  `json:"tale-id"`
	MetadataId        jsnull.NullInt64  `json:"metadata-id"`
	DatabaseName      jsnull.NullString `json:"database-name"`
	SchemaName        jsnull.NullString `json:"schema-name"`
	TableName         jsnull.NullString `json:"table-name"`
	RowCount          jsnull.NullInt64  `json:"row-count"`
	Dumped            jsnull.NullString `json:"data-dumped"`
	Indexed           jsnull.NullString `json:"data-indexed"`
	PathToFile        jsnull.NullString `json:"path-to-file"`
	PathToDataDir     jsnull.NullString `json:"path-to-data-dir"`
	Metadata          *MetadataType
	Columns           []*ColumnInfoType `json:"columns"`
	TableLabelBucket  *bolt.Bucket
	TableBucket       *bolt.Bucket
	ColumnLabelBucket *bolt.Bucket
	dump              *os.File
}
type TableInfoTypeChannel chan *TableInfoType

func (ti *TableInfoType) ResetBuckets() {
	funcName := "TableInfoType.ResetBuckets"
	ti.TableLabelBucket = nil
	ti.TableBucket = nil
	ti.ColumnLabelBucket = nil
	if ti.Columns != nil {
		for _, col := range ti.Columns {
			col.ResetBuckets()
		}
	}
	tracelog.Completed(packageName, funcName)
}

func (ti *TableInfoType) GetOrCreateBuckets(tx *bolt.Tx) (err error) {
	funcName := "TableInfoType.GetOrCreateBuckets"

	tracelog.Started(packageName, funcName)
	if !ti.Id.Valid() {
		err = TableIdNotInitialized
		tracelog.CompletedError(err, packageName, funcName)
		return
	}

	tableBucketIdBytes := utils.Int64ToB8(ti.Id.Value())
	tablesLabelBucket := tx.Bucket(tablesLabelBucketBytes)

	if tablesLabelBucket == nil {
		if tx.Writable() {
			tablesLabelBucket, err = tx.CreateBucket(tablesLabelBucketBytes)
			if err != nil {
				return
			}
			if tablesLabelBucket == nil {
				err = errors.New("Could not create predefined buclet \"" + string(tablesLabelBucketBytes) + "\". Got empty value")
				tracelog.Error(err, packageName, funcName)
				return
			} else {
				tracelog.Info(packageName, funcName, "Bucket \"tables\" created")
			}
		} else {
			err = errors.New("Predefined bucket \"tables\" does not exist")
			return
		}
	}

	ti.TableBucket = tablesLabelBucket.Bucket(tableBucketIdBytes[:])
	if ti.TableBucket == nil {
		if tx.Writable() {
			ti.TableBucket, err = tablesLabelBucket.CreateBucket(tableBucketIdBytes[:])
			if err != nil {
				return
			}
			if ti.TableBucket == nil {
				err = errors.New(fmt.Sprintf("Could not create bucket for table id %v. Got empty value", ti.Id))
				tracelog.Error(err, packageName, funcName)
				return
			} else {
				tracelog.Info(packageName, funcName, "Bucket for table id %v created", ti.Id)

			}
		} else {
			tracelog.Info(packageName, funcName, "Bucket for table id %v has not been created", ti.Id.Value())
			return
		}
	}

	ti.ColumnLabelBucket = ti.TableBucket.Bucket(columnsLabelBucketBytes)
	if ti.ColumnLabelBucket == nil {
		if tx.Writable() {
			ti.ColumnLabelBucket, err = ti.TableBucket.CreateBucket(columnsLabelBucketBytes)
			if err != nil {
				return
			}
			if ti.ColumnLabelBucket == nil {
				err = errors.New("Could not create predefined buclet \"columns\". Got empty value")
				tracelog.Error(err, packageName, funcName)
				return
			} else {
				tracelog.Info(packageName, funcName, "Bucket \"columns\" created", nil)
			}
		} else {
			err = errors.New(fmt.Sprintf("Predefined bucket \"columns\" does not exist for table id %v ", ti.Id))
			tracelog.Error(err, packageName, funcName)
			return
		}
	}
	tracelog.Completed(packageName, funcName)
	return
}

/*
func(ti *TableInfoType) SaveRow(row[][]byte) (length uint32, err error) {
	if ti.dump == nil {
		ti.dump, err = os.OpenFile(
			fmt.Sprintf("%v%v%v.bindata",
				ti.PathToDataDir,
				os.PathSeparator,
				ti.Id.Value(),
			), 0, os.O_CREATE);
	}
	fields := len(row)
	err = binary.Write(ti.dump, binary.BigEndian, uint16(fields))
	length = (1 + fields)*2

	for _, field := range (row) {
		err = binary.Write(ti.dump, binary.BigEndian, uint16(length))
		length += len(field);
	}
	for _, field := range (row) {
		_,err = ti.dump.Write(field)
	}
	return
}
*/

/*func (ci *ColumnInfoType) GetOrCreateBucket(tx *bolt.Tx) (err error) {
	funcName := "ColumnInfoType.GetOrCreateBuckets"
	tracelog.Started(packageName,funcName)

	if !ci.Id.Valid() {
		err = ColumnIdNotInitialized
		return
	}
	if ci.TableInfo == nil {
		err = TableInfoNotInitialized
		return
	}
	if ci.TableInfo.TableBucket == nil {
		err = ci.TableInfo.GetOrCreateBuckets(tx)
		if err != nil {
			tracelog.Error(err,packageName,funcName)
			return
		}
		if ci.TableInfo.TableBucket == nil {
			return
		}
		if ci.TableInfo.ColumnLabelBucket == nil {
			return
		}
	}

	columnBucketIdBytes := utils.Int64ToB8(ci.Id.Value())

	ci.ColumnBucket = ci.TableInfo.ColumnLabelBucket.Bucket(columnBucketIdBytes[:])
	if ci.ColumnBucket == nil {
		if tx.Writable() {
			ci.ColumnBucket, err = ci.TableInfo.ColumnLabelBucket.CreateBucket(columnBucketIdBytes[:])
			if err != nil {
				tracelog.Error(err,packageName,funcName)
				return
			}
			if ci.ColumnBucket == nil {
				err = errors.New(fmt.Sprintf("Could not create bucket for column id %v. Got empty value", ci.Id))
				tracelog.Error(err,packageName,funcName)
				return
			} else {
				tracelog.Info(packageName,funcName,"Bucket for column id %v created", ci.Id)
			}
		} else {
			tracelog.Info(packageName,funcName,"Bucket for column id %v has not been created",ci.Id.Value())
		}
	}
	tracelog.Completed(packageName,funcName)
	return
}
*/

//	MinStringLength jsnull.NullInt64
//	MaxStringLength jsnull.NullInt64

func (h2 H2Type) SaveColumnCategory(column *ColumnInfoType) (err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	/*stmt,err := tx.Prepare("merge into column_datacategory_stats(" +
		" id" +
		", byte_length" +
		", is_numeric" +
		", is_float" +
		", is_negative" +
		", non_null_count" +
		", hash_unique_count" +
		", min_sval" +
		", max_sval" +
		", min_fval" +
		", max_fval) " +
		" key(id,byte_length,is_numeric,is_float,is_negative) " +
		" values(%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v) ")
	if err != nil {
		return
	}*/

	for _, c := range column.DataCategories {
		_, err = tx.Exec(
			fmt.Sprintf("merge into column_datacategory_stats("+
				" id"+
				", byte_length"+
				", is_numeric"+
				", is_negative"+
				", fp_scale"+
				", non_null_count"+
				", hash_unique_count"+
				", min_sval"+
				", max_sval"+
				", min_fval"+
				", max_fval) "+
				" key(id, byte_length, is_numeric, is_negative, fp_scale) "+
				" values(%v, %v, %v, %v, %v, %v, %v, '%v', '%v', %v, %v) ",
				column.Id,
				c.ByteLength,
				c.IsNumeric,
				c.IsNegative,
				c.FloatingPointScale,
				c.NonNullCount,
				c.HashUniqueCount,
				strings.Replace(c.MinStringValue.Value(), "'", "''", -1),
				strings.Replace(c.MaxStringValue.Value(), "'", "''", -1),
				c.MinNumericValue,
				c.MaxNumericValue,
			),
		)
		if err != nil {
			return
		}
	}
	tx.Commit()
	return
}
func (h2 H2Type) CreateDataCategoryTable() (err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	//_, err = tx.Exec("drop table if exists column_datacategory_stats")

	_, err = tx.Exec("create table if not exists column_datacategory_stats(" +
		" id bigint not null " +
		", byte_length int not null " +
		", is_numeric bool not null " +
		", is_negative bool not null " +
		", fp_scale int not null " +
		", non_null_count bigint" +
		", hash_unique_count bigint" +
		", min_sval varchar(4000)" +
		", max_sval varchar(4000)" +
		", min_fval float" +
		", max_fval float" +
		", constraint column_datacategory_stats_pk primary key(id, byte_length, is_numeric, is_negative, fp_scale) " +
		" ) ")
	tx.Commit()

	tx, err = h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	//_, err = tx.Exec("drop table if exists column_datacategory_stats")

	_, err = tx.Exec("create table if not exists column_pair( " +
		"id integer, " +
		"column_1_id integer,  " +
		"column_1_rowcount integer,  " +
		"column_2_id integer,  " +
		"column_2_rowcount integer,  " +
		"category_Intersection_Count integer,  " +
		"hash_Intersection_Count integer,  " +
		"status char(1)," +
		"constraint column_pair_pk primary key (column_1_id,column_2_id) " +
		")  ")
	tx.Commit()

	return
}

func (h2 H2Type) SaveColumnPairs(pairs []*ColumnPairType) (err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	for _, p := range pairs {
		merge := fmt.Sprintf("merge into column_pair("+
			//" id"+
			" column_1_id "+
			", column_2_id"+
			", column_1_rowcount"+
			", column_2_rowcount"+
			", category_intersection_count"+
			", hash_intersection_count"+
			", status "+
			") key (column_1_id, column_2_id) "+
			" values (%v, %v, %v, %v, %v, %v,'N') ",
			//column.Id,
			p.column1.Id,
			p.column2.Id,
			p.column1RowCount,
			p.column2RowCount,
			p.CategoryIntersectionCount,
			p.HashIntersectionCount,
		)
		_, err = tx.Exec(merge)
		if err != nil {
			return
		}
	}
	tx.Commit()
	return
}

func (h2 H2Type) columnPairs(whereFunc func() string) (result ColumnPairsType, err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	queryText := "select " +
		"  p.column_1_id" +
		", p.column_2_id" +
		", p.column_1_rowcount" +
		", p.column_2_rowcount" +
		", p.hash_intersection_count " +
		", p.status " +
		" from column_pair p  "
	if whereFunc != nil {
		queryText = queryText + whereFunc()
	}

	queryText = queryText + " order by p.hash_intersection_count desc"

	rows, err := tx.Query(queryText)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		pair := &ColumnPairType{
			column1: new(ColumnInfoType),
			column2: new(ColumnInfoType),
		}
		err = rows.Scan(
			&pair.column1.Id,
			&pair.column2.Id,
			&pair.column1RowCount,
			&pair.column2RowCount,
			&pair.HashIntersectionCount,
			&pair.ProcessStatus,
		)

		if err != nil {
			return
		}

		if result == nil {
			result = make(ColumnPairsType, 0, 10)
		}

		result = append(result, pair)
	}

	return
}

func (h H2Type) MetadataByWorkflowId(workflowId jsnull.NullInt64) (metadataId1, metadataId2 jsnull.NullInt64, err error) {
	queryText := fmt.Sprintf("select distinct t.metadata_id from link l "+
		" inner join column_info  c on c.id in (l.parent_column_info_id,l.child_column_info_id) "+
		" inner join table_info t on t.id = c.table_info_id "+
		" where l.workflow_id = %v ", workflowId.String())

	tx, err := h.IDb.Begin()
	if err != nil {
		return
	}

	result, err := tx.Query(queryText)
	if err != nil {
		return
	}
	defer result.Close()
	if result.Next() {
		result.Scan(&metadataId1)
	} else {
		err = fmt.Errorf("There is no the first metadata id  for workflow_id = %v", workflowId)
		return
	}
	if result.Next() {
		result.Scan(&metadataId2)
	} else {
		var clean jsnull.NullInt64
		metadataId1 = clean
		err = fmt.Errorf("There is no the second metadata id  for workflow_id = %v", workflowId)
		return
	}
	return
}
