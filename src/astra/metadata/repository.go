package metadata

import (
	"fmt"
	"strings"
	"database/sql"
	"errors"
	"github.com/goinggo/tracelog"
)

type RepositoryConfig struct {
	Login          string
	Password       string
	DatabaseName   string
	Host           string
	Port           string
}

type Repository struct {
	config        *RepositoryConfig
	IDb           *sql.DB
}

func ConnectToAstraDB(conf RepositoryConfig) (result *Repository, err error) {
	var funcName = "ConnectToAstraDB"
	tracelog.Started(packageName,funcName)
	result = new(Repository)

	idb, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"user=%v password=%v dbname=%v host=%v port=%v  timeout=10 sslmode=disable",
			conf.Login, conf.Password, conf.DatabaseName, conf.Host, conf.Port,
		),
	)
	if err != nil {
		tracelog.Error(err,packageName,funcName)
		return
	}
	result.IDb = idb
	result.config = &conf;

	tracelog.Completed(packageName,funcName)
	return
}

func (rps Repository) databaseConfig(whereFunc func() string) (result []DatabaseConfigType, err error) {
	var funcName = "Repository.databaseConfig"
	tracelog.Started(packageName,funcName)

	tx, err := rps.IDb.Begin()
	if err != nil {
		tracelog.Error(err,packageName,funcName)
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
		tracelog.Error(err,packageName,funcName)
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
			tracelog.Error(err,packageName,funcName)
			return
		}
		result = append(result, row)
	}

	tracelog.Completed(packageName,funcName)
	return
}

func (rps Repository) DatabaseConfigAll() (result []DatabaseConfigType, err error) {
	return rps.databaseConfig(nil)
}

func (tps Repository) DatabaseConfigById(Id sql.NullInt64) (result DatabaseConfigType, err error) {
	whereFunc := func() string {
		if Id.Valid() {
			return fmt.Sprintf(" WHERE ID = %v", Id)
		}
		return ""
	}

	res, err := tps.databaseConfig(whereFunc)

	if err == nil && len(res) > 0 {
		result = res[0]
	}
	return
}

func (rps Repository) metadata(whereFunc func() string) (result []*MetadataType, err error) {
	tx, err := rps.IDb.Begin()
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

func (rps Repository) HighestDatabaseConfigVersion(DatabaseConfigId sql.NullInt64) (result sql.NullInt64, err error) {

	if !DatabaseConfigId.Valid() {
		err = errors.New("DatabaseConfigId is not valid")
		return
	}

	tx, err := rps.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	err = tx.QueryRow(fmt.Sprintf("SELECT MAX(VERSION) FROM METADATA WHERE DATABASE_CONFIG_ID = %v", DatabaseConfigId)).Scan(result)
	return
}

func (rps Repository) LastMetadata(DatabaseConfigId sql.NullInt64) (result *MetadataType, err error) {
	version, err := rps.HighestDatabaseConfigVersion(DatabaseConfigId)

	tx, err := rps.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	results, err := rps.metadata(func() string {
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
func (rps Repository) MetadataById(MetadataId sql.NullInt64) (result *MetadataType, err error) {
	if !MetadataId.Valid() {
		err = errors.New("MetadataId is not valid")
		return
	}
	results, err := rps.metadata(func() string {
		return fmt.Sprintf(" WHERE ID = %v", MetadataId)
	})
	if err == nil && len(results) > 0 {
		result = results[0]
	}
	return
}
func (h2 Repository) tableInfo(whereFunc func() string) (result []*TableInfoType, err error) {

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

func (h2 Repository) TableInfoByMetadata(metadata *MetadataType) (result []*TableInfoType, err error) {
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

func (h2 Repository) TableInfoById(Id sql.NullInt64) (result *TableInfoType, err error) {
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

func (h2 Repository) columnInfo(whereFunc func() string) (result []*ColumnInfoType, err error) {

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

func (h2 Repository) ColumnInfoByTable(tableInfo *TableInfoType) (result []*ColumnInfoType, err error) {
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

func (h2 Repository) ColumnInfoById(Id sql.NullInt64) (result *ColumnInfoType, err error) {
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


func (h2 Repository) SaveColumnCategory(column *ColumnInfoType) (err error) {
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
func (h2 Repository) CreateDataCategoryTable() (err error) {
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
		"id integer, "+
		"column_1_id integer,  "+
		"column_1_rowcount integer,  "+
		"column_2_id integer,  "+
		"column_2_rowcount integer,  "+
		"category_Intersection_Count integer,  "+
		"hash_Intersection_Count integer,  "+
		"status char(1),"+
		"constraint column_pair_pk primary key (column_1_id,column_2_id) "+
		")  ");
	tx.Commit()


	return
}


func (h2 Repository) SaveColumnPairs(pairs []*ColumnPairType) (err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	for _, p := range pairs  {
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
		_, err = tx.Exec( merge)
		if err != nil {
			return
		}
	}
	tx.Commit()
	return
}

func (h2 Repository) columnPairs(whereFunc func() string) (result ColumnPairsType, err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback();
	queryText := "select " +
		"  p.column_1_id" +
		", p.column_2_id" +
		", p.column_1_rowcount" +
		", p.column_2_rowcount" +
		", p.hash_intersection_count " +
		", p.status " +
		" from column_pair p  "
	if whereFunc != nil{
		queryText = queryText + whereFunc()
	}

	queryText = queryText + " order by p.hash_intersection_count desc"

	rows,err := tx.Query(queryText);
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		pair := &ColumnPairType {
			column1 : new(ColumnInfoType),
			column2 : new(ColumnInfoType),
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
			result = make(ColumnPairsType,0,10)
		}


		result = append(result,pair)
	}

	return
}

func (h Repository) MetadataByWorkflowId(workflowId sql.NullInt64)(metadataId1,metadataId2 sql.NullInt64, err error){
	queryText := fmt.Sprintf("select distinct t.metadata_id from link l "+
		" inner join column_info  c on c.id in (l.parent_column_info_id,l.child_column_info_id) "+
		" inner join table_info t on t.id = c.table_info_id " +
		" where l.workflow_id = %v ",workflowId.String());

	tx,err := h.IDb.Begin()
	if err != nil {
		return
	}

	result,err := tx.Query(queryText)
	if err != nil {
		return
	}
	defer result.Close()
	if result.Next() {
		result.Scan(&metadataId1)
	} else {
		err = fmt.Errorf("There is no the first metadata id  for workflow_id = %v",workflowId);
		return
	}
	if result.Next() {
		result.Scan(&metadataId2)
	} else {
		var clean sql.NullInt64
		metadataId1 = clean;
		err = fmt.Errorf("There is no the second metadata id  for workflow_id = %v",workflowId);
		return
	}
	return
}