package metadata

import (
	"astra/nullable"
	"database/sql"
	"errors"
	"fmt"
	"github.com/goinggo/tracelog"
	_ "github.com/lib/pq"
)

type RepositoryConfig struct {
	Login        string
	Password     string
	DatabaseName string
	Host         string
	Port         string
}

type Repository struct {
	config *RepositoryConfig
	IDb    *sql.DB
}

func ConnectToAstraDB(conf *RepositoryConfig) (result *Repository, err error) {
	var funcName = "ConnectToAstraDB"
	tracelog.Started(packageName, funcName)
	result = new(Repository)

	idb, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"user=%v password=%v dbname=%v host=%v port=%v  timeout=10 sslmode=disable",
			conf.Login, conf.Password, conf.DatabaseName, conf.Host, conf.Port,
		),
	)
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}
	result.IDb = idb
	result.config = conf

	tracelog.Completed(packageName, funcName)
	return
}

func (rps Repository) databaseConfig(whereFunc func() string) (result []DatabaseConfigType, err error) {
	var funcName = "Repository.databaseConfig"
	tracelog.Started(packageName, funcName)

	tx, err := rps.IDb.Begin()
	if err != nil {
		tracelog.Error(err, packageName, funcName)
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
		tracelog.Error(err, packageName, funcName)
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
			tracelog.Error(err, packageName, funcName)
			return
		}
		result = append(result, row)
	}

	tracelog.Completed(packageName, funcName)
	return
}

func (rps Repository) DatabaseConfigAll() (result []DatabaseConfigType, err error) {
	return rps.databaseConfig(nil)
}

func (tps Repository) DatabaseConfigById(Id nullable.NullInt64) (result DatabaseConfigType, err error) {
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

func (rps Repository) HighestDatabaseConfigVersion(DatabaseConfigId nullable.NullInt64) (result nullable.NullInt64, err error) {

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

func (rps Repository) LastMetadata(DatabaseConfigId nullable.NullInt64) (result *MetadataType, err error) {
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
func (rps Repository) MetadataById(MetadataId nullable.NullInt64) (result *MetadataType, err error) {
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

func (h2 Repository) TableInfoById(Id nullable.NullInt64) (result *TableInfoType, err error) {
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

func (h2 Repository) ColumnInfoById(Id nullable.NullInt64) (result *ColumnInfoType, err error) {
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

func (h Repository) MetadataByWorkflowId(workflowId nullable.NullInt64) (metadataId1, metadataId2 nullable.NullInt64, err error) {
	queryText := fmt.Sprintf("select distinct t.metadata_id from link l "+
		" inner join column_info  c on c.id in (l.parent_column_info_id,l.child_column_info_id) "+
		" inner join table_info t on t.id = c.table_info_id "+
		" where l.workflow_id = %v ", workflowId.Value())

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
		err = fmt.Errorf("There is no the first metadata id for workflow_id = %v", workflowId.Value())
		return
	}
	if result.Next() {
		result.Scan(&metadataId2)
	} else {
		var clean nullable.NullInt64
		metadataId1 = clean
		err = fmt.Errorf("There is no the second metadata id for workflow_id = %v", workflowId.Value())
		return
	}
	return
}

func (h2 *Repository) PutMetadata(m *MetadataType) (err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}

	if !m.Id.Valid() {
		row := tx.QueryRow("select nextval('META_DATA_SEQ')")
		var id int64
		err = row.Scan(&id)
		if err != nil {
			return
		}
		m.Id = nullable.NewNullInt64(id)
	}

	statement := "merge into metadata (id, index, version, database_config) " +
		" key(id) values(%v,%v,%v,%v,%v,%v,%v,%v)"

	statement = fmt.Sprintf(
		statement,
		m.Id,
		m.Index,
		m.Version,
		m.DatabaseConfigId,
	)
	_, err = tx.Exec(statement)
	if err != nil {
		return
	}

	tx.Commit()
	return
}
