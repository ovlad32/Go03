package dataflow

import (
	"astra/metadata"
	"context"
	"fmt"
	"github.com/goinggo/tracelog"
	"astra/nullable"
)

const VarcharMax = 4000

type Repository struct {
	*metadata.Repository
}

func (h2 Repository) PersistDataCategory(ctx context.Context, dataCategory *DataCategoryType) (err error) {
	funcName := "Repository.SaveColumnCategories"

	tx, err := h2.IDb.Begin()
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Begin transaction...")
		return
	}
	defer tx.Rollback()
	_, err = tx.Exec(
		fmt.Sprintf("merge into column_datacategory_stats("+
			" column_id "+
			", key "+
			", byte_length "+
			", is_numeric "+
			", is_negative "+
			", is_integer "+
			", non_null_count "+
			", hash_unique_count "+
			", item_unique_count "+
			", min_sval "+
			", max_sval "+
			", min_fval "+
			", max_fval "+
			", moving_mean "+
			", moving_stddev "+
			" ) key(column_id, key) "+
			" values(%v, '%v', %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v) ",
			dataCategory.Column.Id,
			dataCategory.Key,
			dataCategory.ByteLength.Value(),
			dataCategory.IsNumeric.Value(),
			dataCategory.IsNegative.Value(),
			dataCategory.IsInteger.Value(),
			dataCategory.NonNullCount,
			dataCategory.HashUniqueCount,
			dataCategory.ItemUniqueCount,
			dataCategory.MinStringValue.SQLString(),
			dataCategory.MaxStringValue.SQLString(),
			dataCategory.MinNumericValue,
			dataCategory.MaxNumericValue,
			dataCategory.MovingMean,
			dataCategory.MovingStandardDeviation,
		),
	)
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	/*
		_, err = tx.Exec(fmt.Sprintf(
			"update column_info c set "+
				" (has_numeric_content, min_fval, max_fval, min_sval, max_sval) = ("+
				" select max(is_numeric) as has_numeric_content "+
				", min(min_fval)  as min_fval "+
				", max(max_fval)  as max_fval"+
				", min(min_sval)  as min_sval "+
				", max(max_sval)  as max_sval "+
				" from column_datacategory_stats s "+
				"  where s.column_id = c.id " +
				" ) where c.id = %v ", dataCategory.Column.Id.Value(),
		))

		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}

		_, err = tx.Exec(fmt.Sprintf("update column_info c set " +
			"   integer_unique_count = %v" +
			"   , moving_mean = %v" +
			"   , moving_stddev = %v" +
			"  where c.id = %v",
				column.IntegerUniqueCount,
			column.MovingMean,
			column.MovingStandardDeviation,
			column.Id))*/

	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	tx.Commit()

	return
}

func (h2 Repository) CreateDataCategoryTable() (err error) {
	funcName := "Repository.CreateDataCategoryTable"
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	//_, err = tx.Exec("drop table if exists column_datacategory_stats")

	_, err = tx.Exec("create table if not exists column_datacategory_stats(" +
		" column_id bigint not null " +
		", key varchar(30) not null " +
		", byte_length int null " +
		", is_numeric bool  null " +
		", is_negative bool  null " +
		", is_integer bool  null " +
		", non_null_count bigint" +
		", hash_unique_count bigint" +
		", item_unique_count bigint" +
		", min_sval varchar(" + fmt.Sprintf("%v", VarcharMax) + ")" +
		", max_sval varchar(" + fmt.Sprintf("%v", VarcharMax) + ")" +
		", min_fval float" +
		", max_fval float" +
		", moving_mean float" +
		", moving_stddev float" +
		", constraint column_datacategory_stats_pk " +
		"  primary key(column_id, key) " +
		" ) ")
	err = tx.Commit()
	if err != nil {
		//tracelog.Errorf(err,packageName,funcName,"Commit transaction...")
		//return
	}



	tx, err = h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	//_, err = tx.Exec("drop table if exists column_datacategory_stats")

	_, err = tx.Exec("create table if not exists ckrd_key_info(" +
		" id bigint not null " +
		", table_info_id integer " +
		", key_type char(1) " +
		", column_count integer " +
		", processing_stage char(1) " +
		", reference_to integer" +
		", constraint ckrd_key_info_pk primary key(id)" +
		", constraint ckrd_key_info_fk_table_id foreign key(table_info_id) references table_info(id)" +
		", constraint ckrd_key_info_fk_key foreign key(reference_to) references ckrd_key_info(id)" +
		", constraint ckrd_key_info_nl_table_info check(table_info_id is not null)" +
		", constraint ckrd_key_info_rg_type check(key_type in ('P','F') and key_type is not null)" +
		", constraint ckrd_key_info_rg_col_cnt check(column_count>0 and column_count is not null)" +
		" );" +
		" create sequence if not exists ckrd_id; " +
		" create table if not exists ckrd_key_column_info (" +
		"  id int "+
		", key_info_id int "+
		", column_info_id int " +
		", position int " +
		", constraint ckrd_key_info_column_pk primary key(id)" +
		", constraint ckrd_key_info_column_fk_column_id foreign key(column_info_id) references column_info(id) " +
		", constraint ckrd_key_info_column_fk_key_id foreign key(key_info_id) references ckrd_key_info(id) " +
		", constraint ckrd_key_info_column_nl_column_id check(column_info_id is not null)" +
		", constraint ckrd_key_info_column_nl_key_id check(key_info_id is not null)" +
		", constraint ckrd_key_info_column_rg_position check(position > 0 and position is not null)" +
		" )",

		)
	if err != nil {
		tracelog.Errorf(err,packageName,funcName,"table creation ...")
		//return
	}

	err = tx.Commit()
	if err != nil {
		//tracelog.Errorf(err,packageName,funcName,"Commit transaction...")
		//return
	}



	tx, err = h2.IDb.Begin()
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Begin transaction...")
		return
	}
	defer tx.Rollback()
	//_, err = tx.Exec("drop table if exists column_datacategory_stats")

	_, err = tx.Exec("alter table column_info add column  if not exists has_numeric_content boolean ")
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	_, err = tx.Exec("alter table column_info add column if not exists min_fval double")
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	_, err = tx.Exec("alter table column_info add column if not exists max_fval double ")
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	_, err = tx.Exec("alter table column_info add column  if not exists min_sval varchar(4000) ")
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	_, err = tx.Exec("alter table column_info add column if not exists max_sval varchar(4000) ")
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	_, err = tx.Exec("alter table column_info add column if not exists integer_unique_count bigint")
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	_, err = tx.Exec("alter table column_info add column if not exists moving_mean double")
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	_, err = tx.Exec("alter table column_info add column if not exists moving_stddev double")
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

	tx.Commit()
	//if err != nil {
	//tracelog.Errorf(err, packageName, funcName, "Commit transaction...")
	//return
	//}
	//err = nil

	/*	tx, err = h2.IDb.Begin()
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
	*/

	return
}

func (h2 Repository) dataCategory(whereFunc func() string) (result map[string]*DataCategoryType, err error) {

	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make(map[string]*DataCategoryType, 0)

	query := "SELECT " +
		" KEY " +
		", BYTE_LENGTH " +
		", IS_NUMERIC " +
		", IS_NEGATIVE " +
		", IS_INTEGER " +
		", NON_NULL_COUNT " +
		", HASH_UNIQUE_COUNT " +
		", ITEM_UNIQUE_COUNT " +
		", MIN_SVAL " +
		", MAX_SVAL " +
		", MIN_FVAL " +
		", MAX_FVAL " +
		", MOVING_MEAN " +
		", MOVING_STDDEV " +
		" FROM COLUMN_DATACATEGORY_STATS t "

	if whereFunc != nil {
		query = query + whereFunc()
	}
	rws, err := tx.Query(query)
	if err != nil {
		return
	}

	for rws.Next() {
		var row DataCategoryType
		err = rws.Scan(
			&row.Key,
			&row.ByteLength,
			&row.IsNumeric,
			&row.IsNegative,
			&row.IsInteger,
			&row.NonNullCount,
			&row.HashUniqueCount,
			&row.ItemUniqueCount,
			&row.MinStringValue,
			&row.MaxStringValue,
			&row.MinNumericValue,
			&row.MaxNumericValue,
			&row.MovingMean,
			&row.MovingStandardDeviation,
		)
		if err != nil {
			return
		}
		result[row.Key] = &row
	}
	return
}

func (h2 Repository) DataCategoryByColumnId(column *ColumnInfoType) (result map[string]*DataCategoryType, err error) {
	whereFunc := func() string {
		if column != nil && column.Id.Valid() {
			return fmt.Sprintf(" WHERE COLUMN_ID = %v ", column.Id)
		}
		return ""
	}

	result, err = h2.dataCategory(whereFunc)
	if err != nil {
		return
	}

	for index := range result {
		result[index].Column = column
	}
	return
}




type ComplexKeyInfoType struct {
  Id nullable.NullInt64
  TableInfoId nullable.NullInt64
  KeyType nullable.NullString
  ColumnCount nullable.NullInt64
  ProcessingStage nullable.NullString
  ReferenceTo nullable.NullInt64
  TableInfo *TableInfoType
  Columns[]*ComplexKeyColumnInfoType
}


type ComplexKeyColumnInfoType struct {
	Id nullable.NullInt64
	KeyInfoId nullable.NullInt64
	ColumnInfoId nullable.NullInt64
	Position nullable.NullInt64
	ComplexKey *ComplexKeyInfoType
}


func (h2 Repository) complexKey(whereFunc func() string) (result []*ComplexKeyInfoType, err error) {

	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]*ComplexKeyInfoType, 0,10)

	query := "SELECT " +
		" ID " +
		", TABLE_INFO_ID " +
		", KEY_TYPE " +
		", COLUMN_COUNT " +
		", PROCESSING_STAGE " +
		", REFERENCE_TO " +
		" FROM CKRD_KEY_INFO t "

	if whereFunc != nil {
		query = query + whereFunc()
	}
	rws, err := tx.Query(query)
	if err != nil {
		return
	}

	for rws.Next() {
		var row ComplexKeyInfoType
		err = rws.Scan(
			&row.Id,
			&row.TableInfoId,
			&row.KeyType,
			&row.ColumnCount,
			&row.ProcessingStage,
			&row.ReferenceTo,
		)
		if err != nil {
			return
		}
		result = append(result,&row)
	}
	return
}

func (h2 Repository) ComplexKeysByTable(table *TableInfoType) (result []*ComplexKeyInfoType, err error) {
	result, err = h2.complexKey(func() string {
		return fmt.Sprintf(" WHERE TABLE_INFO_ID = %v", table.Id.Value())
	})
	if err != nil {
		return
	}
	for _, key := range result {
		key.TableInfo = table
		key.Columns, err = h2.ComplexKeyColumnsByKey(key)
		if err != nil {
			return
		}
	}
	return
}



func (h2 Repository) complexKeyColumn(whereFunc func() string) (result []*ComplexKeyColumnInfoType, err error) {

	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]*ComplexKeyColumnInfoType, 0,10)

	query := "SELECT " +
		" ID " +
		", KEY_INFO_ID " +
		", COLUMN_INFO_ID " +
		", POSITION " +
		" FROM CKRD_KEY_COLUMN_INFO t "

	if whereFunc != nil {
		query = query + whereFunc()
	}
	rws, err := tx.Query(query)
	if err != nil {
		return
	}

	for rws.Next() {
		var row ComplexKeyColumnInfoType
		err = rws.Scan(
			&row.Id,
			&row.KeyInfoId,
			&row.ColumnInfoId,
			&row.Position,
		)
		if err != nil {
			return
		}
		result = append(result,&row)
	}
	return
}

func (h2 Repository) ComplexKeyColumnsByKey(key *ComplexKeyInfoType) (result []*ComplexKeyColumnInfoType, err error) {
	result,err = h2.complexKeyColumn(func() string {
		return fmt.Sprintf(" WHERE KEY_INFO_ID = %v", key.Id.Value())
	})
	if err != nil {
		return
	}
	for _, keyColumn := range result {
		keyColumn.ComplexKey = key;
	}
	return
}



func (h2 Repository) NewComplexKeyInfoId() (result nullable.NullInt64,err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		err = fmt.Errorf("begin transaction of getting a new complex key id: %v",err)
		return
	}
	rw := tx.QueryRow("select ckrd_id.nextval")

	err = rw.Scan(&result)
	if err != nil{
		err = fmt.Errorf("fetching a new complex key id: %v",err)
	}
	return
}


func (h2 Repository) PersistComplexKey(key *ComplexKeyInfoType) (err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	if !key.Id.Valid() {
		key.Id, err = h2.NewComplexKeyInfoId()
		if err != nil {
			err = fmt.Errorf(" acquiring a new key info Id to persist a new ComplexKeyInfo: %v", err)
			return
		}
		_, err = tx.Exec(
			fmt.Sprintf("insert into ckrd_key_info(id,key_type,table_info_id,column_count,processing_stage,reference_to) values(%v,%v,%v,%v,%v,%v)",
				key.Id,
				key.KeyType.SQLString(),
				key.TableInfoId,
				key.ColumnCount,
				key.ProcessingStage.SQLString(),
				key.ReferenceTo,
			),
		)
		if err != nil {
			err = fmt.Errorf(" inserting a new ComplexKeyInfo: %v", err)
			return
		}
	} else {
		_, err = tx.Exec(
			fmt.Sprintf("update ckrd_key_info set key_type = %v,table_info_id = %v,column_count = %v,processing_stage = %v ,reference_to=%v  where id = %v",
				key.KeyType.SQLString(), key.TableInfoId, key.ColumnCount, key.ProcessingStage.SQLString(), key.ReferenceTo, key.Id,
			),
		)
		if err != nil {
			err = fmt.Errorf(" updating ComplexKeyInfo with id = %v: %v", key.Id.Value(), err)
			return
		}
	}

	tx.Commit()


	return
}

func (h2 Repository) PersistComplexKeyColumns(key *ComplexKeyInfoType)(err error) {
	ids := make(map[int64]bool)
	for _, column := range key.Columns {
		err = h2.PersistComplexKeyColumn(column)
		if err != nil {
			return
		}
		ids[column.Id.Value()] = true;
	}
	txc, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	rows, err := txc.Query(fmt.Sprintf("select id from ckrd_key_column_info where key_info_id = %v", key.Id))
	if err != nil {
		return
	}
	var id nullable.NullInt64
	if rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return
		}

		if _, found := ids[id.Value()]; !found {
			txd, err := h2.IDb.Begin();
			if err != nil {
				return err
			}
			_, err = txd.Exec("delete from ckrd_key_column_info where id = %v", id)
			if err != nil {
				return err
			}
			txd.Commit();
		}
	}

	return
}


func (h2 Repository) PersistComplexKeyColumn(key *ComplexKeyColumnInfoType) (err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	if !key.Id.Valid() {
		key.Id, err = h2.NewComplexKeyInfoId()
		if err != nil {
			err = fmt.Errorf(" acquiring a new key info Id to persist a new ComplexKeyColumnInfo: %v", err)
			return
		}
		_, err = tx.Exec(
			fmt.Sprintf("insert into ckrd_key_column_info(id,key_info_id,column_info_id,position) values(%v,%v,%v,%v)",
				key.Id,
				key.KeyInfoId,
				key.ColumnInfoId,
				key.Position,
			),
		)
		if err != nil {
			err = fmt.Errorf(" inserting a new ComplexKeyColumnInfo: %v", err)
			return
		}
	} else {
		_, err = tx.Exec(
			fmt.Sprintf("update ckrd_key_column_info set column_info_id = %v,position = %v where id = %v",
				key.ColumnInfoId, key.Position, key.Id,
			),
		)
		if err != nil {
			err = fmt.Errorf(" updating ComplexKeyInfo with id = %v: %v", key.Id.Value(), err)
			return
		}
	}
	tx.Commit();
	return
}

