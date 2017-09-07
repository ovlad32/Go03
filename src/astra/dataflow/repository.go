package dataflow

import (
	"astra/metadata"
	"fmt"
	"github.com/goinggo/tracelog"
	"context"
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
				", integer_unique_count "+
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
				dataCategory.IntegerUniqueCount,
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
		", integer_unique_count bigint" +
		", min_sval varchar("+fmt.Sprintf("%v",VarcharMax) +")" +
		", max_sval varchar("+fmt.Sprintf("%v",VarcharMax) +")" +
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



func (h2 Repository) dataCategory(whereFunc func() string) (result []*DataCategoryType, err error) {

	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]*DataCategoryType, 0)

	query := "SELECT " +
		" KEY "+
		", BYTE_LENGTH "+
		", IS_NUMERIC "+
		", IS_NEGATIVE "+
		", IS_INTEGER "+
		", NON_NULL_COUNT "+
		", HASH_UNIQUE_COUNT "+
		", INTEGER_UNIQUE_COUNT "+
		", MIN_SVAL "+
		", MAX_SVAL "+
		", MIN_FVAL "+
		", MAX_FVAL "+
		", MOVING_MEAN " +
		", MOVING_STDDEV "+
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
			&row.IntegerUniqueCount,
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
		result = append(result, &row)
	}
	return
}

func (h2 Repository) DataCategoryByColumnId(column *ColumnInfoType) (result []*DataCategoryType, err error) {
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