package dataflow

import (
	"astra/metadata"
	"fmt"
	"github.com/goinggo/tracelog"
)

type Repository struct {
	*metadata.Repository
}

func (h2 Repository) SaveColumnCategories(column *ColumnInfoType) (err error) {
	funcName := "Repository.SaveColumnCategories"

	tx, err := h2.IDb.Begin()
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Begin transaction...")
		return
	}
	defer tx.Rollback()

	for _, c := range column.Categories {
		_, err = tx.Exec(
			fmt.Sprintf("merge into column_datacategory_stats("+
				" column_id"+
				", key "+
				", byte_length"+
				", is_numeric"+
				", is_negative"+
				", is_integer"+
				", non_null_count"+
				"/*, hash_unique_count*/"+
				", min_sval"+
				", max_sval"+
				", min_fval"+
				", max_fval) "+
				" key(column_id, key) "+
				" values(%v, '%v', %v, %v, %v, %v, %v, /*%v,*/ %v, %v, %v, %v) ",
				column.Id,
				c.Key(),
				c.ByteLength.Value(),
				c.IsNumeric.Value(),
				c.IsNegative.Value(),
				c.IsInteger.Value(),
				c.NonNullCount.Value(),
				c.HashUniqueCount,
				c.MinStringValue.SQLString(),
				c.MaxStringValue.SQLString(),
				c.MinNumericValue,
				c.MaxNumericValue,
			),
		)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
	}

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
			" ) where c.id = %v ", column.Id.Value(),
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
		column.Id))

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
	_, err = tx.Exec("drop table if exists column_datacategory_stats")

	_, err = tx.Exec("create table if not exists column_datacategory_stats(" +
		" column_id bigint not null " +
		", key varchar(30) not null " +
		", byte_length int null " +
		", is_numeric bool  null " +
		", is_negative bool  null " +
		", is_integer bool  null " +
		//", fp_scale int  null " +
		", non_null_count bigint" +
		", hash_unique_count bigint" +
		", min_sval varchar(4000)" +
		", max_sval varchar(4000)" +
		", min_fval float" +
		", max_fval float" +
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
