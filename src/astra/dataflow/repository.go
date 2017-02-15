package dataflow

import (
	"fmt"
	"strings"
	"astra/metadata"
)
type Repository struct {
	*metadata.Repository
}



func (h2 Repository) SaveColumnCategory(column *ColumnInfoType) (err error) {
	tx, err := h2.IDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	for _, c := range column.Categories {
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
	    ", key varchar(30) "+
		", byte_length int null " +
		", is_numeric bool  null " +
		", is_negative bool  null " +
		", fp_scale int  null " +
		", non_null_count bigint" +
		", hash_unique_count bigint" +
		", min_sval varchar(4000)" +
		", max_sval varchar(4000)" +
		", min_fval float" +
		", max_fval float" +
		", constraint column_datacategory_stats_pk " +
		"  primary key(id, key) " +
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

