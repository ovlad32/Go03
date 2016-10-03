package metadata

import (
//"database/sql"
)

/*
func GetMetadataInfo() []TableInfo {
	tblTx,err := IDB().Begin()

	if err != nil {
		panic(err)
	}
	defer tblTx.Commit()

	tables,err := tblTx.Query("select id,name,dumpfile from table_info")
	if err != nil {
		panic(err)
	}

	var result []TableInfo = make([]TableInfo,0)
	for tables.Next() {
		var ti TableInfo
		err := tables.Scan(&ti.Id,&ti.TableName,&ti.DumpFileName)
		if err != nil {
			panic(err)
		}

		ti.Columns = make([]ColumnInfo,0,2)

		colStatement,err := IDB().Prepare("select id, name, data_type, char_length/ *, precision, scale * / from column_info2 c where c.table_info_id = ? order by num")
		if err != nil {
			panic(err)
		}
		columns,err := colStatement.Query(ti.Id.Int64)
		if err != nil {
			panic(err)
		}
		for columns.Next() {
			var ci ColumnInfo
			err := columns.Scan(
				&ci.Id,
				&ci.ColumnName,
				&ci.DBDataType,
				&ci.DBCharLength,
				&ci.DBPrecision,
				&ci.DBScale,
			)
			if err != nil {
				panic(err)
			}
			ci.TableInfo = &ti
			ti.Columns = append(ti.Columns,ci)
		}
		result = append(result, ti)
	}
	return result;

};

func getDataType(column ColumnInfo) string {
	if column.DBDataType.String == "VARCHAR2" {
		return "VARCHAR"
	}
	return column.DBDataType.String
}

func CreateDumpTables(table *TableInfo) {
	rcols :=""
	hcols :=""
	for _, col := range table.Columns {
		rcols = rcols + ","
		hcols = hcols + ","
		dt := getDataType(col)
		if dt == "CHAR" || dt == "VARCHAR" {
			rcols = rcols + fmt.Sprintf(" c%v %v(%v)",col.Id.Int64,dt,col.DBCharLength.Int64)
		}else if dt == "NUMBER" || dt == "NUMERIC" {
			rcols = rcols + fmt.Sprintf(" c%v %v(%v)",col.Id.Int64,dt,col.DBPrecision.Int64,col.DBScale.Int64)
		} else if dt == "DATE" || dt == "DATETIME" {
			rcols = rcols + fmt.Sprintf(" c%v VARCHAR(30)",col.Id.Int64)
		}
		hcols = hcols  + fmt.Sprintf(" h%v BIGINT",col.Id.Int64,dt,col.DBPrecision.Int64,col.DBScale.Int64)
		fmt.Println(rcols)
	}
	ddl :=""
	ddl = fmt.Sprintf("CREATE TABLE R%v (RN BIGINT primary key,%v)",table.Id.Int64,rcols)
	_,err := IDB().Exec(ddl)
	if err!=nil {
		panic(err)
	}
	ddl = fmt.Sprintf("CREATE TABLE H%v (RN BIGINT primary key,%v)",table.Id.Int64,hcols)
	_,err = IDB().Exec(ddl)
	if err!=nil {
		panic(err)
	}
}
*/
/*
func GetMetadataInfoHC() []TableInfo {
	table1 := "mt.contracts2"
	table2 := "cra.liabilities2"
	result := []TableInfo{
		TableInfo{
			Id: sql.NullInt64{Int64:1,Valid:true},
			Schema: sql.NullString{String:"MT",Valid:true},
			TableName: sql.NullString{String:"CONTRACTS",Valid:true},
			DumpFileName:sql.NullString{String:table1+".txt"},
			Columns: []ColumnInfo {
				ColumnInfo{
					Id: sql.NullInt64{Int64:1001, Valid:true},
					ColumnName: sql.NullString{String:"CONTRACT_ID", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1002, Valid:true},
					ColumnName: sql.NullString{String:"CONTRACT_TYPE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1003, Valid:true},
					ColumnName: sql.NullString{String:"PRODUCT_CODE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1004, Valid:true},
					ColumnName: sql.NullString{String:"CONTRACT_STATUS", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1005, Valid:true},
					ColumnName: sql.NullString{String:"CUSTOMER_ID", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1006, Valid:true},
					ColumnName: sql.NullString{String:"CONTRACT_NUMBER", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1007, Valid:true},
					ColumnName: sql.NullString{String:"CONTRACT_DATE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1008, Valid:true},
					ColumnName: sql.NullString{String:"CLOSING_DATE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1009, Valid:true},
					ColumnName: sql.NullString{String:"INITIAL_AMOUNT", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1010, Valid:true},
					ColumnName: sql.NullString{String:"CURRENCY", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1011, Valid:true},
					ColumnName: sql.NullString{String:"MATURITY_DATE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1012, Valid:true},
					ColumnName: sql.NullString{String:"COLLATERAL_AMOUNT", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1013, Valid:true},
					ColumnName: sql.NullString{String:"COLLATERAL_REEVALUATION_DATE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1014, Valid:true},
					ColumnName: sql.NullString{String:"INTEREST_RATE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:1015, Valid:true},
					ColumnName: sql.NullString{String:"POINT_ID", Valid:true},

				},
			},
		},
		TableInfo{
			Id:sql.NullInt64{Int64:2,Valid:true},
			Schema:sql.NullString{String:"CRA",Valid:true},
			TableName:sql.NullString{String:"LIABILITIES",Valid:true},
			DumpFileName:sql.NullString{String:table2+".txt"},
			Columns:[]ColumnInfo{
				ColumnInfo{
					Id: sql.NullInt64{Int64:2002, Valid:true},
					ColumnName: sql.NullString{String:"ID", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2003, Valid:true},
					ColumnName: sql.NullString{String:"INFORMER_CODE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2004, Valid:true},
					ColumnName: sql.NullString{String:"INFORMER_DEAL_ID", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2005, Valid:true},
					ColumnName: sql.NullString{String:"LIABILITY_DATE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2006, Valid:true},
					ColumnName: sql.NullString{String:"LIABILITY_NUMBER", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2007, Valid:true},
					ColumnName: sql.NullString{String:"LIABILITY_TYPE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2008, Valid:true},
					ColumnName: sql.NullString{String:"ORIGINAL_AMOUNT", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2009, Valid:true},
					ColumnName: sql.NullString{String:"CURRENCY", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2010, Valid:true},
					ColumnName: sql.NullString{String:"DUE_DATE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2011, Valid:true},
					ColumnName: sql.NullString{String:"COLLATERAL_TYPE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2012, Valid:true},
					ColumnName: sql.NullString{String:"COLLATERAL_VALUE", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2013, Valid:true},
					ColumnName: sql.NullString{String:"COLLATERAL_VALUE_CURRENCY", Valid:true},
				},
				ColumnInfo{
					Id: sql.NullInt64{Int64:2014, Valid:true},
					ColumnName: sql.NullString{String:"COLLATERAL_ASSESSMENT_DATE", Valid:true},
				},
			},

		},
	}
	for n,_ := range result {
		for c,_ := range result[n].Columns {
			result[n].Columns[c].TableInfo = &result[n]

		}
	}
	return result
}
*/
