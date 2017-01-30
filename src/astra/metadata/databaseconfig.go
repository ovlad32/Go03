package metadata


import "database/sql"

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
}
