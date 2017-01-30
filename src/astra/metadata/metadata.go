package metadata

import "database/sql"

type MetadataType struct {
	Id               sql.NullInt64
	Index            sql.NullString
	Indexed          sql.NullString
	Version          sql.NullInt64
	DatabaseConfigId sql.NullInt64
	IndexedKeys      sql.NullString
}
