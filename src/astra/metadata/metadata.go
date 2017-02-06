package metadata

import (
	"astra/nullable"
)

var packageName = "astra/metadata"

type MetadataType struct {
	Id               nullable.NullInt64
	Index            nullable.NullString
	Indexed          nullable.NullString
	Version          nullable.NullInt64
	DatabaseConfigId nullable.NullInt64
	IndexedKeys      nullable.NullString
}
