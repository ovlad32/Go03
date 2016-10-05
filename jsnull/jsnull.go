package Null

import (
	"encoding/json"
	"database/sql"
)

type NullString struct {
	sql.NullString
}

type NullInt64 struct {
	sql.NullInt64
}

type NullFloat64 struct {
	sql.NullFloat64
}
type NullBool struct {
	sql.NullBool
}

func (s NullString) MarshalJSON() ([]byte, error) {
	if s.Valid {
		return json.Marshal(s.String)
	} else {
		return json.Marshal(nil)
	}
}
func (s NullInt64) MarshalJSON() ([]byte, error) {
	if s.Valid {
		return json.Marshal(s.Int64)
	} else {
		return json.Marshal(nil)
	}
}

func (s NullBool) MarshalJSON() ([]byte, error) {
	if s.Valid {
		return json.Marshal(s.Bool)
	} else {
		return json.Marshal(nil)
	}
}
func (s NullFloat64) MarshalJSON() ([]byte, error) {
	if s.Valid {
		return json.Marshal(s.Float64)
	} else {
		return json.Marshal(nil)
	}
}


