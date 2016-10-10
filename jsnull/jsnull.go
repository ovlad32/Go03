package jsnull

import (
	"encoding/json"
	"database/sql"
	"fmt"
)

type NullString struct{
	sql.NullString
}

func NewNullString(value string) (result NullString) {
	result.String = value
	result.Valid = true
	return
}

func(v NullString) String() (result string) {
	if v.Valid {
		result  = fmt.Sprintf("'%v'",v.String)
	} else {
		result = "null"
	}
	return
}

type NullInt64 struct {
	sql.NullInt64
}

func NewNullInt64(val int64) (result NullInt64) {
	result.Int64 = val
	result.Valid = true
	return
}

func( v NullInt64) String() (string) {
	if v.Valid {
		return fmt.Sprintf("%v",v.Int64)
	}
	return "null"
}

type NullFloat64 struct {
	sql.NullFloat64
}

func NewNullFloat64(val float64) (result NullFloat64) {
	result.Float64 = val
	result.Valid = true
	return
}

func( v NullFloat64) String() (string) {
	if v.Valid {
		return fmt.Sprintf("%v",v.Float64)
	}
	return "null"
}

type NullBool struct {
	sql.NullBool
}

func NewNullBool(val bool) (result NullBool) {
	result.Bool = val
	result.Valid = true
	return
}

func( v NullBool) String() (string) {
	if v.Valid {
		return fmt.Sprintf("%v",v.Bool)
	}
	return "null"
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


