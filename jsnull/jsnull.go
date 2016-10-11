package jsnull

import (
	"encoding/json"
	"database/sql"
	"fmt"
)

//-------------------------------------------------------------------------------
type NullString struct{
	internal sql.NullString
}

func NewNullString(value string) (result NullString) {
	result.internal.String = value
	result.internal.Valid = true
	return
}

func (n *NullString) Scan(value interface{}) error {
	return n.internal.Scan(value)
}

func (n *NullString) Value() string {
	return n.internal.String
}
func (n *NullString) Valid() bool {
	return n.internal.Valid
}



func(v NullString) String() (result string) {
	if v.Valid() {
		result  = fmt.Sprintf("'%v'",v.internal.String)
	} else {
		result = "null"
	}
	return
}


func (s NullString) MarshalJSON() ([]byte, error) {
	if s.internal.Valid {
		return json.Marshal(s.internal.String)
	} else {
		return json.Marshal(nil)
	}
}


//-------------------------------------------------------------------------------
type NullInt64 struct {
	internal sql.NullInt64
}

func NewNullInt64(val int64) (result NullInt64) {
	result.internal.Int64 = val
	result.internal.Valid = true
	return
}

func (n *NullInt64) Scan(value interface{}) error {
	return n.internal.Scan(value)
}

func (n *NullInt64) Value() int64 {
	return n.internal.Int64
}
func (n *NullInt64) Valid() bool {
	return n.internal.Valid
}
func( v NullInt64) String() (string) {
	if v.Valid() {
		return fmt.Sprintf("%v",v.internal.Int64)
	}
	return "null"
}

func(v NullInt64) Reference() *int64 {
	return &v.internal.Int64
}

func (s NullInt64) MarshalJSON() ([]byte, error) {
	if s.Valid() {
		return json.Marshal(s.internal.Int64)
	} else {
		return json.Marshal(nil)
	}
}

//-------------------------------------------------------------------------------
type NullFloat64 struct {
	internal sql.NullFloat64
}

func NewNullFloat64(val float64) (result NullFloat64) {
	result.internal.Float64 = val
	result.internal.Valid = true
	return
}

func (n *NullFloat64) Scan(value interface{}) error {
	return n.internal.Scan(value)
}

func (n *NullFloat64) Value() float64 {
	return n.internal.Float64
}
func (n *NullFloat64) Valid() bool {
	return n.internal.Valid
}
func( v NullFloat64) String() (string) {
	if v.Valid() {
		return fmt.Sprintf("%v",v.internal.Float64)
	}
	return "null"
}

func(v NullFloat64) Reference() *float64 {
	return &v.internal.Float64
}

func (s NullFloat64) MarshalJSON() ([]byte, error) {
	if s.internal.Valid {
		return json.Marshal(s.internal.Float64)
	} else {
		return json.Marshal(nil)
	}
}

//-------------------------------------------------------------------------------
type NullBool struct {
	internal sql.NullBool
}

func NewNullBool(val bool) (result NullBool) {
	result.internal.Bool = val
	result.internal.Valid = true
	return
}

func( v NullBool) String() (string) {
	if v.Valid() {
		return fmt.Sprintf("%v",v.internal.Bool)
	}
	return "null"
}

func (n *NullBool) Scan(value interface{}) error {
	return n.internal.Scan(value)
}
func (n *NullBool) Value() bool {
	return n.internal.Bool
}

func (n *NullBool) Valid() bool {
	return n.internal.Valid
}

func(v NullBool) Reference() *bool {
	return &v.internal.Bool
}

func (s NullBool) MarshalJSON() ([]byte, error) {
	if s.Valid() {
		return json.Marshal(s.internal.Bool)
	} else {
		return json.Marshal(nil)
	}
}


