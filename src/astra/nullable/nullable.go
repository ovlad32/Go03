package nullable

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

//-------------------------------------------------------------------------------
type NullString struct {
	InternalValue sql.NullString
}

func NewNullString(value string) (result NullString) {
	result.InternalValue.String = value
	result.InternalValue.Valid = true
	return
}

func (n *NullString) Scan(value interface{}) error {
	return n.InternalValue.Scan(value)
}

func (n *NullString) Value() string {
	return n.InternalValue.String
}
func (n *NullString) Valid() bool {
	return n.InternalValue.Valid
}

func (n NullString) String() (result string) {
	if n.Valid() {
		result = n.InternalValue.String
	} else {
		result = "null"
	}
	return
}

func (n NullString) SQLString() (result string) {
	if n.Valid() {
		result = "'" + strings.Replace(n.InternalValue.String, "'", "''", -1) + "'"
	} else {
		result = "null"
	}
	return
}

func (s NullString) MarshalJSON() ([]byte, error) {
	if s.InternalValue.Valid {
		return json.Marshal(s.InternalValue.String)
	} else {
		return json.Marshal(nil)
	}
}

//-------------------------------------------------------------------------------
type NullInt64 struct {
	InternalValue sql.NullInt64
}

func NewNullInt64(val int64) (result NullInt64) {
	result.InternalValue.Int64 = val
	result.InternalValue.Valid = true
	return
}

func (n *NullInt64) Scan(value interface{}) error {
	return n.InternalValue.Scan(value)
}

func (n NullInt64) Value() int64 {
	return n.InternalValue.Int64
}
func (n NullInt64) Valid() bool {
	return n.InternalValue.Valid
}
func (n NullInt64) String() string {
	if n.Valid() {
		return fmt.Sprintf("%v", n.InternalValue.Int64)
	}
	return "null"
}

func (n *NullInt64) Reference() *int64 {
	return &(n.InternalValue.Int64)
}

func (n *NullInt64) MarshalJSON() ([]byte, error) {
	if n.Valid() {
		return json.Marshal(n.InternalValue.Int64)
	} else {
		return json.Marshal(nil)
	}
}

//-------------------------------------------------------------------------------
type NullFloat64 struct {
	InternalValue sql.NullFloat64
}

func NewNullFloat64(val float64) (result NullFloat64) {
	result.InternalValue.Float64 = val
	result.InternalValue.Valid = true
	return
}

func (n *NullFloat64) Scan(value interface{}) error {
	return n.InternalValue.Scan(value)
}

func (n *NullFloat64) Value() float64 {
	return n.InternalValue.Float64
}
func (n *NullFloat64) Valid() bool {
	return n.InternalValue.Valid
}
func (n NullFloat64) String() string {
	if n.Valid() {
		return fmt.Sprintf("%v", n.InternalValue.Float64)
	}
	return "null"
}

func (v *NullFloat64) Reference() *float64 {
	return &v.InternalValue.Float64
}

func (n NullFloat64) MarshalJSON() ([]byte, error) {
	if n.InternalValue.Valid {
		return json.Marshal(n.InternalValue.Float64)
	} else {
		return json.Marshal(nil)
	}
}

//-------------------------------------------------------------------------------
type NullBool struct {
	InternalValue sql.NullBool
}

func NewNullBool(val bool) (result NullBool) {
	result.InternalValue.Bool = val
	result.InternalValue.Valid = true
	return
}

func (n NullBool) String() string {
	if n.Valid() {
		return fmt.Sprintf("%v", n.InternalValue.Bool)
	}
	return "null"
}

func (n *NullBool) Scan(value interface{}) error {
	return n.InternalValue.Scan(value)
}
func (n *NullBool) Value() bool {
	return n.InternalValue.Bool
}

func (n *NullBool) Valid() bool {
	return n.InternalValue.Valid
}

func (n NullBool) Reference() *bool {
	return &n.InternalValue.Bool
}

func (n NullBool) MarshalJSON() ([]byte, error) {
	if n.Valid() {
		return json.Marshal(n.InternalValue.Bool)
	} else {
		return json.Marshal(nil)
	}
}
