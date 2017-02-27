package nullable

import (
	"database/sql"
	"fmt"
	"encoding/json"
	"strings"
)


//-------------------------------------------------------------------------------
type NullString struct {
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

func (n NullString) String() (result string) {
	if n.Valid() {
		result = n.internal.String
	} else {
		result = "null"
	}
	return
}

func (n NullString) SQLString() (result string) {
	if n.Valid() {
		result = "'"+strings.Replace(n.internal.String, "'", "''", -1)+"'"
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

func (n NullInt64) Value() int64 {
	return n.internal.Int64
}
func (n NullInt64) Valid() bool {
	return n.internal.Valid
}
func (n NullInt64) String() string {
	if n.Valid() {
		return fmt.Sprintf("%v", n.internal.Int64)
	}
	return "null"
}

func (n *NullInt64) Reference() *int64 {
	return &(n.internal.Int64)
}

func (n *NullInt64) MarshalJSON() ([]byte, error) {
	if n.Valid() {
		return json.Marshal(n.internal.Int64)
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
func (n NullFloat64) String() string {
	if n.Valid() {
		return fmt.Sprintf("%v", n.internal.Float64)
	}
	return "null"
}

func (v *NullFloat64) Reference() *float64 {
	return &v.internal.Float64
}

func (n NullFloat64) MarshalJSON() ([]byte, error) {
	if n.internal.Valid {
		return json.Marshal(n.internal.Float64)
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

func (n NullBool) String() string {
	if n.Valid() {
		return fmt.Sprintf("%v", n.internal.Bool)
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

func (n NullBool) Reference() *bool {
	return &n.internal.Bool
}

func (n NullBool) MarshalJSON() ([]byte, error) {
	if n.Valid() {
		return json.Marshal(n.internal.Bool)
	} else {
		return json.Marshal(nil)
	}
}
