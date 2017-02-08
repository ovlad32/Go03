package dataflow
import (
	"astra/metadata"
	"errors"
	"fmt"
	"encoding/binary"
	"astra/nullable"
)


type DataCategoryType struct{
	*metadata.ColumnInfoType
	ByteLength         nullable.NullInt64
	IsNumeric          nullable.NullBool // if array of bytes represents a numeric value
	IsNegative         nullable.NullBool
	FloatingPointScale nullable.NullInt64
	DataCount          nullable.NullInt64
	HashUniqueCount    nullable.NullInt64
	MinStringValue     nullable.NullString
	MaxStringValue     nullable.NullString
	MinNumericValue    nullable.NullFloat64
	MaxNumericValue    nullable.NullFloat64
	IsSubHash          nullable.NullBool
	SubHash            nullable.NullInt64
}

// Type,byteLength,
//
func (dc *DataCategoryType) PopulateFromBytes(k []byte) (err error) {
	kLen := len(k)
	if kLen < 2 {
		err = errors.New(fmt.Sprintf("Can not explan category for chain of bytes %v. Too short.", k))
		return
	}

	dc.ByteLength = nullable.NewNullInt64(
		int64(binary.LittleEndian.Uint16(k[1:])),
	)


	dc.IsNumeric = nullable.NewNullBool(k[0] == 0)
	if !dc.IsNumeric.Value() {
		dc.IsSubHash = nullable.NewNullBool(kLen > 3)
		if dc.IsSubHash.Value() {
			dc.SubHash = nullable.NewNullInt64(int64(k[3]))
		}
	} else {
		dc.IsNegative = nullable.NewNullBool(((k[0] >> 0) & 0x01) > 0)
		isFp := ( k[0] >>1 ) & 0x01 == 0
		if isFp {
			dc.FloatingPointScale = nullable.NewNullInt64(int64(k[3]))
			dc.IsSubHash = nullable.NewNullBool(kLen > 4)
			if dc.IsSubHash.Value() {
				dc.SubHash = nullable.NewNullInt64(int64(k[4]))
			}
		} else {
			dc.FloatingPointScale = nullable.NewNullInt64(int64(0))
			dc.IsSubHash = nullable.NewNullBool(kLen > 3)
			if dc.IsSubHash.Value() {
				dc.SubHash = nullable.NewNullInt64(int64(k[3]))
			}
		}
	}
	return
}


func (cdc DataCategoryType) String() (result string) {
	if !cdc.IsNumeric.Value() {
		result = fmt.Sprintf("char[%v]", cdc.ByteLength.Value())
	} else {
		if cdc.IsNegative.Value() {
			result = "-"
		} else {
			result = "+"
		}
		if cdc.FloatingPointScale.Value() != 0 {
			result = fmt.Sprintf("%vF[%v,%v]",
				result,
				cdc.ByteLength,
				cdc.FloatingPointScale.Value(),
			)
		} else {
			result = fmt.Sprintf("%vI[%v]",
				result,
				cdc.ByteLength,
			)
		}
	}
	if cdc.IsSubHash.Value() {
		result = fmt.Sprintf("%v(%v)", result, cdc.SubHash.Value())
	}
	return
}

