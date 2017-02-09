package dataflow

import (
	"astra/metadata"
	"astra/nullable"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

type DataCategoryType struct {
	*metadata.ColumnInfoType
	ByteLength          nullable.NullInt64
	IsNumeric           nullable.NullBool // if array of bytes represents a numeric value
	IsNegative          nullable.NullBool
	FloatingPointScale  nullable.NullInt64
	DataCount           nullable.NullInt64
	HashUniqueCount     nullable.NullInt64
	MinStringValue      nullable.NullString
	MaxStringValue      nullable.NullString
	MinNumericValue     nullable.NullFloat64
	MaxNumericValue     nullable.NullFloat64
	IsSubHash           nullable.NullBool
	SubHash             nullable.NullInt64
	stringAnalysisLock  sync.Mutex
	numericAnalysisLock sync.Mutex
}

func (dc *DataCategoryType) AnalyzeStringValue(stringValue string) {
	dc.stringAnalysisLock.Lock()
	defer dc.stringAnalysisLock.Unlock()

	(*dc.NonNullCount.Reference())++

	if !dc.MaxStringValue.Valid() || dc.MaxStringValue.Value() < stringValue {
		dc.MaxStringValue = nullable.NewNullString(stringValue)
	}
	if !dc.MinStringValue.Valid() || dc.MinStringValue.Value() > stringValue {
		dc.MinStringValue = nullable.NewNullString(stringValue)
	}

	lValue := int64(len(stringValue))
	if !dc.MaxStringLength.Valid() {
		dc.MaxStringLength = nullable.NewNullInt64(lValue)
	} else if dc.MaxStringLength.Value() < lValue {
		(*dc.MaxStringLength.Reference()) = lValue

	}
	if !dc.MinStringLength.Valid() {
		dc.MinStringLength = nullable.NewNullInt64(lValue)
	} else if dc.MinStringLength.Value() > lValue {
		(*dc.MinStringLength.Reference()) = lValue

	}
}

func (dc *DataCategoryType) AnalyzeNumericValue(floatValue float64) {
	dc.numericAnalysisLock.Lock()
	defer dc.numericAnalysisLock.Unlock()

	(*dc.NumericCount.Reference())++
	if !dc.MaxNumericValue.Valid() {
		dc.MaxNumericValue = nullable.NewNullFloat64(floatValue)
	} else if dc.MaxNumericValue.Value() < floatValue {
		(*dc.MaxNumericValue.Reference()) = floatValue
	}

	if !dc.MinNumericValue.Valid() {
		dc.MinNumericValue = nullable.NewNullFloat64(floatValue)
	} else if dc.MinNumericValue.Value() > floatValue {
		(*dc.MinNumericValue.Reference()) = floatValue
	}

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
		isFp := (k[0]>>1)&0x01 == 0
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

func (cdc DataCategoryType) Key() (result string) {
	if !cdc.IsNumeric.Value() {
		result = fmt.Sprintf("C%v", cdc.ByteLength.Value())
	} else {
		if cdc.FloatingPointScale.Value() != 0 {
			if cdc.IsNegative.Value() {
				result = "M"
			} else {
				result = "F"
			}
			result = result + fmt.Sprintf("%vP%v",
				cdc.ByteLength,
				cdc.FloatingPointScale.Value(),
			)
		} else {
			if cdc.IsNegative.Value() {
				result = "I"
			} else {
				result = "N"
			}
			result = result + fmt.Sprintf("%v", cdc.ByteLength)
		}
	}
	if cdc.IsSubHash.Value() {
		result = result + fmt.Sprintf("H%v", cdc.SubHash.Value())
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
