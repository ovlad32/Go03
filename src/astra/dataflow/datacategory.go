package dataflow

import (
	"astra/nullable"
	"context"
	"fmt"
)

type DataCategorySimpleType struct {
	ByteLength         int
	IsNumeric          bool
	IsNegative         bool
	FloatingPointScale int
	IsSubHash          bool
	SubHash            uint
}

func (simple *DataCategorySimpleType) Key() (result string) {
	if !simple.IsNumeric {
		result = fmt.Sprintf("C%v", simple.ByteLength)
	} else {
		if simple.FloatingPointScale > 0 {
			if simple.IsNegative {
				result = "M"
			} else {
				result = "F"
			}
			result = result + fmt.Sprintf("%vP%v", simple.ByteLength, simple.FloatingPointScale)
		} else {
			if simple.IsNegative {
				result = "I"
			} else {
				result = "N"
			}
			result = result + fmt.Sprintf("%v", simple.ByteLength)
		}
	}
	if simple.IsSubHash {
		result = result + fmt.Sprintf("H%v", simple.SubHash)
	}
	return
}

func (simple *DataCategorySimpleType) covert() (result *DataCategoryType) {
	result = &DataCategoryType{
		IsNumeric:  nullable.NewNullBool(simple.IsNumeric),
		ByteLength: nullable.NewNullInt64(int64(simple.ByteLength)),
	}
	if simple.IsNumeric {
		result.IsNegative = nullable.NewNullBool(simple.IsNegative)
		result.FloatingPointScale = nullable.NewNullInt64(int64(simple.FloatingPointScale))
	}

	if simple.IsSubHash {
		result.SubHash = nullable.NewNullInt64(int64(simple.SubHash))
	}
	return
}


type DataCategoryType struct {
	//column *metadata.ColumnInfoType
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
	NonNullCount       nullable.NullInt64
	SubHash            nullable.NullInt64

	stringAnalysisChan  chan string
	numericAnalysisChan chan float64

}

func (dc *DataCategoryType) RunAnalyzer(ctx context.Context) (err error){
	if dc.stringAnalysisChan == nil {
		dc.stringAnalysisChan = make(chan string,1000)
		go func() {
		outer:
			for {
				select {
				case <-ctx.Done():
					break outer
				case stringValue, opened := <-dc.stringAnalysisChan:
					if !opened {
						break outer
					}
					if dc.NonNullCount.Reference() == nil {
						dc.NonNullCount = nullable.NewNullInt64(int64(0))
					}

					(*dc.NonNullCount.Reference())++

					if !dc.MaxStringValue.Valid() || dc.MaxStringValue.Value() < stringValue {
						dc.MaxStringValue = nullable.NewNullString(stringValue)
					}
					if !dc.MinStringValue.Valid() || dc.MinStringValue.Value() > stringValue {
						dc.MinStringValue = nullable.NewNullString(stringValue)
					}
				}
			}
		}()
	}

	if dc.numericAnalysisChan == nil {
		dc.numericAnalysisChan = make(chan float64,1000)
		go func() {

		outer:
			for {
				select {
				case <-ctx.Done():
					break outer
				case floatValue, opened := <-dc.numericAnalysisChan:
					if !opened {
						break outer
					}
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
			}
		}()
	}
	return
}

func (dc *DataCategoryType) CloseAnalyzerChannels() {
	if dc.numericAnalysisChan != nil {
		close(dc.numericAnalysisChan)
	}
	if dc.stringAnalysisChan != nil {
		close(dc.stringAnalysisChan)
	}
}


func (cdc DataCategoryType) Key() (result string) {
	simple := DataCategorySimpleType{
		ByteLength:         int(cdc.ByteLength.Value()),
		IsNumeric:          cdc.IsNumeric.Value(),
		IsNegative:         cdc.IsNegative.Value(),
		FloatingPointScale: int(cdc.FloatingPointScale.Value()),
		IsSubHash:          cdc.SubHash.Valid(),
		SubHash:            uint(cdc.SubHash.Value()),
	}
	return simple.Key()
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
	if cdc.SubHash.Valid() {
		result = fmt.Sprintf("%v(%v)", result, cdc.SubHash.Value())
	}
	return
}
