package dataflow

import (
	"astra/nullable"
	"context"
	"fmt"
	"sync"
	"astra/B8"
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
		var wg sync.WaitGroup
		dc.stringAnalysisChan = make(chan string,1000)
		wg.Add(1)
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
			wg.Done()
		}()
		go func() {
			wg.Wait()
			close(dc.stringAnalysisChan)
		}()
	}

	if dc.numericAnalysisChan == nil {
		var wg sync.WaitGroup
		dc.numericAnalysisChan = make(chan float64,100)
		wg.Add(1)
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
			wg.Done()
		}()

		go func() {
			wg.Wait()
			close(dc.numericAnalysisChan)
		}()
	}
	return
}



func (dc *DataCategoryType) RunStorage(
	ctx context.Context,
	storagePath string,
	columnId int64,
	) (err error){
	//TODO: check emptyness
	if dc.columnDataChan == nil {
		var wg sync.WaitGroup
		offsetBytes := make([]byte,0,8)
		dc.columnDataChan = make(chan *ColumnDataType,1000)

		wg.Add(1)
		go func() {
			writtenTx :=  uint64(0);
			outer:
			for {
				select {
				case <-ctx.Done():
					break outer
				case columnData, opened := <-dc.columnDataChan:
					if !opened {
						break outer
					}
					offsetBytes = B8.Clear(offsetBytes)
					B8.UInt64ToBuff(offsetBytes,columnData.LineOffset)
					dc.hashStorage.rootBucket.Put(columnData.HashValue,offsetBytes)
					writtenTx++
					if writtenTx >= 1000 {
						writtenTx = 0
						err = dc.hashStorage.currentTx.Commit();
						if err != nil {
							// TODO: change to channel
							//return err
						}
						dc.hashStorage.currentTx,err = dc.hashStorage.storage.Begin(true)
						if err != nil {
							// TODO: change to channel
							//return err
						}
					}
				}
			}
			wg.Done()
		}()
		go func() {
			wg.Wait()
			close(dc.columnDataChan)
		}()
	}

	return
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
func (dc *DataCategoryType) OpenHashStorage(
	ctx context.Context,
	pathToStorageDir string,
	columnID int64,
) (err error) {
	dc.hashStorage = &boltStorageGroupType{}
	err = dc.hashStorage.Open(ctx, "hash", pathToStorageDir, columnID, dc.Key())
	return
}
