package dataflow

import (
	"sparsebitset"
	"fmt"
	"strconv"
	"astra/nullable"
)

type ComplexKeyType struct {
	TableInfo        *TableInfoType
	Columns          ColumnInfoArrayType
	ColumnPositions  []int
	FirstBitset      *sparsebitset.BitSet
	ComplexKeyInfoId int64
}

type ComplexPKDupDataType struct {
	Data       []*[]byte
	LineNumber uint64
}



func (pkc ComplexKeyType) Description() string {
	return "Key Data Hash"
}

func (pkc *ComplexKeyType) BitSet() (*sparsebitset.BitSet, error) {
	return pkc.FirstBitset, nil
}

func (pkc ComplexKeyType) FileName() (string, error) {
	return fmt.Sprintf("%v.KeyHash.bitset",
		pkc.ComplexKeyInfoId,
	), nil
}

func (pkc ComplexKeyType) ColumnIndexString() (result string) {
	result = ""
	for index, column := range pkc.Columns {
		if index == 0 {
			result = strconv.FormatInt(int64(column.Id.Value()), 10)
		} else {
			result = result + "-" + strconv.FormatInt(int64(column.Id.Value()), 10)
		}
	}
	return result
}

type ComplexPKCombinationType struct {
	*ComplexKeyType
	//	Columns          ColumnArrayType
	ComplexForeignKeys    map[*TableInfoType][]*ComplexKeyType
	cardinality           uint64
	lastSortedColumnIndex int
	duplicateBitset       *sparsebitset.BitSet
	duplicatesByHash      map[uint32][]*ComplexPKDupDataType
}

func (pkc *ComplexPKCombinationType) InitializeInternals() {
	pkc.ReinitializeInternals()
	pkc.FirstBitset = sparsebitset.New(0)
	pkc.ColumnPositions = make([]int, len(pkc.Columns))
	for keyColumnIndex, column := range pkc.Columns {
		for tableColumnIndex := 0; tableColumnIndex < len(column.TableInfo.Columns); tableColumnIndex++ {
			if column.Id.Value() == column.TableInfo.Columns[tableColumnIndex].Id.Value() {
				pkc.ColumnPositions[keyColumnIndex] = tableColumnIndex
			}
		}
	}
}

func (pkc *ComplexPKCombinationType) ReinitializeInternals() {
	pkc.duplicateBitset = sparsebitset.New(0)
	pkc.duplicatesByHash = make(map[uint32][]*ComplexPKDupDataType)
}

func (pkc *ComplexPKCombinationType) ResetDuplicateStructures() {
	pkc.duplicateBitset = nil
	pkc.duplicatesByHash = nil
}





func (pkc *ComplexPKCombinationType) Reset() {
	pkc.FirstBitset = nil
	pkc.duplicateBitset = nil
	pkc.duplicatesByHash = nil
}

func (pkc *ComplexPKCombinationType) NewComplexKeyInfo() *ComplexKeyInfoType {

	complexKey := &ComplexKeyInfoType{
		TableInfo:       pkc.Columns[0].TableInfo,
		TableInfoId:     pkc.Columns[0].TableInfo.Id,
		KeyType:         nullable.NewNullString("P"),
		ProcessingStage: nullable.NewNullString("N"),
		ColumnCount:     nullable.NewNullInt64(int64(len(pkc.Columns))),
		Columns:         make([]*ComplexKeyColumnInfoType, 0, len(pkc.Columns)),
	}

	for position, column := range pkc.Columns {
		complexKey.Columns = append(
			complexKey.Columns,
			&ComplexKeyColumnInfoType{
				ColumnInfoId: column.Id,
				Position:     nullable.NewNullInt64(int64(position + 1)),
				ComplexKey:   complexKey,
			},
		)
	}
	return complexKey
}