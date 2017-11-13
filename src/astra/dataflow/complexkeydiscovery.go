package dataflow

import (
	"astra/nullable"
	"context"
	"fmt"
	"github.com/goinggo/tracelog"
	"sort"
	"sparsebitset"
	"strings"
	"runtime"
	"hash/fnv"
	"bytes"
)

const ColumnPairArrayInitSize = 1000

type ComplexKeyDiscoveryConfigType struct {
	ParentColumnNullAllowed bool
	FloatContentAllowed     bool
	CollisionLevel          float64
	PathToBitsetDirectory string
	NumberOfKeysPerFilePass int
	DumpReaderConfig *DumpReaderConfigType
}

type ComplexKeyDiscoveryType struct {
	config     *ComplexKeyDiscoveryConfigType
	repository *Repository
}

type ColumnPairType struct {
	ParentColumn *ColumnInfoType
	ChildColumn  *ColumnInfoType
}
type ColumnPairArrayType []*ColumnPairType

type TablePairType struct {
	ParentTable *TableInfoType
	ChildTable  *TableInfoType
}
type TablePairArrayType []*TablePairType

type TablePairMapType map[TablePairType]ColumnPairArrayType

func NewComplexKeyDiscovery(config *ComplexKeyDiscoveryConfigType) (*ComplexKeyDiscoveryType, error) {
	if config == nil {

	}
	if config.CollisionLevel <= float64(0) {

	}

	return &ComplexKeyDiscoveryType{
		config:     config,
		repository: repository,
	}, nil
}

func (d *ComplexKeyDiscoveryType) IfOnlyRow(
	column *ColumnInfoType,
) (result bool, explanation string, err error) {
	// Null existence
	funcName := "ComplexKeyDiscoveryType::IfOnlyRow"
	if !column.TableInfo.RowCount.Valid() {
		err = fmt.Errorf("RowCount statistics is empty for %v", column.TableInfo)
		tracelog.Error(err, packageName, funcName)
		return
	}

	result = column.TableInfo.RowCount.Value() < 2
	if result {
		explanation = fmt.Sprintf("Table %v has the only row",
			column.TableInfo,
		)
		tracelog.Info(packageName, funcName, explanation)
		return
	}
	return
}

func (d *ComplexKeyDiscoveryType) IfFloatNumericContent(
	column *ColumnInfoType,
) (result bool, explanation string, err error) {
	funcName := "ComplexKeyDiscoveryType::IfFloatNumericContent"
	if !d.config.ParentColumnNullAllowed {
		floatContentFound := false
		allNumeric := true
		for _, category := range column.Categories {
			if !category.NonNullCount.Valid() {
				err = fmt.Errorf("NonNullCount statistics is empty for %v", category)
				tracelog.Error(err, packageName, funcName)
				return
			}
			floatContentFound = floatContentFound || (category.IsNumeric.Value() && !category.IsInteger.Value())
			allNumeric = allNumeric && category.IsNumeric.Value()
		}
		result = allNumeric && floatContentFound
		if result {
			explanation = "has float numeric content"
			tracelog.Info(packageName, funcName, explanation)
			return
		}
	}
	return
}

func (d *ComplexKeyDiscoveryType) IfNullExists(
	column *ColumnInfoType,
) (result bool, explanation string, err error) {
	funcName := "ComplexKeyDiscoveryType::IfNullExists"

	if !column.TotalRowCount.Valid() {
		err = fmt.Errorf("TotalRowCount statistics is empty for %v", column.TableInfo)
		tracelog.Error(err, packageName, funcName)
		return
	}

	if !column.NonNullCount.Valid() {
		err = fmt.Errorf("NonNullCount statistics is empty for %v", column.TableInfo)
		tracelog.Error(err, packageName, funcName)
		return
	}

	result = uint64(column.TableInfo.RowCount.Value()) != uint64(column.NonNullCount.Value())
	if result {
		explanation = fmt.Sprintf(
			"TotalRowCount != TotalNotNullCount. %v != %v",
			uint64(column.TableInfo.RowCount.Value()),
			uint64(column.NonNullCount.Value()),
		)
		tracelog.Info(packageName, funcName, explanation)
	}
	return
}

func (d *ComplexKeyDiscoveryType) IfHashDataUnique(
	column *ColumnInfoType,
) (result bool, explanation string, err error) {
	funcName := "ComplexKeyDiscoveryType::ifNullExists"

	if !column.TotalRowCount.Valid() {
		err = fmt.Errorf("TotalRowCount statistics is empty for %v", column.TableInfo)
		tracelog.Error(err, packageName, funcName)
		return
	}
	if !column.NonNullCount.Valid() {
		err = fmt.Errorf("NonNullCount statistics is empty for %v", column.TableInfo)
		tracelog.Error(err, packageName, funcName)
		return
	}

	result = column.TotalRowCount.Value() == column.HashUniqueCount.Value()
	if result {
		explanation = fmt.Sprintf(
			"Set of UniqueHashCount == TotalRowCount. %v == %v",
			column.HashUniqueCount.Value(),
			column.TotalRowCount.Value(),
		)
		tracelog.Info(packageName, funcName, explanation)

		return
	}
	return
}

func (d *ComplexKeyDiscoveryType) IsNotParent(
	parentColumn *ColumnInfoType,
) (isNotParent bool, explanation string, err error) {
	funcName := "ComplexKeyDiscoveryType::IsNotParent"

	if isNotParent, explanation, err = d.IfOnlyRow(parentColumn); err != nil || isNotParent {
	} else if isNotParent, explanation, err = d.IfFloatNumericContent(parentColumn); err != nil || isNotParent {
	} else if isNotParent, explanation, err = d.IfNullExists(parentColumn); err != nil || isNotParent {
	} else if isNotParent, explanation, err = d.IfHashDataUnique(parentColumn); err != nil || isNotParent {
	} else {
		return false, "", nil
	}
	if err != nil {
		err = fmt.Errorf(
			"checking column %v for being Parent: %v ",
			parentColumn,
			err,
		)
		tracelog.Error(err, packageName, funcName)
	} else if isNotParent {
		explanation = fmt.Sprintf(
			"Columns %v is not Parent: %v",
			parentColumn,
			explanation,
		)
	}
	return isNotParent, explanation, err
}

func (d *ComplexKeyDiscoveryType) IfMoreCategories(
	childColumn, parentColumn *ColumnInfoType,
) (isNotChild bool, explanation string, err error) {
	funcName := "ComplexKeyDiscoveryType.IfNotInParentCategories"

	childCategoryCount := len(childColumn.Categories)
	if isNotChild = childCategoryCount == 0; isNotChild {
		explanation = fmt.Sprintf(
			"number of DataCategories in %v is 0",
			childColumn,
		)
		tracelog.Info(packageName, funcName, explanation)
		return
	}

	parentCategoryCount := len(parentColumn.Categories)
	if isNotChild = childCategoryCount > parentCategoryCount; isNotChild {
		explanation = fmt.Sprintf(
			"number of data categories of Child column is bigger than "+
				"number of data categories of Parent column: %v > %v",
			childCategoryCount,
			parentCategoryCount,
		)
		tracelog.Info(packageName, funcName, explanation)
		return
	}
	return
}

func (d *ComplexKeyDiscoveryType) IfNotFitByCategories(
	childColumn, parentColumn *ColumnInfoType,
) (isNotChild bool, explanation string, err error) {
	funcName := "ComplexKeyDiscoveryType::IfNotFitByCategories"

	for categoryKey, childCategory := range childColumn.Categories {
		if parentCategory, found := parentColumn.Categories[categoryKey]; !found {
			isNotChild = true
			explanation = fmt.Sprintf(
				"child data category (code: %v) is not found in "+
					" parent data categories",
				categoryKey,
			)
			return
		} else {
			if !childCategory.IsNumeric.Valid() {
				err = fmt.Errorf("child IsNumeric statistics is empty in %v", childCategory)
				tracelog.Error(err, packageName, funcName)
				return
			}
			if !parentCategory.IsNumeric.Valid() {
				err = fmt.Errorf("parent IsNumeric statistics is empty in %v", parentCategory)
				tracelog.Error(err, packageName, funcName)
				return
			}

			if childCategory.IsNumeric.Value() && parentCategory.IsNumeric.Value() {
				if !childCategory.IsInteger.Valid() {
					err = fmt.Errorf("child IsInteger statistics is empty in %v", childCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !parentCategory.IsInteger.Valid() {
					err = fmt.Errorf("parent IsInteger statistics is empty in %v", parentCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}

				if childCategory.IsInteger.Value() && parentCategory.IsInteger.Value() {
					if !childCategory.ItemUniqueCount.Valid() {
						err = fmt.Errorf("child ItemUniqueCount statistics is empty in %v", childCategory)
						tracelog.Error(err, packageName, funcName)
						return
					}
					if !parentCategory.ItemUniqueCount.Valid() {
						err = fmt.Errorf("parent ItemUniqueCount statistics is empty in %v", parentCategory)
						tracelog.Error(err, packageName, funcName)
						return
					}

					isNotChild = childCategory.ItemUniqueCount.Value() > parentCategory.ItemUniqueCount.Value()
					if isNotChild {
						explanation = fmt.Sprintf(
							"child ItemUniqueCount > Parent ItemUniqueCount in DataCategory %v: %v > %v",
							categoryKey,
							childCategory.ItemUniqueCount.Value(),
							parentCategory.ItemUniqueCount.Value(),
						)
						tracelog.Info(packageName, funcName, explanation)
						return
					}
				}

				if !childCategory.MinNumericValue.Valid() {
					err = fmt.Errorf("child MinNumericValue statistics is empty in %v", childCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !parentCategory.MinNumericValue.Valid() {
					err = fmt.Errorf("parent MinNumericValue statistics is empty in %v", parentCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}

				if !childCategory.MaxNumericValue.Valid() {
					err = fmt.Errorf("child MaxNumericValue statistics is empty in %v", childCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !parentCategory.MaxNumericValue.Valid() {
					err = fmt.Errorf("parent MaxNumericValue statistics is empty in %v", parentCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				isNotChild = childCategory.MaxNumericValue.Value() > parentCategory.MaxNumericValue.Value()
				if isNotChild {
					explanation = fmt.Sprintf(
						"child MaxNumericValue > parent MaxNumericValue in DataCategory %v: %v > %v",
						categoryKey,
						childCategory.MaxNumericValue.Value(),
						parentCategory.MaxNumericValue.Value(),
					)
					tracelog.Info(packageName, funcName, explanation)
					return
				}
				isNotChild = childCategory.MinNumericValue.Value() < parentCategory.MinNumericValue.Value()
				if isNotChild {
					explanation = fmt.Sprintf(
						"child MinNumericValue < parent MinNumericValue in DataCategory %v: %v < %v",
						categoryKey,
						childCategory.MinNumericValue.Value(),
						parentCategory.MinNumericValue.Value())
					tracelog.Info(packageName, funcName, explanation)
					return
				}
			} else {
				if !childCategory.ItemUniqueCount.Valid() {
					err = fmt.Errorf("child ItemUniqueCount statistics is empty in %v", childCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !parentCategory.ItemUniqueCount.Valid() {
					err = fmt.Errorf("parent ItemUniqueCount statistics is empty in %v", parentCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}

				isNotChild = childCategory.ItemUniqueCount.Value() > parentCategory.ItemUniqueCount.Value()
				if isNotChild {
					explanation = fmt.Sprintf(
						"child ItemUniqueCount > parent ItemUniqueCount in DataCategory %v: %v > %v",
						categoryKey,
						childCategory.ItemUniqueCount.Value(),
						parentCategory.ItemUniqueCount.Value(),
					)
					tracelog.Info(packageName, funcName, explanation)
					return
				}

				isNotChild = float64(childCategory.HashUniqueCount.Value()) > float64(parentCategory.HashUniqueCount.Value())*d.config.CollisionLevel
				if isNotChild {
					explanation = fmt.Sprintf(
						"child HashUniqueCount > Parent HashUniqueCount * ratio(%v) in DataCategory %v: %v > %v",
						d.config.CollisionLevel,
						categoryKey,
						childCategory.HashUniqueCount.Value(),
						uint64(float64(parentCategory.HashUniqueCount.Value())*d.config.CollisionLevel),
					)
					tracelog.Info(packageName, funcName, explanation)
					return
				}
			}

		}
	}

	return
}

func (d *ComplexKeyDiscoveryType) IfNotFitByHashCount(
	childColumn, parentColumn *ColumnInfoType,
) (isNotChild bool, explanation string, err error) {
	funcName := "ComplexKeyDiscoveryType.IfNotFitByHashCount"
	if !childColumn.HashUniqueCount.Valid() {
		err = fmt.Errorf("cshild HashUniqueCount statistics is empty in %v", childColumn)
		tracelog.Error(err, packageName, funcName)
		return
	}

	if !parentColumn.HashUniqueCount.Valid() {
		err = fmt.Errorf("child HashUniqueCount statistics is empty in %v", parentColumn)
		tracelog.Error(err, packageName, funcName)
		return
	}

	isNotChild =
		float64(childColumn.HashUniqueCount.Value()) >
			float64(parentColumn.HashUniqueCount.Value())*d.config.CollisionLevel
	if isNotChild {
		explanation = fmt.Sprintf(
			"Column %v is not Child toward %v: HashUniqueCountFK > HashUniqueCountPK*CollisionLevel(%v); %v > %v",
			childColumn, parentColumn, d.config.CollisionLevel,
			parentColumn.HashUniqueCount.Value(),
			uint64(float64(parentColumn.HashUniqueCount.Value())*d.config.CollisionLevel),
		)
		tracelog.Info(packageName, funcName, explanation)
		return
	}
	return
}
func (d *ComplexKeyDiscoveryType) IsNotChild(
	childColumn, parentColumn *ColumnInfoType,
) (isNotChild bool, explanation string, err error) {
	funcName := "ComplexKeyDiscoveryType.IsNotChild"
	if isNotChild, explanation, err = d.IfOnlyRow(childColumn); err != nil || isNotChild {
	} else if isNotChild, explanation, err = d.IfMoreCategories(childColumn, parentColumn); err != nil || isNotChild {
	} else if isNotChild, explanation, err = d.IfNotFitByCategories(childColumn, parentColumn); err != nil || isNotChild {
	} else if isNotChild, explanation, err = d.IfNotFitByHashCount(childColumn, parentColumn); err != nil || isNotChild {
	} else {
		return false, "", nil
	}

	if err != nil {
		err = fmt.Errorf(
			"checking column %v for being Child to Parent %v: %v ",
			childColumn, parentColumn,
			err,
		)
		tracelog.Error(err, packageName, funcName)
	} else if isNotChild {
		explanation = fmt.Sprintf(
			"Columns %v is not Child toward Parent %v: %v",
			childColumn, parentColumn,
			explanation,
		)
	}
	return
}

func (d *ComplexKeyDiscoveryType) ExtractColumns(
	ctx context.Context,
	metadataIds []int64,
) (result ColumnInfoArrayType, err error) {
	funcName := "ComplexKeyDiscoveryType.ExtractColumns"

	for _, id := range metadataIds {
		meta, err := d.repository.MetadataById(nullable.NewNullInt64(id))
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}

		tables, err := d.repository.TableInfoByMetadata(meta)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}

		for _, table := range tables {
			exTable := ExpandFromMetadataTable(table)
			for _, column := range exTable.Columns {
				column.Categories, err = d.repository.DataCategoryByColumnId(column)
				column.AggregateDataCategoryStatistics()
				result = append(result, column)
			}
		}
	}
	return
}

func (d *ComplexKeyDiscoveryType) ComposeAndFilterColumnPairsByFeatures(
	ctx context.Context,
	columns ColumnInfoArrayType,
) (result ColumnPairArrayType, err error) {
	funcName := "ComplexKeyDiscoveryType::ComposeAndFilterColumnPairsByFeatures"

	pairsFilteredByFeatures := make(ColumnPairArrayType, 0, ColumnPairArrayInitSize)
	var explanation string

	nonParentColumns := make(map[*ColumnInfoType]bool)

	for leftIndex, leftColumn := range columns {
		if !leftColumn.NonNullCount.Valid() {
			err = leftColumn.AggregateDataCategoryStatistics()
			if err != nil {
				return
			}
		}

		var leftNonParent, rightNonParent, columnFound bool
		if leftNonParent, columnFound = nonParentColumns[leftColumn]; !columnFound {
			leftNonParent, explanation, err = d.IsNotParent(leftColumn)
			if err != nil {
				return
			}
			if leftNonParent {
				tracelog.Info(packageName, funcName, explanation)
				nonParentColumns[leftColumn] = leftNonParent
			}

		}

		for rightIndex := leftIndex + 1; rightIndex < len(columns); rightIndex++ {

			rightColumn := columns[rightIndex]
			if !rightColumn.HashUniqueCount.Valid() {
				err = rightColumn.AggregateDataCategoryStatistics()
				if err != nil {
					return
				}

			}
			rightNonParent = false
			if rightNonParent, columnFound = nonParentColumns[rightColumn]; !columnFound {
				rightNonParent, explanation, err = d.IsNotParent(rightColumn)
				if err != nil {
					return
				}
				if rightNonParent {
					tracelog.Info(packageName, funcName, explanation)
					nonParentColumns[rightColumn] = rightNonParent
				}
			}

			if rightNonParent && leftNonParent {
				continue
			}
			if !leftNonParent {
				rightNonChild, explanation, err := d.IsNotChild(rightColumn, leftColumn)
				if err != nil {
					tracelog.Info(packageName, funcName, explanation)
					return
				}
				if !rightNonChild {
					pair := &ColumnPairType{
						ParentColumn: leftColumn,
						ChildColumn:  rightColumn,
					}
					pairsFilteredByFeatures = append(pairsFilteredByFeatures, pair)
				}
			}
			if !rightNonParent {
				leftNonChild, explanation, err := d.IsNotChild(leftColumn, rightColumn)
				if err != nil {
					tracelog.Info(packageName, funcName, explanation)
					return
				}
				if !leftNonChild {
					pair := &ColumnPairType{
						ParentColumn: rightColumn,
						ChildColumn:  leftColumn,
					}
					pairsFilteredByFeatures = append(pairsFilteredByFeatures, pair)
				}
			}

		}
	}
	result = pairsFilteredByFeatures
	return

}

func (d *ComplexKeyDiscoveryType) FilterColumnPairsByBitSets(
	ctx context.Context,
	columnPairs ColumnPairArrayType,
) (result ColumnPairArrayType, err error) {

	funcName := "ComplexKeyDiscoveryType::FilterColumnPairsByBitSets"
	sort.Slice(
		columnPairs,
		func(i, j int) bool {
			if columnPairs[i].ParentColumn.HashUniqueCount.Value() == columnPairs[j].ParentColumn.HashUniqueCount.Value() {
				return columnPairs[i].ParentColumn.Id.Value() < columnPairs[j].ParentColumn.HashUniqueCount.Value()
			} else {
				return columnPairs[i].ParentColumn.HashUniqueCount.Value() < columnPairs[j].ParentColumn.HashUniqueCount.Value()
			}
		},
	)

	var lastParentColumn *ColumnInfoType

	analyzeItemBitsetFunc := func(ctx context.Context, dataCategoryPK, dataCategoryFK *DataCategoryType) (bool, error) {
		if dataCategoryPK.Stats.ItemBitset == nil {
			dataCategoryPK.Stats.ItemBitset = sparsebitset.New(0)
			err = dataCategoryPK.ReadBitsetFromDisk(ctx, d.config.PathToBitsetDirectory, ItemBitsetSuffix)
		}
		if dataCategoryFK.Stats.ItemBitset == nil {
			dataCategoryFK.Stats.ItemBitset = sparsebitset.New(0)
			err = dataCategoryFK.ReadBitsetFromDisk(ctx, d.config.PathToBitsetDirectory, ItemBitsetSuffix)
		}
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return false, err
		}
		var cardinality uint64
		if dataCategoryFK.Stats.ItemBitset == nil {
			tracelog.Info(packageName, funcName, "Item Bitset for %v (%v) is null ", dataCategoryFK.Column, dataCategoryFK.Key)
			return false, nil
		} else {
			cardinality, err = dataCategoryFK.Stats.ItemBitset.IntersectionCardinality(dataCategoryPK.Stats.ItemBitset)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return false, err
			}
		}
		result := cardinality == dataCategoryFK.Stats.ItemBitsetCardinality
		if !result {
			tracelog.Info(funcName, packageName,
				"Column %v is not FKColumn to %v for DataCategory %v: IntersectionCardinality != FkCardinality for Content values %v != %v",
				dataCategoryFK.Column, dataCategoryPK.Column, dataCategoryFK.Key,
				cardinality, dataCategoryFK.Stats.ItemBitsetCardinality,
			)
		}
		return result, nil
	}

	analyzeHashBitsetFunc := func(ctx context.Context, dataCategoryPK, dataCategoryFK *DataCategoryType) (bool, error) {
		if dataCategoryPK.Stats.HashBitset == nil {
			dataCategoryPK.Stats.HashBitset = sparsebitset.New(0)
			err = dataCategoryPK.ReadBitsetFromDisk(ctx, d.config.PathToBitsetDirectory, HashBitsetSuffix)
		}
		if dataCategoryFK.Stats.HashBitset == nil {
			dataCategoryFK.Stats.HashBitset = sparsebitset.New(0)
			err = dataCategoryFK.ReadBitsetFromDisk(ctx, d.config.PathToBitsetDirectory, HashBitsetSuffix)
		}
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return false, err
		}
		var cardinality uint64
		if dataCategoryFK.Stats.HashBitset == nil {
			tracelog.Info(packageName, funcName, "Hash Bitset for %v (%v) is null ", dataCategoryFK.Column.Id, dataCategoryFK.Key)
			return false, nil
		} else {
			cardinality, err = dataCategoryFK.Stats.HashBitset.IntersectionCardinality(dataCategoryPK.Stats.HashBitset)

			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return false, err
			}
		}
		result := cardinality == dataCategoryFK.Stats.HashBitsetCardinality
		if !result {
			tracelog.Info(funcName, packageName,
				"Column %v is not FKColumn to %v for DataCategory %v: IntersectionCardinality != FkCardinality for Hash values %v != %v",
				dataCategoryFK.Column, dataCategoryPK.Column, dataCategoryFK.Key,
				cardinality, dataCategoryFK.Stats.HashBitsetCardinality,
			)
		}
		return result, nil
	}

	traversePairs := func(
		ctx context.Context,
		pairs ColumnPairArrayType,
		processPairFunc func(ctx context.Context, dataCategoryPK, dataCategoryFK *DataCategoryType) (bool, error),
	) (result ColumnPairArrayType, err error) {

		for _, pair := range pairs {
			if lastParentColumn != nil {
				if lastParentColumn != pair.ParentColumn {
					lastParentColumn.ResetBitset(ItemBitsetSuffix)
				}
			}
			var processingPairResult = true
			for dataCategoryKey, dataCategoryFK := range pair.ChildColumn.Categories {
				if dataCategoryPK, found := pair.ParentColumn.Categories[dataCategoryKey]; !found {
					err = fmt.Errorf(
						"the second pass for column pair PKColumn:%v - FKColumn:"+
							"%v doesn't reveal datacategory for the key code %v",
						pair.ParentColumn,
						pair.ChildColumn,
						dataCategoryKey,
					)
					tracelog.Error(err, packageName, funcName)
					return
				} else {
					processingPairResult, err = processPairFunc(ctx, dataCategoryPK, dataCategoryFK)
					if err != nil {
						tracelog.Error(err, packageName, funcName)
						return nil, err
					}
					if !processingPairResult {
						break
					}
				}
			}
			if processingPairResult {
				if result == nil {
					result = make(ColumnPairArrayType, 0, ColumnPairArrayInitSize)
				}
				result = append(result, pair)
			}
		}
		return result, nil
	}

	filteredByContent, err := traversePairs(ctx, columnPairs, analyzeItemBitsetFunc)
	if err != nil {

	}
	if filteredByContent == nil {
		return
	}
	for _, pair := range columnPairs {
		pair.ParentColumn.ResetBitset(ItemBitsetSuffix)
		pair.ChildColumn.ResetBitset(ItemBitsetSuffix)
	}

	result, err = traversePairs(ctx, filteredByContent, analyzeHashBitsetFunc)
	if err != nil {

	}
	return result, err

}

func (d *ComplexKeyDiscoveryType) ComposeTablePairs(
	ctx context.Context,
	columnPairs ColumnPairArrayType,
) (result TablePairMapType, err error) {

	/*for _, pair := range TablePairArrayType {
		pair.PKColumn.UniqueRowCount = pair.PKColumn.HashUniqueCount
		pair.FKColumn.UniqueRowCount = pair.FKColumn.HashUniqueCount
	}*/

	tablePairMap := make(TablePairMapType)

	for _, pair := range columnPairs {

		tablePair := TablePairType{
			ParentTable: pair.ParentColumn.TableInfo,
			ChildTable:  pair.ChildColumn.TableInfo,
		}

		if arr, found := tablePairMap[tablePair]; !found {
			arr = make(ColumnPairArrayType, 0, 10)
			arr = append(arr, pair)
			tablePairMap[tablePair] = arr
		} else {
			tablePairMap[tablePair] = append(arr, pair)
		}
	}

	for tablePair, columnPairs := range tablePairMap {
		if len(columnPairs) <= 1 {
			if result == nil {
				delete(tablePairMap, tablePair)
			}
		}
	}

	result = tablePairMap

	return
}
func (d *ComplexKeyDiscoveryType) ComposeColumnCombinations(
	ctx context.Context,
	columnPairs ColumnPairArrayType,
) (result ComplexKeyColumnCombinationArrayType, err error) {
	result = make(ComplexKeyColumnCombinationArrayType, 0, 10)
	//ComplexPKStage1 := make([]*ComplexKeyColumnCombinationType, 0, 10)
	//fmt.Printf("\nPKColumn:%v - FKColumn:%v:\n", columnPairs[0].PKColumn.TableInfo, columnPairs[0].FKColumn.TableInfo)

	//* Collecting possible PK columns
	uniqueColumns := make(map[*ColumnInfoType]ColumnPairArrayType)

	for _, columnPair := range columnPairs {
		if pairs, found := uniqueColumns[columnPair.ParentColumn]; !found {
			pairs := make(ColumnPairArrayType, 0, 10)
			pairs = append(pairs, columnPair)
			uniqueColumns[columnPair.ParentColumn] = pairs
		} else {
			pairs = append(pairs, columnPair)
			uniqueColumns[columnPair.ParentColumn] = pairs
		}
	}

	var SortedPKColumns ColumnInfoArrayType
	SortedPKColumns = make(ColumnInfoArrayType, 0, len(uniqueColumns))

	for column := range uniqueColumns {
		SortedPKColumns = append(SortedPKColumns, column)
	}

	//* Sort possible PK columns from the highest data cardinality
	sort.Slice(SortedPKColumns, func(i, j int) bool {
		if SortedPKColumns[i].UniqueRowCount.Value() == SortedPKColumns[j].UniqueRowCount.Value() {
			return SortedPKColumns[i].Id.Value() > SortedPKColumns[j].Id.Value()
		} else {
			return SortedPKColumns[i].UniqueRowCount.Value() > SortedPKColumns[j].UniqueRowCount.Value()
		}
	},
	)

	//* Making PK column combinations
	parentTable := SortedPKColumns[0].TableInfo

	for {
		if len(SortedPKColumns) == 0 {
			break
		}
		var inputColumnCombinations []*ComplexKeyColumnCombinationType
		var CPKeysLast int

		inputColumnCombinations = make([]*ComplexKeyColumnCombinationType, 1, len(SortedPKColumns))
		inputColumnCombinations[0] = &ComplexKeyColumnCombinationType{
			ComplexKeyType: &ComplexKeyType{
				Columns:   make(ColumnInfoArrayType, 1),
				TableInfo: parentTable,
			},
			LastSortedColumnIndex: 1,
		}
		inputColumnCombinations[0].Columns[0] = SortedPKColumns[0]
		//1,2,3,4 -> 12,13,14,123,124,134,23,24,234,34

		for {
			CPKeysLast = len(result)
			for _, inputColumnCombination := range inputColumnCombinations {
				var columnCombination *ComplexKeyColumnCombinationType
				inputLength := len(inputColumnCombination.Columns)
				for index := inputColumnCombination.LastSortedColumnIndex; index < len(SortedPKColumns); index++ {
					columnCombination = &ComplexKeyColumnCombinationType{
						ComplexKeyType: &ComplexKeyType{
							Columns: make(ColumnInfoArrayType, inputLength, inputLength+1),
						},
						LastSortedColumnIndex: index + 1,
					}
					copy(columnCombination.Columns, inputColumnCombination.Columns)
					columnCombination.Columns = append(columnCombination.Columns, SortedPKColumns[index])
					result = append(result, columnCombination)
				}
			}
			inputColumnCombinations = result[CPKeysLast:]
			if len(inputColumnCombinations) == 0 {
				break
			}
		}
		SortedPKColumns = SortedPKColumns[1:]
	}

	return
}

func (d *ComplexKeyDiscoveryType) FilterColumnCombinationsByCardinality(
	ctx context.Context,
	columnCombinations []*ComplexKeyColumnCombinationType,
) (result ComplexKeyColumnCombinationArrayType, err error) {
	result = make(ComplexKeyColumnCombinationArrayType, 0, len(columnCombinations))
	for _, columnCombination := range columnCombinations {
		for index, column := range columnCombination.Columns {
			if index == 0 {
				columnCombination.CombinationCardinality = uint64(column.UniqueRowCount.Value())
			} else {
				columnCombination.CombinationCardinality = columnCombination.CombinationCardinality * uint64(column.UniqueRowCount.Value())
			}
		}
		if columnCombination.CombinationCardinality <
			uint64(columnCombination.Columns[0].TableInfo.RowCount.Value()) {
			/* tracelog.Info(packageName, funcName,
				"Data volume of column permutation %v is insufficient to fill the table row count. %v < %v",
				columnCombination.Columns,
				columnCombination.cardinality,
				columnCombination.Columns[0].TableInfo.RowCount.Value(),
			) */
			continue
		}
		result = append(result, columnCombination)
		//table := columnCombination.Columns[0].TableInfo

		//
		//if collectedColumnCombinations, cccFound := tableCPKs[table]; !cccFound {
		//collectedColumnCombinations = make(map[string]*ComplexPKCombinationType)
		//collectedColumnCombinations[columnCombination.ColumnIndexString()] = columnCombination
		//tableCPKs[table] = collectedColumnCombinations
		//} else {
		//collectedColumnCombinations[columnCombination.ColumnIndexString()] = columnCombination
		//tableCPKs[table] = collectedColumnCombinations
		//}
	}
	return
}
func (d *ComplexKeyDiscoveryType) CollectColumnCombinationsByParentTable(
	ctx context.Context,
	tablePairMap TablePairMapType,
) (result map[*TableInfoType]ComplexKeyColumnCombinationMapType,
	err error,
) {
	funcName := "ComplexKeyDiscoveryType::CollectColumnCombinationsByParentTable"

	result = make(map[*TableInfoType]ComplexKeyColumnCombinationMapType)
	for tablePair, columnPairs := range tablePairMap {

		composedCombinations, err := d.ComposeColumnCombinations(ctx, columnPairs)
		if err != nil {
		}
		if len(composedCombinations) == 0 {
		}
		filteredCombinations, err := d.FilterColumnCombinationsByCardinality(ctx, composedCombinations)
		if err != nil {
		}
		if len(filteredCombinations) == 0 {
		}
		for table, combination := range filteredCombinations {
			if len(combination.Columns) == 0 {
				err = fmt.Errorf("table %v has zero-length column combination", table)
				tracelog.Info(packageName, funcName, "%v", err)
				break
			}
			if collectedColumnCombinations, cccFound := result[tablePair.ParentTable]; !cccFound {
				combination.InitializeInternals()
				collectedColumnCombinations = make(ComplexKeyColumnCombinationMapType)
				collectedColumnCombinations[combination.ColumnIndexString()] = combination
				result[tablePair.ParentTable] = collectedColumnCombinations
			}
		}
	}
	return
}


func(d *ComplexKeyDiscoveryType) CheckParentColumnCombinationDataUniqueness(
	parentColumnCombinations map[*TableInfoType]ComplexKeyColumnCombinationMapType,
	) (err error){
	funcName := "ComplexKeyDiscoveryType::CheckUniqueness"

		for parentTable, columnCombinationMap := range parentColumnCombinations {

		if parentTable.TableName.Value() != "TX_FAIL" {
			//		continue //_ITEM_REVERSED
		}

		storedKeys, err := d.repository.ComplexKeysByTable(parentTable)
		if err != nil {
			return err
		}

		columnCombinationMap.RemoveKeyColumnCombinations(storedKeys)

		if len(columnCombinationMap) == 0 {
			continue
		}




		cumulativeSavedDataLength := uint64(0)
		LineNumberToCheckBySlaveHorseTo := uint64(0)
		leadChan, slaveChan := make(chan interface{}), make(chan interface{})

		var horseConfig DumpReaderConfigType

			horseConfig = *d.config.DumpReaderConfig
			horseConfig.PopulateWithTableInfo(parentTable)
			var leadHorseResult DumpReaderResultType
		var slaveHorseResult DumpReaderResultType
		var columnCombinationMapForLeadHorse ComplexKeyColumnCombinationMapType
		var columnCombinationMapForSlaveHorse ComplexKeyColumnCombinationMapType

		horsesContext, horsesCancelFunc := context.WithCancel(context.Background())

		LeadHorse := func(ctc context.Context, LineNumber, DataPosition uint64, data [][]byte) (result DumpReaderActionType, err error) {
			var truncateCombinations = false
			if LineNumber == 0 {
				fmt.Println("Column combination(s) to check duplicates:")
				statements := make([]string, 0, len(columnCombinationMapForLeadHorse))
				for _, columnCombination := range columnCombinationMapForLeadHorse {
					fmt.Printf("--%v\n ", columnCombination.Columns)
					columns := make([]string, 0, len(columnCombination.Columns))
					for _, c := range columnCombination.Columns {
						columns = append(columns, c.ColumnName.Value())
					}
					s := strings.Join(columns, ", ")
					statements = append(statements, fmt.Sprintf("select '%v' from dual where not exists (select %v,count(*) as ccount from %v.%v group by %v having count(*)>1)  ",
						s, s, parentTable.SchemaName.Value(),
						parentTable.TableName.Value(), s),
					)

				}
				{
					s := strings.Join(statements, "union all \n")
					fmt.Println(s)
				}
				fmt.Printf("\n")
			}

			if cumulativeSavedDataLength > 1024*1024 {
				LineNumberToCheckBySlaveHorseTo = LineNumber
				for columnCombinationKey, columnCombination := range columnCombinationMapForLeadHorse {
					if len(columnCombination.DuplicatesByHash) > 0 {
						columnCombinationMapForSlaveHorse[columnCombinationKey] = columnCombination
					}
				}

				slaveChan <- true
				<-leadChan
				if len(columnCombinationMapForLeadHorse) > 0 {
					tracelog.Info(packageName, funcName, "Lead Horse continues processing %v from line %v with %v column combinations",
						parentTable, LineNumber,
						len(columnCombinationMapForLeadHorse),
					)
					for _, columnCombination := range columnCombinationMapForLeadHorse {
						columnCombination.ResetDuplicateStructures()
						runtime.GC()
						columnCombination.ReinitializeInternals()
					}
					cumulativeSavedDataLength = 0
				}

			}
			var copiedDataMap map[int]*[]byte
			copiedDataMap = make(map[int]*[]byte)
			firstHashMethod := fnv.New32()
		columns:
			for columnCombinationMapKey, columnCombination := range columnCombinationMapForLeadHorse {
				firstHashMethod.Reset()
				for _, position := range columnCombination.ColumnPositions {
					firstHashMethod.Write(data[position])
				}
				hv1 := firstHashMethod.Sum32()

				if !columnCombination.FirstBitset.Set(uint64(hv1)) {
					continue
				}

				secondHashMethod := fnv.New32a()
				for _, position := range columnCombination.ColumnPositions {
					secondHashMethod.Write(data[position])
				}

				hashValue := secondHashMethod.Sum32()

				newDuplicate := !columnCombination.DuplicateBitset.Set(uint64(hashValue))

				pData := make([]*[]byte, len(columnCombination.Columns))
				addToDuplicateByHash := func(duplicates []*ComplexPKDupDataType) {
					newDup := &ComplexPKDupDataType{
						//							ColumnCombinationKey: columnCombinationMapKey,
						Data:       make([]*[]byte, len(columnCombination.Columns)),
						LineNumber: LineNumber,
					}
					// Rough estimation of memory consumption
					//8 -  ref to columnCombinationMapKey
					//24+8N: (Slice internally (3*8)+ 8*count of Ref to columns)
					//8 - lineNumber
					//8 - ref for columnCombination.duplicatesByHash
					//8 - hashValue
					//24 +len(dataCopy):
					//16 ~  Map system internals
					cumulativeSavedDataLength = cumulativeSavedDataLength +
						uint64(8+24+8*len(columnCombination.Columns)+8+8+8)
					columnLengths := make([]int, len(columnCombination.Columns))
					for index, position := range columnCombination.ColumnPositions {
						dataLength := len(data[position])
						columnLengths[index] = dataLength
						pointer := pData[index]
						if pointer != nil {
							newDup.Data[index] = pointer
						} else {
							if dataCopyRef, isDataCopied := copiedDataMap[position]; !isDataCopied {
								cumulativeSavedDataLength = cumulativeSavedDataLength + 24 + uint64(dataLength) + 16

								dataCopy := make([]byte, dataLength)
								copy(dataCopy, data[position])
								copiedDataMap[position] = &dataCopy
								newDup.Data[index] = &dataCopy
							} else {
								newDup.Data[index] = dataCopyRef
							}
						}
					}
					duplicates = append(duplicates, newDup)
					columnCombination.DuplicatesByHash[hashValue] = duplicates
				}

				if newDuplicate {
					duplicates := make([]*ComplexPKDupDataType, 0, 3)
					addToDuplicateByHash(duplicates)
					if false {
						fmt.Printf("--------%v ---\n", hv1)
						for _, position := range columnCombination.ColumnPositions {
							fmt.Printf("%v, ", data[position])
						}
						fmt.Printf("++++++\n")
					}
				} else if duplicates, found := columnCombination.DuplicatesByHash[hashValue]; !found {
					duplicates = make([]*ComplexPKDupDataType, 0, 3)
					addToDuplicateByHash(duplicates)
				} else {
					for _, dup := range duplicates {
						countDifferentPieces := 0
						for index, position := range columnCombination.ColumnPositions {
							var result int
							if len(*dup.Data[index]) == len(data[position]) {
								result = bytes.Compare(*dup.Data[index], data[position])
							} else {
								result = 1
							}
							if result != 0 {
								countDifferentPieces++
							} else {
								if pData[index] == nil {
									pData[index] = dup.Data[index]
								}
							}
						}
						if countDifferentPieces == 0 {
							truncateCombinations = true
							tracelog.Info(packageName, funcName, "Lead Horse:Data duplication found for columns %v in lines %v and %v", columnCombination.Columns, dup.LineNumber, LineNumber)
							columnCombination.Reset()
							delete(columnCombinationMapForLeadHorse, columnCombinationMapKey)
							continue columns
						}
						//}
					}
					addToDuplicateByHash(duplicates)
				}
			}

			if truncateCombinations {
				if len(columnCombinationMapForLeadHorse) == 0 {
					tracelog.Info(packageName, funcName,
						"There is no column combination available for %v to check",
						parentTable,
					)
					return DumpReaderActionAbort, nil
				}
			}
			return DumpReaderActionContinue, nil
		}

		SlaveHorse := func(ctc context.Context, LineNumber, DataPosition uint64, data [][]byte) (result DumpReaderActionType, err error) {
			var truncateCombinations = false
			if LineNumberToCheckBySlaveHorseTo > 0 && LineNumberToCheckBySlaveHorseTo == LineNumber {
				return DumpReaderActionAbort, nil
			}
			firstHashMethod := fnv.New32()
			secondHashMethod := fnv.New32a()
		columns:
			for columnCombinationMapKey, columnCombination := range columnCombinationMapForSlaveHorse {

				firstHashMethod.Reset()
				secondHashMethod.Reset()

				for _, position := range columnCombination.ColumnPositions {
					secondHashMethod.Write(data[position])
				}
				hashValue := secondHashMethod.Sum32()

				if !columnCombination.DuplicateBitset.Test(uint64(hashValue)) {
					continue
				}
				firstHashMethod.Reset()

				for _, position := range columnCombination.ColumnPositions {
					firstHashMethod.Write(data[position])
				}
				if !columnCombination.FirstBitset.Test(uint64(firstHashMethod.Sum32())) {
					continue
				}

				if duplicates, found := columnCombination.DuplicatesByHash[hashValue]; found {
					for _, dup := range duplicates {
						countDifferentPieces := 0
						if dup.LineNumber == LineNumber {
							continue
						}
						for index, position := range columnCombination.ColumnPositions {
							result := bytes.Compare(*dup.Data[index], data[position])
							if result != 0 {
								countDifferentPieces++
								break
							}
						}
						if countDifferentPieces == 0 {
							// TODO:DUPLICATE!
							truncateCombinations = true
							tracelog.Info(packageName, funcName, "Slave Horse:Data duplication found for columns %v in lines %v and %v", columnCombination.Columns, LineNumber, dup.LineNumber)
							columnCombination.Reset()
							delete(columnCombinationMapForLeadHorse, columnCombinationMapKey)
							delete(columnCombinationMapForSlaveHorse, columnCombinationMapKey)
							continue columns
						}
					}
				}
			}

			if truncateCombinations {
				if len(columnCombinationMapForSlaveHorse) == 0 {
					tracelog.Info(packageName, funcName,
						"There is no column combination available for %v to check",
						parentTable,
					)
					return DumpReaderActionAbort, nil
				}
			}
			return DumpReaderActionContinue, nil
		}

		if len(columnCombinationMap) > 0 {
			for _, columnCombination := range columnCombinationMap {
				columnCombination.InitializeInternals()
			}
		}

		cumulativeSavedDataLength = 0

		go func() {

			processedKeys := make(map[string]bool)

			keyList := make([]string, 0, len(columnCombinationMap))

			for columnCombinationMapKey := range columnCombinationMap {
				keyList = append(keyList, columnCombinationMapKey)
			}

			sort.Slice(keyList, func(i, j int) bool {
				if len(columnCombinationMap[keyList[i]].Columns) == len(columnCombinationMap[keyList[j]].Columns) {
					return keyList[i] > keyList[j]
				} else {
					return len(columnCombinationMap[keyList[i]].Columns) > len(columnCombinationMap[keyList[j]].Columns)
				}
			})

		mainLoopLeadHorse:
			for {
				var allKeysProcessed = true

				for _, key := range keyList {
					if _, found := processedKeys[key]; found {
						continue
					}
					allKeysProcessed = false
					break
				}
				if allKeysProcessed {
					break mainLoopLeadHorse
				}

				{
					var removed, processed = 0, 0
					for _, exists := range processedKeys {
						if exists {
							processed++
						} else {
							removed++
						}
					}

					tracelog.Info(packageName, funcName,
						"Column combinations: total %v, found duplicates in: %v, ready for exact test: %v, leftover: %v;  ",
						len(keyList), removed, processed,
						len(keyList)-removed-processed,
					)
				}

				for removedKey, exists := range processedKeys {
					if !exists {
						for key, columnCombination := range columnCombinationMap {
							if _, found := processedKeys[key]; !found {
								if key != removedKey && columnCombination.Columns.isSubsetOf(columnCombinationMap[removedKey].Columns) {
									tracelog.Info(packageName, funcName,
										"Combination %v is subset of already checked and rejected %v. Skipped ",
										columnCombination.Columns,
										columnCombinationMap[removedKey].Columns,
									)
									processedKeys[key] = false

									complexKey := columnCombination.NewComplexKeyInfo()
									complexKey.ProcessingStage = nullable.NewNullString("u")
									err = d.repository.PersistComplexKey(complexKey)
									if err != nil {
										tracelog.Error(err, packageName, funcName)
										break mainLoopLeadHorse
									}
								}
							}
						}
					}
				}

				columnCombinationMapForLeadHorse = make(ComplexKeyColumnCombinationMapType)
				columnCombinationMapForSlaveHorse = make(ComplexKeyColumnCombinationMapType)

				processingKeys := make(map[string]bool)

				for _, key := range keyList {
					if _, found := processedKeys[key]; found {
						continue
					}
					if len(processingKeys) >= d.config.NumberOfKeysPerFilePass {
						break
					}
					columnCombinationMapForLeadHorse[key] = columnCombinationMap[key]
					processingKeys[key] = true
				}

				if len(columnCombinationMapForLeadHorse) == 0 {
					continue
				}

				tracelog.Info(packageName, funcName, "Leading Horse starts processing %v with %v column combinations:",
					parentTable, len(columnCombinationMapForLeadHorse),
				)

				for _, columnCombination := range columnCombinationMapForLeadHorse {
					_ = columnCombination
					//	tracelog.Info(packageName, funcName, "%v", columnCombination.Columns)
				}

				ReadAstraDump(
					horsesContext,
					&horseConfig,
					LeadHorse,
				)

				LineNumberToCheckBySlaveHorseTo = 0
				for columnCombinationKey, columnCombination := range columnCombinationMapForLeadHorse {
					if len(columnCombination.DuplicatesByHash) > 0 {
						columnCombinationMapForSlaveHorse[columnCombinationKey] = columnCombination
					}
				}

				for key := range processingKeys {
					_, exists := columnCombinationMapForLeadHorse[key]
					processedKeys[key] = exists
					columnCombination := columnCombinationMap[key]
					complexKey := columnCombination.NewComplexKeyInfo()
					if !exists {
						complexKey.ProcessingStage = nullable.NewNullString("u")
						err = d.repository.PersistComplexKey(complexKey)
						if err != nil {
							columnCombination.Reset()
							tracelog.Error(err, packageName, funcName)
							break mainLoopLeadHorse
						}
					} else if _, exists = columnCombinationMapForSlaveHorse[key]; !exists {
						complexKey.ProcessingStage = nullable.NewNullString("B")
						err = d.repository.PersistComplexKey(complexKey)
						if err != nil {
							tracelog.Error(err, packageName, funcName)
							columnCombination.Reset()
							break mainLoopLeadHorse
						}
						columnCombination.ComplexKeyInfoId = complexKey.Id.Value()

						err = WriteBitsetToFile(horsesContext, d.config.PathToBitsetDirectory, columnCombination)
						columnCombination.Reset()
						if err != nil {
							tracelog.Error(err, packageName, funcName)
							break mainLoopLeadHorse
						}
					}

				}

				if len(columnCombinationMapForSlaveHorse) == 0 {
					continue mainLoopLeadHorse
				} else {
					slaveChan <- true //Launch Slave horse
					<-leadChan        //Wait until Slave finishes

					for key := range processingKeys {

						_, exists := columnCombinationMapForSlaveHorse[key]

						processedKeys[key] = exists
						columnCombination := columnCombinationMap[key]
						complexKey := columnCombination.NewComplexKeyInfo()
						if !exists {
							columnCombination.Reset()
							complexKey.ProcessingStage = nullable.NewNullString("u")
							err = d.repository.PersistComplexKey(complexKey)
							if err != nil {
								tracelog.Error(err, packageName, funcName)
								break mainLoopLeadHorse
							}

						} else if _, exists = columnCombinationMapForLeadHorse[key]; exists {
							complexKey.ProcessingStage = nullable.NewNullString("B")

							err = d.repository.PersistComplexKey(complexKey)
							if err != nil {
								tracelog.Error(err, packageName, funcName)
								columnCombination.Reset()
								break mainLoopLeadHorse
							}

							columnCombination.ComplexKeyInfoId = complexKey.Id.Value()

							err = WriteBitsetToFile(horsesContext, d.config.PathToBitsetDirectory, columnCombination)
							columnCombination.Reset()
							if err != nil {
								tracelog.Error(err, packageName, funcName)
								break mainLoopLeadHorse
							}
						}
					}
				}
			} //mainLoopLeadHorse

			for key, exists := range processedKeys {
				if !exists {
					delete(columnCombinationMap, key)
				}
			}
			slaveChan <- false
			slaveChan <- DumpReaderResultOk
			slaveChan <- err
			close(slaveChan)

		}()

	horses2:
		for {
			if unresolved, open := <-slaveChan; open {
				switch continued := unresolved.(type) {
				case bool:
					if !continued {
						unresolved = <-slaveChan
						err, _ = unresolved.(error)
						leadHorseResult, _ = unresolved.(DumpReaderResultType)
						columnCombinationMapForSlaveHorse = nil
						close(leadChan)
					}

				}
			}

			if columnCombinationMapForSlaveHorse != nil && len(columnCombinationMapForSlaveHorse) > 0 {
				if LineNumberToCheckBySlaveHorseTo == 0 {
					tracelog.Info(packageName, funcName, "Slave Horse works on %v up to the EOF with %v column combinations", parentTable, len(columnCombinationMapForSlaveHorse))
				} else {
					tracelog.Info(packageName, funcName, "Slave Horse works on %v up to the line %v with %v column combinations", parentTable, LineNumberToCheckBySlaveHorseTo, len(columnCombinationMapForSlaveHorse))
				}

				slaveHorseResult, _, err = ReadAstraDump(
					horsesContext,
					&horseConfig,
					SlaveHorse,
				)

				if err != nil {
					tracelog.Errorf(err, packageName, funcName, "err!")
					horsesCancelFunc()
					leadChan <- true
					return err
				} else {
					leadChan <- true
				}
			} else {
				slaveHorseResult = DumpReaderResultOk
				break horses2
			}
		}

		if leadHorseResult == DumpReaderResultOk && slaveHorseResult == DumpReaderResultOk {

		}

		if len(columnCombinationMap) > 0 {
			fmt.Println("Column combination(s) after checking duplicates:")
			for _, columnCombination := range columnCombinationMap {
				fmt.Println(columnCombination.Columns)
			}
			fmt.Printf("\n\n")
		}
	}
	return

}