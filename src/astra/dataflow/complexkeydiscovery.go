package dataflow

import (
	"astra/nullable"
	"fmt"
	"github.com/goinggo/tracelog"
)

type ComplexKeyDiscoveryConfigType struct {
	ParentColumnNullAllowed bool
	FloatContentAllowed     bool
	CollisionLevel          float64
}

type ColumnPairType struct {
	ParentColumn *ColumnInfoType
	ChildColumn  *ColumnInfoType
}
type ColumnPairArrayType []*ColumnPairType

type ComplexKeyDiscoveryType struct {
	config     *ComplexKeyDiscoveryConfigType
	repository *Repository
}

func NewComplexKeyDiscoverer(config *ComplexKeyDiscoveryConfigType) (*ComplexKeyDiscoveryType, error) {
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
			"Number of DataCategories = 0",
			childColumn,
		)
		tracelog.Info(packageName, funcName, explanation)
		return
	}

	parentCategoryCount := len(parentColumn.Categories)
	if isNotChild = childCategoryCount > parentCategoryCount; isNotChild {
		explanation = fmt.Sprintf(
			"Number of data categories of Child column is bigger than "+
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
				"Child data category (code: %v) is not found in "+
					" Parent data categories",
				categoryKey,
			)
			return
		} else {
			if !childCategory.IsNumeric.Valid() {
				err = fmt.Errorf("Child IsNumeric statistics is empty in %v", childCategory)
				tracelog.Error(err, packageName, funcName)
				return
			}
			if !parentCategory.IsNumeric.Valid() {
				err = fmt.Errorf("Parent IsNumeric statistics is empty in %v", parentCategory)
				tracelog.Error(err, packageName, funcName)
				return
			}

			if childCategory.IsNumeric.Value() && parentCategory.IsNumeric.Value() {
				if !childCategory.IsInteger.Valid() {
					err = fmt.Errorf("Child IsInteger statistics is empty in %v", childCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !parentCategory.IsInteger.Valid() {
					err = fmt.Errorf("Parent IsInteger statistics is empty in %v", parentCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}

				if childCategory.IsInteger.Value() && parentCategory.IsInteger.Value() {
					if !childCategory.ItemUniqueCount.Valid() {
						err = fmt.Errorf("Child ItemUniqueCount statistics is empty in %v", childCategory)
						tracelog.Error(err, packageName, funcName)
						return
					}
					if !parentCategory.ItemUniqueCount.Valid() {
						err = fmt.Errorf("Parent ItemUniqueCount statistics is empty in %v", parentCategory)
						tracelog.Error(err, packageName, funcName)
						return
					}

					isNotChild = childCategory.ItemUniqueCount.Value() > parentCategory.ItemUniqueCount.Value()
					if isNotChild {
						explanation = fmt.Sprintf(
							"Child ItemUniqueCount > Parent ItemUniqueCount in DataCategory %v; %v > %v",
							categoryKey,
							childCategory.ItemUniqueCount.Value(),
							parentCategory.ItemUniqueCount.Value(),
						)
						tracelog.Info(packageName, funcName, explanation)
						return
					}
				}

				if !childCategory.MinNumericValue.Valid() {
					err = fmt.Errorf("Child MinNumericValue statistics is empty in %v", childCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !parentCategory.MinNumericValue.Valid() {
					err = fmt.Errorf("Parent MinNumericValue statistics is empty in %v", parentCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}

				if !childCategory.MaxNumericValue.Valid() {
					err = fmt.Errorf("Child MaxNumericValue statistics is empty in %v", childCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !parentCategory.MaxNumericValue.Valid() {
					err = fmt.Errorf("Parent MaxNumericValue statistics is empty in %v", parentCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				isNotChild = childCategory.MaxNumericValue.Value() > parentCategory.MaxNumericValue.Value()
				if isNotChild {
					explanation = fmt.Sprintf(
						" Child MaxNumericValue > Parent MaxNumericValue in DataCategory %v; %v > %v",
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
						"Child MinNumericValue < Parent MinNumericValue in DataCategory %v; %v < %v",
						categoryKey,
						childCategory.MinNumericValue.Value(),
						parentCategory.MinNumericValue.Value())
					tracelog.Info(packageName, funcName, explanation)
					return
				}
			} else {
				if !childCategory.ItemUniqueCount.Valid() {
					err = fmt.Errorf("Child ItemUniqueCount statistics is empty in %v", childCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !parentCategory.ItemUniqueCount.Valid() {
					err = fmt.Errorf("Parent ItemUniqueCount statistics is empty in %v", parentCategory)
					tracelog.Error(err, packageName, funcName)
					return
				}

				isNotChild = childCategory.ItemUniqueCount.Value() > parentCategory.ItemUniqueCount.Value()
				if isNotChild {
					explanation = fmt.Sprintf(
						"ItemUniqueCountFK > ItemUniqueCountPK DataCategory %v: %v > %v",
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
						"Child HashUniqueCount > Parent HashUniqueCount * ratio(%v) in DataCategory; %v > %v",
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
		err = fmt.Errorf("Child HashUniqueCount statistics is empty in %v", childColumn)
		tracelog.Error(err, packageName, funcName)
		return
	}

	if !parentColumn.HashUniqueCount.Valid() {
		err = fmt.Errorf("Child HashUniqueCount statistics is empty in %v", parentColumn)
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
	columns ColumnInfoArrayType,
) (result ColumnPairArrayType, err error) {
	funcName := "ComplexKeyDiscoveryType::ComposeAndFilterColumnPairsByFeatures"
	const COLUMN_PAIR_ARRAY_INIT_SIZE = 1000

	pairsFilteredByFeatures := make(ColumnPairArrayType, 0, COLUMN_PAIR_ARRAY_INIT_SIZE)
	var bruteForcePairCount int = 0
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
			bruteForcePairCount = bruteForcePairCount + 1

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
