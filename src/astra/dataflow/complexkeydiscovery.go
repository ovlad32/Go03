package dataflow

import (
	"fmt"
	"github.com/goinggo/tracelog"
	"unicode"
)

type ComplexKeyDiscoveryConfigType struct {
	ParentColumnNullAllowed bool
	FloatContentAllowed     bool
	CollisionLevel float64
}

type ColumnInfoPairType struct {
	ParentColumn *ColumnInfoType
	ChildColumn  *ColumnInfoType
}

type ComplexKeyDiscovererType struct {
	config *ComplexKeyDiscoveryConfigType
}

func NewComplexKeyDiscoverer(config *ComplexKeyDiscoveryConfigType) (*ComplexKeyDiscovererType, error){
	if config == nil {

	}
	if config.CollisionLevel<= float64(0) {

	}

	return &ComplexKeyDiscovererType{
		config:config,
	},nil
}


func (d *ComplexKeyDiscovererType) IsNotParent(parentColumn *ColumnInfoType) (result bool, explanation string, err error) {
	funcName := "ComplexKeyDiscovererType.IsNotParent"
	// Null existence
	if !parentColumn.TableInfo.RowCount.Valid() {
		err = fmt.Errorf("RowCount statistics is empty for %v", parentColumn.TableInfo)
		tracelog.Error(err, packageName, funcName)
		return
	}

	result = parentColumn.TableInfo.RowCount.Value() < 2
	if result {
		explanation = fmt.Sprintf("Table %v has the only row",
			parentColumn.TableInfo,
		)
		return
	}

	if !d.config.ParentColumnNullAllowed {
		var totalNonNullCount uint64 = 0

		for _, category := range parentColumn.Categories {
			if !category.NonNullCount.Valid() {
				err = fmt.Errorf("NonNullCount statistics is empty for %v", category)
				tracelog.Error(err, packageName, funcName)
				return
			}
			totalNonNullCount += uint64(category.NonNullCount.Value())
		}

		result = uint64(parentColumn.TableInfo.RowCount.Value()) != totalNonNullCount
		if result {
			explanation = fmt.Sprintf(
				"Column %v is not Parent: TotalRowCount != TotalNotNullCount. %v != %v",
				parentColumn,
				uint64(parentColumn.TableInfo.RowCount.Value()),
				totalNonNullCount,
			)
			return
		}
	}

	result = parentColumn.TotalRowCount.Value() == parentColumn.HashUniqueCount.Value()
	if result {
		explanation = fmt.Sprintf(
			"Columns %v is not Parent: Set of UniqueHashCount == TotalRowCount. %v == %v",
			parentColumn,
			parentColumn.HashUniqueCount.Value(),
			parentColumn.TotalRowCount.Value(),
		)
		return
	}

	return
}

func (d *ComplexKeyDiscovererType) IsNotChild(
	childColumn, parentColumn *ColumnInfoType,
) (result bool, explanation string, err error) {
	funcName := "ComplexKeyDiscovererType.IsNotChild"
	if !childColumn.TableInfo.RowCount.Valid() {
		err = fmt.Errorf("RowCount statistics is empty in %v", childColumn.TableInfo)
		tracelog.Error(err, packageName, funcName)
		return
	}

	result = childColumn.TableInfo.RowCount.Value() < 2
	if result {
		explanation = fmt.Sprintf("Column %v is not Child: RowCount < 2", childColumn)
		return
	}

	categoryCountFK := len(childColumn.Categories)
	result = categoryCountFK == 0
	if result {
		explanation = fmt.Sprintf("Column %v is not Child: DataCategory count  = 0", childColumn)
		return
	}

	categoryCountPK := len(parentColumn.Categories)

	result = categoryCountFK == 0 || categoryCountFK > categoryCountPK
	if result {
		explanation = fmt.Sprintf("Column %v is not Child to %v: categoryCountFK > categoryCountPK; %v > %v", childColumn, parentColumn, categoryCountFK, categoryCountPK)
		return
	}

	for categoryKey, categoryFK := range childColumn.Categories {
		if categoryPK, found := parentColumn.Categories[categoryKey]; !found {
			return
		} else {
			if !categoryFK.IsNumeric.Valid() {
				err = fmt.Errorf("IsNumeric statistics is empty in %v", categoryFK)
				tracelog.Error(err, packageName, funcName)
				return
			}
			if categoryFK.IsNumeric.Value() {
				if !categoryFK.IsInteger.Valid() {
					err = fmt.Errorf("IsInteger statistics is empty in %v", categoryFK)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if categoryFK.IsInteger.Value() {
					if !categoryFK.ItemUniqueCount.Valid() {
						err = fmt.Errorf("ItemUniqueCount statistics is empty in %v", categoryFK)
						tracelog.Error(err, packageName, funcName)
						return
					}

					result = categoryFK.ItemUniqueCount.Value() > categoryPK.ItemUniqueCount.Value()
					if result {
						explanation = fmt.Sprintf(
							"Column %v is not Child to %v for DataCategory %v: ItemUniqueCountFK > ItemUniqueCountPK; %v > %v",
							childColumn, parentColumn, categoryFK.Key,
							categoryFK.ItemUniqueCount.Value(),
							categoryPK.ItemUniqueCount.Value(),
						)
						return
					}
				}

				if !categoryFK.MinNumericValue.Valid() {
					err = fmt.Errorf("MinNumericValue statistics is empty in %v", categoryFK)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !categoryFK.MaxNumericValue.Valid() {
					err = fmt.Errorf("MaxNumericValue statistics is empty in %v", categoryFK)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !categoryPK.MinNumericValue.Valid() {
					err = fmt.Errorf("MinNumericValue statistics is empty in %v", categoryPK)
					tracelog.Error(err, packageName, funcName)
					return
				}
				if !categoryPK.MaxNumericValue.Valid() {
					err = fmt.Errorf("MaxNumericValue statistics is empty in %v", categoryPK)
					tracelog.Error(err, packageName, funcName)
					return
				}
				result = categoryFK.MaxNumericValue.Value() > categoryPK.MaxNumericValue.Value()
				if result {
					explanation = fmt.Sprintf(
						"Column %v is not Child to %v for DataCategory %v:  MaxNumericValueFK > MaxNumericValuePK; %v > %v",
						childColumn, parentColumn, categoryFK.Key,
						categoryFK.MaxNumericValue.Value(),
						categoryPK.MaxNumericValue.Value(),
					)
					return
				}
				result = categoryFK.MinNumericValue.Value() < categoryPK.MinNumericValue.Value()
				if result {
					explanation = fmt.Sprintf(
						"Column %v is not Child to %v for DataCategory %v: MinNumericValueFK < MinNumericValuePK; %v < %v",
						childColumn, parentColumn, categoryFK.Key,
						categoryFK.MinNumericValue.Value(),
						categoryPK.MinNumericValue.Value())
					return
				}
			} else {
				result = categoryFK.ItemUniqueCount.Value() > categoryPK.ItemUniqueCount.Value()
				if result {
					explanation = fmt.Sprintf(
						"Column %v is not Child to %v for DataCategory %v: ItemUniqueCountFK > ItemUniqueCountPK; %v > %v",
						childColumn, parentColumn, categoryFK.Key,
						categoryFK.ItemUniqueCount.Value(),
						categoryPK.ItemUniqueCount.Value(),
					)
					return
				}

				result = float64(categoryFK.HashUniqueCount.Value()) > float64(categoryPK.HashUniqueCount.Value())*d.config.CollisionLevel
				if result {
					explanation = fmt.Sprintf(
						"Column %v is not Child to %v for DataCategory %v: HashUniqueCountFK > DataCategory.HashUniqueCountPK*ratio(%v); %v > %v",
						childColumn, parentColumn, categoryFK.Key, d.config.CollisionLevel,
						categoryFK.HashUniqueCount.Value(),
						uint64(float64(categoryPK.HashUniqueCount.Value())*d.config.CollisionLevel),
					)
					return
				}
			}

		}
	}

	// FKColumn Hash unique count has to be less than PKColumn Hash unique count
	{
		result = float64(childColumn.HashUniqueCount.Value()) > float64(parentColumn.HashUniqueCount.Value())*d.config.CollisionLevel
		if result {
			explanation = fmt.Sprintf(
				"Column %v is not Child to %v: HashUniqueCountFK > HashUniqueCountPK*CollisionLevel(%v); %v > %v",
				childColumn, parentColumn, d.config.CollisionLevel,
				parentColumn.HashUniqueCount.Value(),
				uint64(float64(parentColumn.HashUniqueCount.Value())*d.config.CollisionLevel),
			)
			return
		}
	}

	return
}
