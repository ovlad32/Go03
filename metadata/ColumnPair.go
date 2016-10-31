package metadata

import (
	"github.com/goinggo/tracelog"
	"github.com/boltdb/bolt"
	utils "./../utils"
	"os"
	"fmt"
	"errors"
)

var columnPairCategoriesBucket = []byte("categories")
var columnPairStatsBucket = []byte("stats")
var columnPairStatsNonNullCountKey = []byte("nonNullCount")
var columnPairStatsHashUniqueCountKey = []byte("uniqueHashCount")

type ColumnPairType struct {
	dataCategory      []byte
	column1           *ColumnInfoType
	column2           *ColumnInfoType
	IntersectionCount uint64
	dataBucketName    []byte
	storage           *bolt.DB
	currentTx         *bolt.Tx
	CategoriesBucket  *bolt.Bucket
	CategoryBucket    *bolt.Bucket
	StatsBucket       *bolt.Bucket
}


func NewColumnPair(column1, column2 *ColumnInfoType, dataCategory []byte) (result *ColumnPairType, err error) {
	funcName := "NewColumnPair"
	if column1 == nil || column2 == nil ||
		!column1.Id.Valid() || !column2.Id.Valid() {
		err = ColumnInfoNotInitialized
		tracelog.Error(err, packageName, funcName)
		return
	}
	if dataCategory == nil {
		err = errors.New("DataCategory is empty!")
		tracelog.Error(err, packageName, funcName)
		return
	}
	result = &ColumnPairType{
		dataCategory: dataCategory,
	}
	if column1.Id.Value() < column2.Id.Value() {
		result.column1 = column1
		result.column2 = column2
	} else {
		result.column2 = column1
		result.column1 = column2
	}

	result.dataBucketName = make([]byte, 8*2, 8*2)
	b81 := utils.Int64ToB8(result.column1.Id.Value())
	copy(result.dataBucketName, b81)

	b82 := utils.Int64ToB8(result.column2.Id.Value())
	copy(result.dataBucketName[8:], b82)

	tracelog.Completed(packageName, funcName)
	return
}

func (cp *ColumnPairType) OpenStorage(writable bool) (err error) {
	funcName := "ColumnPairType.OpenStorage"

	if cp.column1 == nil || cp.column2 == nil ||
		!cp.column1.Id.Valid() || !cp.column2.Id.Valid() {
		err = ColumnInfoNotInitialized
		tracelog.Error(err, packageName, funcName)
		return
	}

	if cp.storage == nil {
		path := "./DBS"
		err = os.MkdirAll(path, 0600)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
		file := fmt.Sprintf("%v/%v-%v.boltdb", path, cp.column1.Id.Value(), cp.column2.Id.Value())

		cp.storage, err = bolt.Open(file, 0600, nil)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
		if cp.storage == nil {
			tracelog.Warning(packageName, funcName, "Storage has not been created for pair id=[%v,%v]", cp.column1.Id, cp.column2.Id)
			return
		}
	}

	if cp.currentTx == nil {
		cp.currentTx, err = cp.storage.Begin(writable)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return err
		}
		if cp.currentTx == nil {
			tracelog.Warning(packageName, funcName, "Transaction has not been opened for pair id=[%v,%v]", cp.column1.Id, cp.column2.Id)
			return
		}
	}
	/*if cp.hashBucket == nil {
		cp.hashBucket = categoryBucket.Bucket(cp.dataBucketName)
		if cp.hashBucket == nil {
			if cp.currentTx.Writable() {
				cp.hashBucket, err = categoryBucket.CreateBucket(cp.dataBucketName)
				if err != nil {
					tracelog.Error(err, packageName, funcName)
					return
				}
				if cp.hashBucket == nil {
					err = errors.New("Hash bucket has not been created for pair...")
					tracelog.Error(err, packageName, funcName)
					return
				}
			}
		}
	}*/
	tracelog.Completed(packageName, funcName)
	return
}

func (cp *ColumnPairType) OpenCategoriesBucket() (err error)  {
	funcName := "ColumnPairType.OpenCategoriesBucket"
	cp.CategoriesBucket = cp.currentTx.Bucket(columnPairCategoriesBucket)
	if cp.CategoriesBucket == nil {
		if cp.currentTx.Writable() {
			cp.CategoriesBucket, err = cp.currentTx.CreateBucket(columnPairCategoriesBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if cp.CategoriesBucket == nil {
				err = errors.New("Categories bucket has not been created for pair...")
				tracelog.Error(err, packageName, funcName)
				return
			}

		}
	}
	return
}

func (cp *ColumnPairType) OpenStatsBucket() (err error)  {
	funcName := "ColumnPairType.OpenStatsBucket"
	cp.StatsBucket = cp.currentTx.Bucket(columnPairStatsBucket)
	if cp.StatsBucket == nil {
		if cp.currentTx.Writable() {
			cp.StatsBucket, err = cp.currentTx.CreateBucket(columnPairStatsBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if cp.StatsBucket == nil {
				err = errors.New("Stats bucket has not been created for pair...")
				tracelog.Error(err, packageName, funcName)
				return
			}
		}
	}
	return
}

func (cp *ColumnPairType) OpenCurrentCategoryBucket() (err error)  {
	funcName := "ColumnPairType.OpenCurrentCategoryBucket"
	cp.CategoryBucket = cp.CategoriesBucket.Bucket(cp.dataCategory)
	if cp.CategoryBucket == nil {
		if cp.currentTx.Writable() {
			cp.CategoryBucket, err = cp.CategoriesBucket.CreateBucket(cp.dataCategory)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if cp.CategoryBucket == nil {
				err = errors.New("DataCategory bucket has not been created for pair...")
				tracelog.Error(err, packageName, funcName)
				return
			}
		}
	}
	return
}



func (cp *ColumnPairType) CloseStorageTransaction(commit bool) (err error) {
	funcName := "ColumnPairType.CloseStorageTransaction"
	if cp.currentTx != nil {
		if commit {
			err = cp.currentTx.Commit()
		} else {
			err = cp.currentTx.Rollback()
		}
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
		cp.currentTx = nil
		cp.CategoriesBucket = nil
		cp.CategoryBucket = nil
		cp.StatsBucket = nil
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (cp *ColumnPairType) CloseStorage() (err error) {
	funcName := "ColumnPairType.CloseStorage"
	cp.CloseStorageTransaction(false)
	if cp.storage != nil {
		cp.storage.Close()
	}
	tracelog.Completed(packageName, funcName)
	return
}
