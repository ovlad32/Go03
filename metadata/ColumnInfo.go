package metadata

import (
	jsnull "./../jsnull"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"os"
	"sync"
)

var columnInfoCategoriesBucket = []byte("categories")
var columnInfoStatsBucket = []byte("stats")
var columnInfoStatsNonNullCountKey = []byte("nonNullCount")
var columnInfoStatsHashUniqueCountKey = []byte("uniqueHashCount")

type ColumnInfoType struct {
	Id               jsnull.NullInt64  `json:"column-id"`
	TableInfoId      jsnull.NullInt64  `json:"table-id"`
	ColumnName       jsnull.NullString `json:"column-name"`
	Position         jsnull.NullInt64  `json:"column-position"`
	DataType         jsnull.NullString `json:"data-type"`
	DataPrecision    jsnull.NullInt64  `json:"numeric-precision"`
	DataScale        jsnull.NullInt64  `json:"numeric-scale"`
	DataLength       jsnull.NullInt64  `json:"byte-length"`
	CharLength       jsnull.NullInt64  `json:"character-length"`
	Nullable         jsnull.NullString `json:"nullable"`
	RealDataType     jsnull.NullString `json:"java-data-type"`
	MinStringValue   jsnull.NullString `json:"min-string-value"`
	MaxStringValue   jsnull.NullString `json:"max-string-value"`
	HashUniqueCount  jsnull.NullInt64
	UniqueRowCount   jsnull.NullInt64
	TotalRowCount    jsnull.NullInt64
	MinStringLength  jsnull.NullInt64
	MaxStringLength  jsnull.NullInt64
	IsAllNumeric     jsnull.NullString
	IsAllInteger     jsnull.NullString
	MinNumericValue  jsnull.NullFloat64
	MaxNumericValue  jsnull.NullFloat64
	NonNullCount     jsnull.NullInt64
	DistinctCount    jsnull.NullInt64
	TableInfo        *TableInfoType
	DataCategories   []*ColumnDataCategoryStatsType
	NumericCount     jsnull.NullInt64
	bucketLock       sync.Mutex
	storage          *bolt.DB
	currentTx        *bolt.Tx
	categoriesBucket *bolt.Bucket
	statsBucket      *bolt.Bucket
}

func (c ColumnInfoType) String() string {
	var result string

	if c.TableInfo != nil {
		return fmt.Sprintf("%v.%v", c.TableInfo.String(), c.ColumnName.String())
	} else {
		return fmt.Sprintf("%v", c.ColumnName.String())
	}
	return result
}


func (c ColumnInfoType) СheckId() {
	if !c.Id.Valid() {
		panic("Column Id is not initialized!")
	}
}

func (c ColumnInfoType) СheckTableInfo() {
	if c.TableInfo == nil {
		panic("Table reference is not initialized!")
	}
}

func (ci *ColumnInfoType) ResetBuckets() {
	ci.categoriesBucket = nil
	ci.statsBucket = nil
	if ci.DataCategories != nil {
		for _, dc := range ci.DataCategories {
			dc.ResetBuckets()
		}
	}
}
func (ci *ColumnInfoType) CleanStorage() (err error) {
	bucket := ci.currentTx.Bucket(columnInfoCategoriesBucket);
	if bucket != nil {
		err = ci.currentTx.DeleteBucket(columnInfoCategoriesBucket);
		if err != nil {
			return
		}
	}

	bucket = ci.currentTx.Bucket(columnInfoStatsBucket);
	if bucket != nil {
		ci.currentTx.DeleteBucket(columnInfoStatsBucket);
		if err != nil {
			return
		}
	}

	return
}


func (ci *ColumnInfoType) OpenStorage(writable bool) (err error) {
	funcName := "ColumnInfoType.Buckets"
	if !ci.Id.Valid() {
		err = ColumnIdNotInitialized
		return
	}
	if ci.TableInfo == nil {
		err = TableInfoNotInitialized
		return
	}
	if ci.storage == nil {
		path := fmt.Sprintf("./%v", ci.TableInfo.PathToDataDir.Value())
		_ = os.MkdirAll(path, 0)
		file := fmt.Sprintf("%v/%v.boltdb", path, ci.Id.Value())
		ci.storage, err = bolt.Open(
			file,
			0600,
			nil,
		)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
	}
	if ci.currentTx == nil {
		ci.currentTx, err = ci.storage.Begin(writable)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
	}
	//columnBucketIdBytes := utils.Int64ToB8(ci.Id.Value())


	tracelog.Completed(packageName, funcName)
	return
}

func (ci *ColumnInfoType) OpenCategoriesBucket() (err error) {
	funcName := "ColumnInfoType.OpenCategoriesBucket"
	ci.categoriesBucket = ci.currentTx.Bucket(columnInfoCategoriesBucket)
	if ci.categoriesBucket == nil {
		if ci.currentTx.Writable() {
			ci.categoriesBucket, err = ci.currentTx.CreateBucket(columnInfoCategoriesBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if ci.categoriesBucket == nil {
				err = errors.New(fmt.Sprintf("Could not create bucket for column id %v data categories. Got empty value", ci.Id))
				tracelog.Error(err, packageName, funcName)
				return
			} else {
				tracelog.Info(packageName, funcName, "Bucket for column id %v data categories created", ci.Id)
			}
		} else {
			//tracelog.Info(packageName, funcName, "Bucket for column id %v data categories has not been created", ci.Id)
		}
	}
	return
}

func (ci *ColumnInfoType) OpenStatsBucket() (err error) {
	funcName := "ColumnInfoType.OpenStatsBucket"

	ci.statsBucket = ci.currentTx.Bucket(columnInfoStatsBucket)
	if ci.statsBucket == nil {
		if ci.currentTx.Writable() {
			ci.statsBucket, err = ci.currentTx.CreateBucket(columnInfoStatsBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if ci.statsBucket == nil {
				err = errors.New(fmt.Sprintf("Could not create bucket for column id %v statistics. Got empty value", ci.Id))
				tracelog.Error(err, packageName, funcName)
				return
			} else {
				tracelog.Info(packageName, funcName, "Bucket for column id %v statistics created", ci.Id)
			}
		} else {
			//tracelog.Info(packageName, funcName, "Bucket for column id %v statistics has not been created", ci.Id)
		}
	}
	return
}

func (cp *ColumnInfoType) CloseStorageTransaction(commit bool) (err error) {
	funcName := "ColumnInfoType.CloseStorageTransaction"
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
	}
	cp.ResetBuckets()
	tracelog.Completed(packageName, funcName)
	return
}

func (cp *ColumnInfoType) CloseStorage() (err error) {
	funcName := "ColumnPairType.CloseStorage"
	if cp.currentTx != nil {
		err = cp.currentTx.Rollback()
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
		cp.currentTx = nil
	}
	cp.ResetBuckets()
	if cp.storage != nil {
		cp.storage.Close()
		cp.storage = nil
	}

	tracelog.Completed(packageName, funcName)
	return
}

func (ci ColumnInfoType) FindDataCategory(
	byteLength uint16,
	isNumeric bool,
	isNegative bool,
	fpScale int8,
	isSubHash bool,
	subHash uint8,
) *ColumnDataCategoryStatsType {

	if ci.DataCategories == nil {
		return nil
	}
	for _, cdc := range ci.DataCategories {
		if byteLength == uint16(cdc.ByteLength.Value()) {
			if !isNumeric && isNumeric == cdc.IsNumeric.Value() {
				if !isSubHash && isSubHash == cdc.IsSubHash.Value() {
					return cdc
				} else if isSubHash == cdc.IsSubHash.Value() &&
					subHash == uint8(cdc.SubHash.Value()) {
					return cdc
				}
			} else if isNumeric == cdc.IsNumeric.Value() {
				if isNegative == cdc.IsNegative.Value() &&
					fpScale == int8(cdc.FloatingPointScale.Value()) {
					if !isSubHash && isSubHash == cdc.IsSubHash.Value() {
						return cdc
					} else if isSubHash == cdc.IsSubHash.Value() &&
						subHash == uint8(cdc.SubHash.Value()) {
						return cdc
					}
				}
			}
		}
	}
	return nil
}
