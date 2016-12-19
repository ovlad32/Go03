package metadata

import (
	jsnull "./../jsnull"
	utils "./../utils"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"os"
	"sync"
	"io"
)


var columnInfoCategoriesBucket = []byte("categories")

var columnInfoCategoryHashBucket = []byte("hash")
var columnInfoCategoryBitsetBucket = []byte("bitset")

var columnInfoCategoryStatsRowCountKey = []byte("rowCount")
var columnInfoCategoryStatsNonNullCountKey = []byte("nonNullCount")
var columnInfoCategoryStatsHashUniqueCountKey = []byte("uniqueHashCount")


var columnInfoStatsBucket = []byte("stats")
var columnInfoRowsBucket = []byte("rows")
var columnInfoStatsCategoryCountKey = []byte("categoryCount")
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
	CategoryCount    jsnull.NullInt64
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
	Storage          *bolt.DB
	CurrentTx        *bolt.Tx
	CategoriesBucket *bolt.Bucket
	RowsBucket       *bolt.Bucket
	StatsBucket      *bolt.Bucket
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
	ci.CategoriesBucket = nil
	ci.StatsBucket = nil
	ci.RowsBucket = nil
	if ci.DataCategories != nil {
		for _, dc := range ci.DataCategories {
			dc.ResetBuckets()
		}
	}
}
func (ci *ColumnInfoType) CleanStorage() (err error) {
	bucket := ci.CurrentTx.Bucket(columnInfoCategoriesBucket);
	if bucket != nil {
		err = ci.CurrentTx.DeleteBucket(columnInfoCategoriesBucket);
		if err != nil {
			return
		}
	}

	bucket = ci.CurrentTx.Bucket(columnInfoStatsBucket);
	if bucket != nil {
		ci.CurrentTx.DeleteBucket(columnInfoStatsBucket);
		if err != nil {
			return
		}
	}
	bucket = ci.CurrentTx.Bucket(columnInfoRowsBucket);
	if bucket != nil {
		ci.CurrentTx.DeleteBucket(columnInfoRowsBucket);
		if err != nil {
			return
		}
	}

	return
}


func (ci *ColumnInfoType) OpenStorage(writable bool) (err error) {
	funcName := "ColumnInfoType.OpenStorage"
	tracelog.Started(packageName, funcName)

	if !ci.Id.Valid() {
		err = ColumnIdNotInitialized
		return
	}
	if ci.TableInfo == nil {
		err = TableInfoNotInitialized
		return
	}
	if ci.Storage == nil {
		path := fmt.Sprintf("./%v", ci.TableInfo.PathToDataDir.Value())
		_ = os.MkdirAll(path, 0)
		file := fmt.Sprintf("%v/%v.boltdb", path, ci.Id.Value())
		ci.Storage, err = bolt.Open(
			file,
			0600,
			nil,
		)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
	}
	if ci.CurrentTx == nil {
		ci.CurrentTx, err = ci.Storage.Begin(writable)
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
	ci.CategoriesBucket = ci.CurrentTx.Bucket(columnInfoCategoriesBucket)
	if ci.CategoriesBucket == nil {
		if ci.CurrentTx.Writable() {
			ci.CategoriesBucket, err = ci.CurrentTx.CreateBucket(columnInfoCategoriesBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if ci.CategoriesBucket == nil {
				err = errors.New(fmt.Sprintf("Could not create bucket for column id %v data categories. Got empty value", ci.Id))
				tracelog.Error(err, packageName, funcName)
				return
			} else {
				//tracelog.Info(packageName, funcName, "Bucket for column id %v data categories created", ci.Id)
			}
		} else {
			//tracelog.Info(packageName, funcName, "Bucket for column id %v data categories has not been created", ci.Id)
		}
	}
	return
}

func (ci *ColumnInfoType) OpenStatsBucket() (err error) {
	funcName := "ColumnInfoType.OpenStatsBucket"

	ci.StatsBucket = ci.CurrentTx.Bucket(columnInfoStatsBucket)
	if ci.StatsBucket == nil {
		if ci.CurrentTx.Writable() {
			ci.StatsBucket, err = ci.CurrentTx.CreateBucket(columnInfoStatsBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if ci.StatsBucket == nil {
				err = errors.New(fmt.Sprintf("Could not create bucket for column id %v statistics. Got empty value", ci.Id))
				tracelog.Error(err, packageName, funcName)
				return
			} else {
				//tracelog.Info(packageName, funcName, "Bucket for column id %v statistics created", ci.Id)
			}
		} else {
			//tracelog.Info(packageName, funcName, "Bucket for column id %v statistics has not been created", ci.Id)
		}
	}
	return
}


func (ci *ColumnInfoType) OpenRowsBucket() (err error) {
	funcName := "ColumnInfoType.OpenRowsBucket"

	ci.RowsBucket = ci.CurrentTx.Bucket(columnInfoRowsBucket)
	if ci.RowsBucket == nil {
		if ci.CurrentTx.Writable() {
			ci.RowsBucket, err = ci.CurrentTx.CreateBucket(columnInfoRowsBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if ci.RowsBucket == nil {
				err = errors.New(fmt.Sprintf("Could not create bucket for column id %v data. Got empty value", ci.Id))
				tracelog.Error(err, packageName, funcName)
				return
			} else {
				//tracelog.Info(packageName, funcName, "Bucket for column id %v data created", ci.Id)
			}
		} else {
			//tracelog.Info(packageName, funcName, "Bucket for column id %v data has not been created", ci.Id)
		}
	}
	return
}

func (cp *ColumnInfoType) CloseStorageTransaction(commit bool) (err error) {
	funcName := "ColumnInfoType.CloseStorageTransaction"
	tracelog.Started(packageName, funcName)

	if cp.CurrentTx != nil {

		if commit {
			err = cp.CurrentTx.Commit()
		} else {
			err = cp.CurrentTx.Rollback()
		}
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}

		cp.CurrentTx = nil
	}
	cp.ResetBuckets()
	tracelog.Completed(packageName, funcName)
	return
}

func (cp *ColumnInfoType) CloseStorage() (err error) {
	funcName := "ColumnPairType.CloseStorage"
	tracelog.Started(packageName, funcName)

	if cp.CurrentTx != nil {
		err = cp.CurrentTx.Rollback()
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
		cp.CurrentTx = nil
	}
	cp.ResetBuckets()
	if cp.Storage != nil {
		cp.Storage.Close()
		cp.Storage = nil
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

func (c *ColumnInfoType) ShowStatsReport(out io.Writer) (err error ){
	if c.Storage == nil {
		err = c.OpenStorage(false)
		if err != nil {
			return
		}
	}
	fmt.Fprintf(out,"%v\n",c)
	if c.CategoriesBucket == nil {
		err = c.OpenCategoriesBucket()
		if err != nil {
			return
		}
	}
	fmt.Fprintf(out,"|-\"%v\"",string(columnInfoCategoriesBucket))
	c.CategoriesBucket.ForEach(
		func(category,_ []byte) (err error) {
			if category == nil {
				fmt.Fprintf(out,"| |-%nill\n")
				return nil
			}
			dc := &ColumnDataCategoryStatsType{Column:c}
			dc.PopulateFromBytes(category)

			fmt.Fprintf(out,"| |-%v\n",dc)
			_, err  = dc.OpenBucket(category)
			if err != nil {
				return
			}
			err = dc.OpenHashValuesBucket()
			if err != nil {
				return
			}

			var count = uint64(0);
			dc.HashValuesBucket.ForEach(
				func(_,_[]byte) error{
					count++;
					return nil
				},
			)
			fmt.Fprintf(out,"|    |-\"%v\"\n",string(columnInfoCategoryHashBucket))
			fmt.Fprintf(out,"|       |-%v key(s)\n",count)

			err = dc.OpenBitsetBucket()
			if err != nil {
				return
			}
			count = uint64(0);
			dc.BitsetBucket.ForEach(
				func(_,_[]byte) error{
					count++;
					return nil
				},
			)
			fmt.Fprintf(out,"|    |-\"%v\"\n",string(columnInfoCategoryBitsetBucket))
			fmt.Fprintf(out,"|       |-%v key(s)\n",count)

			count,_ = utils.B8ToUInt64(dc.CategoryBucket.Get(columnInfoCategoryStatsHashUniqueCountKey));
			fmt.Fprintf(out,"|    |-\"%v\" - %v\n",string(columnInfoCategoryStatsHashUniqueCountKey),count)

			count,_ = utils.B8ToUInt64(dc.CategoryBucket.Get(columnInfoCategoryStatsNonNullCountKey));
			fmt.Fprintf(out,"|    |-\"%v\" - %v\n",string(columnInfoCategoryStatsNonNullCountKey),count)

			return nil;
		},
	)
	err = c.OpenRowsBucket()
	if err != nil {
		return
	}
	if c.RowsBucket == nil {
		return
	}

	var count = uint64(0);

	c.RowsBucket.ForEach(
		func(_,_[]byte) error{
			count++;
			return nil
		},
	)
	fmt.Fprintf(out,"|-\"%\"\n",string(columnInfoRowsBucket))
	fmt.Fprintf(out,"| |-%v key(s)\n",count)

	err = c.OpenStatsBucket()
	if err != nil {
		return
	}


	fmt.Fprintf(out,"|-\"%\"\n",string(columnInfoStatsBucket))
	count,_ = utils.B8ToUInt64(c.StatsBucket.Get(columnInfoStatsCategoryCountKey));
	fmt.Fprintf(out,"  |-\"%v\" - %v\n",string(columnInfoStatsCategoryCountKey),count)

	count,_ = utils.B8ToUInt64(c.StatsBucket.Get(columnInfoStatsHashUniqueCountKey));
	fmt.Fprintf(out,"  |-\"%v\" - %v\n",string(columnInfoStatsHashUniqueCountKey),count)

	count,_ = utils.B8ToUInt64(c.StatsBucket.Get(columnInfoStatsNonNullCountKey));
	fmt.Fprintf(out,"  |-\"%v\" - %v\n",string(columnInfoStatsNonNullCountKey),count)

	fmt.Fprintf(out,"\n")



	return
}