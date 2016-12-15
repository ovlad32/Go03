package metadata

import (
	jsnull "./../jsnull"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"os"
)

var columnPairCategoriesBucket = []byte("categories")
var columnPairStatsBucket = []byte("stats")
var columnPairIntersectionBucket = []byte("intersection")
//var columnPairCategoryStatsBucket = []byte("stats")
//var columnPairBitsetBucket = []byte("bitset")
//var columnPairStatsHashUniqueCountKey = []byte("uniqueHashCount")
var columnPairHashUniqueCountKey = []byte("uniqueHashCount")
var columnPairHashCategoryCountKey = []byte("categoryCount")

type ColumnPairType struct {
	dataCategory        []byte
	column1             *ColumnInfoType
	column2             *ColumnInfoType
	ProcessStatus       jsnull.NullString
	HashIntersectionCount   jsnull.NullInt64
	CategoryIntersectionCount jsnull.NullInt64
	column1RowCount     jsnull.NullInt64
	column2RowCount     jsnull.NullInt64
	//dataBucketName      []byte
	storage             *bolt.DB
	currentTx           *bolt.Tx
	CategoriesBucket    *bolt.Bucket
	//CategoryBucket      *bolt.Bucket
	//CategoryHashBucket  *bolt.Bucket
	//CategoryStatsBucket *bolt.Bucket
	//BitsetBucket        *bolt.Bucket
	StatsBucket         *bolt.Bucket
//	HashIntersectionBytes    *bytes.Buffer
	//HashIntersectionBitset    *sparsebitset.BitSet
	Assossiated  map[*ColumnPairType] uint64;
}

type ColumnPairsType []*ColumnPairType;

type byHashCount []*ColumnPairType;

func (v byHashCount) Len() int{
	return len(v)
}
func (v byHashCount) Less(i, j int) bool{
	return v[i].HashIntersectionCount.Value()> v[j].HashIntersectionCount.Value()
}

func (v byHashCount) Swap(i, j int)  {
	v[i], v[j] = v[j], v[i]
}


type byRowsCount []*ColumnPairType;

func (v byRowsCount) Len() int{
	return len(v)
}
func (v byRowsCount) Less(i, j int) bool{
	return (v[i].column1RowCount.Value() + v[i].column2RowCount.Value()) >
		(v[j].column1RowCount.Value() + v[j].column2RowCount.Value())
}

func (v byRowsCount) Swap(i, j int)  {
	v[i], v[j] = v[j], v[i]
}




func NewColumnPair(column1, column2 *ColumnInfoType, dataCategory []byte) (result *ColumnPairType, err error) {
	funcName := "NewColumnPair"
	tracelog.Started(funcName,packageName)
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
	result.CategoryIntersectionCount = jsnull.NewNullInt64(int64(0))
	result.HashIntersectionCount = jsnull.NewNullInt64(int64(0))
	result.column1RowCount = jsnull.NewNullInt64(int64(0))
	result.column2RowCount = jsnull.NewNullInt64(int64(0))


	/*result.dataBucketName = make([]byte, 8*2, 8*2)
	b81 := utils.Int64ToB8(result.column1.Id.Value())
	copy(result.dataBucketName, b81)

	b82 := utils.Int64ToB8(result.column2.Id.Value())
	copy(result.dataBucketName[8:], b82) */

	tracelog.Completed(packageName, funcName)
	return
}

func (cp ColumnPairType) String() string {
	return fmt.Sprintf("column pair:%v - %v",cp.column1,cp.column2)
}

func(cp ColumnPairType) PathToStorage() (pathTo, pathToFileName string, err error ) {
	funcName := "ColumnPairType.OpenStorage"
	tracelog.Startedf(funcName,packageName,"%v",cp)
	if cp.column1 == nil || cp.column2 == nil ||
		!cp.column1.Id.Valid() || !cp.column2.Id.Valid() {
		err = ColumnInfoNotInitialized
		tracelog.Errorf(err, packageName, funcName, "%v",cp)
		return
	}
	pathTo = "./DBS"
	pathToFileName = fmt.Sprintf("%v/%v-%v.boltdb", pathTo, cp.column1.Id.Value(), cp.column2.Id.Value())
	return
}

func (cp *ColumnPairType) OpenStorage(writable bool) (err error) {
	funcName := "ColumnPairType.OpenStorage"
	tracelog.Started(funcName,packageName)
	var path, file string
	if cp.storage == nil {
		path, file, err = cp.PathToStorage()
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}

		if !writable {
			if _, err = os.Stat(file); os.IsNotExist(err) {
				return
			}
		} else {
			err = os.MkdirAll(path, 0600)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
		}



		cp.storage, err = bolt.Open(file, 0600, nil)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
		if cp.storage == nil {
			tracelog.Warning(packageName, funcName, "Storage has not been created for column pair id=[%v,%v]", cp.column1.Id, cp.column2.Id)
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
			tracelog.Warning(packageName, funcName, "Transaction has not been opened for column pair id=[%v,%v]", cp.column1.Id, cp.column2.Id)
			return
		}
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (cp *ColumnPairType) OpenCategoriesBucket() (err error) {
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
				err = fmt.Errorf("Categories bucket has not been created for column pair %v",cp)
				tracelog.Error(err, packageName, funcName)
				return
			}

		}
	}
	return
}

func (cp *ColumnPairType) OpenStatsBucket() (err error) {
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
				err = fmt.Errorf("Stats bucket has not been created for column pair %v",cp)
				tracelog.Error(err, packageName, funcName)
				return
			}
		}
	}
	return
}
/*
func (cp *ColumnPairType) OpenCategoryBucket(dataCategory []byte) (err error) {
	funcName := "ColumnPairType.OpenCategoryBucket"
	cp.CategoryBucket = cp.CategoriesBucket.Bucket(dataCategory)
	if cp.CategoryBucket == nil {
		//cp.CategoryStatsBucket = nil
		//cp.CategoryHashBucket = nil
		if cp.currentTx.Writable() {
			cp.CategoryBucket, err = cp.CategoriesBucket.CreateBucket(dataCategory)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if cp.CategoryBucket == nil {
				err = fmt.Errorf("DataCategory bucket has not been created for column pair %v",cp)
				tracelog.Error(err, packageName, funcName)
				return
			}
		}
	}
	return
}
*/
/*
func (cp *ColumnPairType) OpenCategoryHashBucket() (err error) {
	funcName := "ColumnPairType.OpenCategoryHashBucket"
	cp.CategoryHashBucket = cp.CategoryBucket.Bucket(columnPairCategoryHashBucket)
	if cp.CategoryHashBucket == nil {
		if cp.currentTx.Writable() {
			cp.CategoryHashBucket, err = cp.CategoryBucket.CreateBucket(columnPairCategoryHashBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if cp.CategoryHashBucket == nil {
				err = fmt.Errorf("DataCategory hash bucket has not been created for column pair %v",cp)
				tracelog.Error(err, packageName, funcName)
				return
			}
		}
	}
	return
}

func (cp *ColumnPairType) OpenBitsetBucket() (err error) {
	funcName := "ColumnPairType.OpenCategoryStatsBucket"
	cp.BitsetBucket = cp.currentTx.Bucket(columnPairBitsetBucket)
	if cp.BitsetBucket == nil {
		if cp.currentTx.Writable() {
			cp.BitsetBucket, err = cp.currentTx.CreateBucket(columnPairBitsetBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if cp.BitsetBucket == nil {
				err = fmt.Errorf("Bitset bucket has not been created for column pair %v",cp)
				tracelog.Error(err, packageName, funcName)
				return
			}
		}
	}
	return
}


func (cp *ColumnPairType) OpenCategoryStatsBucket() (err error) {
	funcName := "ColumnPairType.OpenCategoryStatsBucket"
	cp.CategoryStatsBucket = cp.CategoryBucket.Bucket(columnPairCategoryStatsBucket)
	if cp.CategoryStatsBucket == nil {
		if cp.currentTx.Writable() {
			cp.CategoryStatsBucket, err = cp.CategoryBucket.CreateBucket(columnPairCategoryStatsBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if cp.CategoryStatsBucket == nil {
				err = fmt.Errorf("DataCategory stats bucket has not been created for column pair %v",cp)
				tracelog.Error(err, packageName, funcName)
				return
			}
		}
	}
	return
}*/


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
		//cp.CategoryBucket = nil
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
		cp.storage = nil
	}
	tracelog.Completed(packageName, funcName)
	return
}
/**

func (cp *ColumnPairType) match(categoryString string,category, rowNumber1, rowNumber2 *[]byte) (result bool,err error) {

	if cp.column1.RowsBucket == nil {
		err = cp.column1.OpenStorage(false)
		if err != nil {
			panic(err)
		}
		err = cp.column1.OpenRowsBucket()
		if err != nil {
			panic(err)
		}
	}

	if cp.column2.RowsBucket == nil {
		err = cp.column2.OpenStorage(false)
		if err != nil {
			panic(err)
		}
		err = cp.column2.OpenRowsBucket()
		if err != nil {
			panic(err)
		}
	}

	hashValue1 := cp.column1.RowsBucket.Get(rowNumber1);
	hashValue2 := cp.column2.RowsBucket.Get(rowNumber2);
	result = true;
	for index, b := range hashValue1[:8] {
		if hashValue2[index] != b {
			result = false;
			break;
		}
	}



	return
}
*/