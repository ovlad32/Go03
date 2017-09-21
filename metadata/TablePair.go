package metadata

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"os"
)

var tablePairSourceBucket = []byte("source")
var tablePairStatsBucket = []byte("stats")

type TablePairType struct {
	table1 *TableInfoType
	table2 *TableInfoType
	//dataBucketName    *[]byte
	IntersectionCount uint64
	storage           *bolt.DB
	currentTx         *bolt.Tx
	SourceBucket      *bolt.Bucket
	StatsBucket       *bolt.Bucket
}

func NewTablePair(table1, table2 *TableInfoType) (result *TablePairType, err error) {
	funcName := "NewTablePair"
	tracelog.Started(funcName, packageName)
	if table1 == nil || table2 == nil ||
		!table1.Id.Valid() || !table2.Id.Valid() {
		err = TableInfoNotInitialized
		tracelog.Error(err, packageName, funcName)
		return
	}
	result = &TablePairType{
		table1: table1,
		table2: table2,
	}
	if table1.Id.Value() > table2.Id.Value() {
		result.table1, result.table2 = result.table2, result.table1
	}
	tracelog.Completedf(packageName, funcName, "%v", result)
	return
}

func (tp TablePairType) String() string {
	return fmt.Sprintf("table pair:%v - %v", tp.table1, tp.table2)
}

func (tp TablePairType) PathToStorage() (pathTo, pathToFileName string, err error) {
	funcName := "TablePairType.OpenStorage"
	tracelog.Startedf(funcName, packageName, "%v", tp)
	if tp.table1 == nil || tp.table2 == nil ||
		!tp.table1.Id.Valid() || !tp.table2.Id.Valid() {
		err = ColumnInfoNotInitialized
		tracelog.Errorf(err, packageName, "%v", funcName, tp)
		return
	}
	pathTo = "./DBT"
	pathToFileName = fmt.Sprintf("%v/%v-%v.boltdb", pathTo, tp.table1.Id.Value(), tp.table2.Id.Value())
	return
}

func (tp *TablePairType) OpenStorage(writable bool) (err error) {
	funcName := "TablePairType.OpenStorage"
	tracelog.Startedf(funcName, packageName, "%v", tp)
	var path, file string
	if tp.storage == nil {
		path, file, err = tp.PathToStorage()

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

		tp.storage, err = bolt.Open(file, 0600, nil)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
		if tp.storage == nil {
			tracelog.Warning(packageName, funcName, "Storage has not been created for table pair id=[%v,%v]", tp.table1.Id, tp.table2.Id)
			return
		}
	}

	if tp.currentTx == nil {
		tp.currentTx, err = tp.storage.Begin(writable)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return err
		}
		if tp.currentTx == nil {
			tracelog.Warning(packageName, funcName, "Transaction has not been opened for table pair id=[%v,%v]", tp.table1.Id, tp.table2.Id)
			return
		}
	}
	tracelog.Completedf(packageName, funcName, "%v", tp)
	return
}

func (tp *TablePairType) OpenSourceBucket() (err error) {
	funcName := "TablePairType.OpenSourceBucket"
	tp.SourceBucket = tp.currentTx.Bucket(tablePairSourceBucket)
	if tp.SourceBucket == nil {
		if tp.currentTx.Writable() {
			tp.SourceBucket, err = tp.currentTx.CreateBucket(tablePairSourceBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if tp.SourceBucket == nil {
				err = fmt.Errorf("Source bucket has not been created for table pair %v", tp)
				tracelog.Error(err, packageName, funcName)
				return
			}

		}
	}
	return
}

func (tp *TablePairType) OpenStatsBucket() (err error) {
	funcName := "TablePairType.OpenStatsBucket"
	tp.StatsBucket = tp.currentTx.Bucket(tablePairStatsBucket)
	if tp.StatsBucket == nil {
		if tp.currentTx.Writable() {
			tp.StatsBucket, err = tp.currentTx.CreateBucket(tablePairStatsBucket)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if tp.StatsBucket == nil {
				err = fmt.Errorf("Stats bucket has not been created for table pair %v", tp)
				tracelog.Error(err, packageName, funcName)
				return
			}
		}
	}
	return
}

func (tp *TablePairType) CloseStorageTransaction(commit bool) (err error) {
	funcName := "TablePairType.CloseStorageTransaction"
	if tp.currentTx != nil {
		if commit {
			err = tp.currentTx.Commit()
		} else {
			err = tp.currentTx.Rollback()
		}
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
		tp.currentTx = nil
		tp.SourceBucket = nil
		tp.StatsBucket = nil
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (tp *TablePairType) CloseStorage() (err error) {
	funcName := "TablePairType.CloseStorage"
	tp.CloseStorageTransaction(false)
	if tp.storage != nil {
		tp.storage.Close()
		tp.storage = nil
	}
	tracelog.Completed(packageName, funcName)
	return
}
