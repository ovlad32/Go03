package dataflow

import (
	"astra/metadata"
	"astra/nullable"
	"bufio"
	"errors"
	"fmt"
	"github.com/goinggo/tracelog"
	"golang.org/x/net/context"
	"os"
	"sync"
)

type ColumnInfoType struct {
	*metadata.ColumnInfoType

	stringAnalysisLock  sync.Mutex
	numericAnalysisLock sync.Mutex

	categoryLock sync.Mutex
	categories   map[string]*DataCategoryType
}

func (ci *ColumnInfoType) CategoryByKey(simple *DataCategorySimpleType) (result *DataCategoryType) {
	ci.categoryLock.Lock()
	defer ci.categoryLock.Unlock()

	if ci.categories == nil {
		ci.categories = make(map[string]*DataCategoryType)
	}
	key := simple.Key()
	if value, found := ci.categories[key]; !found {
		result = simple.covert()
		ci.categories[key] = result
	} else {
		result = value
	}
	return result
}

func (ci *ColumnInfoType) AnalyzeStringValue(stringValue string) {
	ci.stringAnalysisLock.Lock()
	defer ci.stringAnalysisLock.Unlock()

	(*ci.NonNullCount.Reference())++

	if !ci.MaxStringValue.Valid() || ci.MaxStringValue.Value() < stringValue {
		ci.MaxStringValue = nullable.NewNullString(stringValue)
	}
	if !ci.MinStringValue.Valid() || ci.MinStringValue.Value() > stringValue {
		ci.MinStringValue = nullable.NewNullString(stringValue)
	}

	lValue := int64(len(stringValue))
	if !ci.MaxStringLength.Valid() {
		ci.MaxStringLength = nullable.NewNullInt64(lValue)
	} else if ci.MaxStringLength.Value() < lValue {
		(*ci.MaxStringLength.Reference()) = lValue

	}
	if !ci.MinStringLength.Valid() {
		ci.MinStringLength = nullable.NewNullInt64(lValue)
	} else if ci.MinStringLength.Value() > lValue {
		(*ci.MinStringLength.Reference()) = lValue

	}
}

func (ci *ColumnInfoType) AnalyzeNumericValue(floatValue float64) {
	ci.numericAnalysisLock.Lock()
	defer ci.numericAnalysisLock.Unlock()

	(*ci.NumericCount.Reference())++
	if !ci.MaxNumericValue.Valid() {
		ci.MaxNumericValue = nullable.NewNullFloat64(floatValue)
	} else if ci.MaxNumericValue.Value() < floatValue {
		(*ci.MaxNumericValue.Reference()) = floatValue
	}

	if !ci.MinNumericValue.Valid() {
		ci.MinNumericValue = nullable.NewNullFloat64(floatValue)
	} else if ci.MinNumericValue.Value() > floatValue {
		(*ci.MinNumericValue.Reference()) = floatValue
	}

}

/*

if !column.MaxNumericValue.Valid() {
column.MaxNumericValue = jsnull.NewNullFloat64(floatValue)
} else if column.MaxNumericValue.Value() < floatValue {
(*column.MaxNumericValue.Reference()) = floatValue
}*/

type TableInfoType struct {
	*metadata.TableInfoType
	TankWriter   *bufio.Writer
	tankFile     *os.File
	tankFileLock sync.Mutex
}

func (ti *TableInfoType) OpenTank(ctx context.Context, pathToTankDir string, flags int) (err error) {
	funcName := "TableInfoType.OpenTank"
	tracelog.Started(packageName, funcName)

	if pathToTankDir == "" {
		err = errors.New("Given path is empty")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	err = os.MkdirAll(pathToTankDir, 700)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Making directories for path %v", pathToTankDir)
		return err
	}

	pathToTankFile := fmt.Sprintf("%v%v%v.tank",
		pathToTankDir,
		os.PathSeparator,
		ti.Id.String(),
	)

	if ti.tankFile == nil && ((flags & os.O_CREATE) == os.O_CREATE) {
		ti.tankFileLock.Lock()
		defer ti.tankFileLock.Unlock()
		if ti.tankFile == nil && ((flags & os.O_CREATE) == os.O_CREATE) {
			file, err := os.OpenFile(pathToTankFile, flags, 0666)
			if err != nil {
				tracelog.Errorf(err, packageName, funcName, "Opening file %v", pathToTankFile)
				return err
			}
			ti.TankWriter = bufio.NewWriter(file)
			ti.tankFile = file
			go func() {
				funcName := "TableInfoType.OpenTank.closeTank"
				tracelog.Started(packageName, funcName)
				if ti.TankWriter != nil {
					select {
					case <-ctx.Done():
						ti.TankWriter.Flush()
						ti.tankFile.Close()
					}
				}
				tracelog.Completed(packageName, funcName)
			}()
		}
	}

	return
}
