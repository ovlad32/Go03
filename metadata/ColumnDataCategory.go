package metadata


import (
	jsnull "../src/util/jsnull"
	utils "./../utils"
	sparsebitset "../src/sparsebitset"
	"github.com/boltdb/bolt"
	"github.com/goinggo/tracelog"
	"encoding/binary"
	"fmt"
	"errors"
	"bytes"
)



type ColumnDataCategoryStatesConfig struct{
	StorageDirectory string
}

type storageState struct{
	storage *bolt.DB;
	writtenCount uint64
	writtenLimit uint64
}




type ColumnDataCategoryStatsType struct {
	Column             *ColumnInfoType
	ByteLength         jsnull.NullInt64
	IsNumeric          jsnull.NullBool
	IsNegative         jsnull.NullBool
	FloatingPointScale jsnull.NullInt64
	NonNullCount       jsnull.NullInt64
	HashUniqueCount    jsnull.NullInt64
	MinStringValue     jsnull.NullString `json:"min-string-value"`
	MaxStringValue     jsnull.NullString `json:"max-string-value"`
	MinNumericValue    jsnull.NullFloat64
	MaxNumericValue    jsnull.NullFloat64
	IsSubHash          jsnull.NullBool
	SubHash            jsnull.NullInt64
	CategoryBucket     *bolt.Bucket
	BitsetBucket       *bolt.Bucket
	HashValuesBucket   *bolt.Bucket
	CurrentHashBucket  *bolt.Bucket
	bitSetStorage      *storageState
	writtenCount       uint64
	HashValuesStorage  *bolt.DB
	config  *ColumnDataCategoryStatesConfig
}

func NewColumnDataCategoryFromBytes(k []byte) (result *ColumnDataCategoryStatsType, err error) {
	result = &ColumnDataCategoryStatsType{}
	if err = result.PopulateFromBytes(k); err != nil {
		return nil, err
	}
	return
}
/*
func (cdc *ColumnDataCategoryStatsType) Code() (result string, err error){
	mask := "%v%v."
	result = "."
	if !cdc.IsNumeric.Value() {
		result = fmt.Sprintf(mask,result,"S")
		result = fmt.Sprintf(mask,result,cdc.ByteLength.Value())
		if cdc.IsSubHash.Value() {
			result = fmt.Sprintf(mask,result,cdc.SubHash.Value())
		}
	} else {
		if cdc.IsNegative.Value() {
			result = fmt.Sprintf(mask,result,"N")
		} else {
			result = fmt.Sprintf(mask,result,"P")
		}
		result = fmt.Sprintf(mask,result,cdc.ByteLength)
		result = fmt.Sprintf(mask,result,cdc.FloatingPointScale.Value())
	}
	return
}

func (cdc *ColumnDataCategoryStatsType) openStorage(suffix string,options *bolt.Options) (storage *bolt.DB,err error) {
	code, err := cdc.Code();
	if err != nil {
		return err
	}

	return bolt.Open(
		fmt.Sprintf("%v%v%v%v",
			cdc.config.StorageDirectory,
			cdc.Column.Id.Value(),
			code,
			suffix,
		),0,options,
	);
}
func(cdc *ColumnDataCategoryStatsType) openBitSetStorage(options *bolt.Options) (err error) {
	storage, err := cdc.openStorage(".bs.boltdb",options)
	if err != nil {
		return err
	}
	cdc.bitSetStorage := &storageState{
		storage: storage,
		writtenCount:0,
		writtenLimit:options.
	}

	return
}

func(cdc *ColumnDataCategoryStatsType) Write() (err error) {
	if (cdc.BitSetStorage == nil) {
		err = cdc.openBitSetStorage(&bolt.Options{
			ReadOnly:false,
		})
		if err != nil {
			return err
		}
	}
	cdc.BitSetStorage.
}

*/


func (cdc *ColumnDataCategoryStatsType) ConvertToBytes() (result []byte, err error) {
	funcName := "ColumnDataCategoryStatsType.DataCategoryBytes"
	tracelog.Started(packageName, funcName)

	result = make([]byte, 3, 5)

	if !cdc.IsNumeric.Valid() {
		err = errors.New("IsNumeric not initialized!")
		tracelog.Error(err, packageName, funcName)
		return
	} else {
		if !cdc.FloatingPointScale.Valid() {
			err = errors.New("FloatingPointScale not initialized!")
			tracelog.Error(err, packageName, funcName)
			return
		}

		if !cdc.IsNegative.Valid() {
			err = errors.New("IsNegative not initialized!")
			tracelog.Error(err, packageName, funcName)
			return
		}
	}

	if !cdc.IsSubHash.Valid() {
		err = errors.New("IsSubHash not initialized!")
		tracelog.Error(err, packageName, funcName)
		return
	} else if !cdc.SubHash.Valid() {
		err = errors.New("SubHash not initialized!")
		tracelog.Error(err, packageName, funcName)
		return
	}

	if cdc.IsNumeric.Value() {
		result[0] = (1 << 2)
		if cdc.FloatingPointScale.Value() != -1 {
			if cdc.IsNegative.Value() {
				result[0] = result[0] | (1 << 0)
			}
		} else {
			result[0] = result[0] | (1 << 1)
			if cdc.IsNegative.Value() {
				result[0] = result[0] | (1 << 0)
			}
		}
	}

	binary.LittleEndian.PutUint16(result[1:], uint16(cdc.ByteLength.Value()))
	if cdc.IsNumeric.Value() {
		if cdc.FloatingPointScale.Value() != -1 {
			result = append(
				result,
				byte(cdc.FloatingPointScale.Value()),
			)
		}
	}
	if cdc.IsSubHash.Value() {
		result = append(
			result,
			byte(cdc.SubHash.Value()),
		)
	}
	return
}

func (cdc *ColumnDataCategoryStatsType) ResetBuckets() {
	cdc.CategoryBucket = nil
	cdc.BitsetBucket = nil
	cdc.HashValuesBucket = nil
	cdc.CurrentHashBucket = nil
}

func (cdc *ColumnDataCategoryStatsType) OpenBucket(dataCategoryBytes []byte) (newInstance bool, err error) {
	funcName := "ColumnDataCategoryStatsType.OpenBucket"
	tracelog.Started(packageName, funcName)
	if cdc == nil {
		tracelog.Alert("!!", packageName, funcName, "")
	}
	if cdc.Column == nil {
		err = ColumnInfoNotInitialized
		return
	}
	if dataCategoryBytes == nil {
		dataCategoryBytes, err = cdc.ConvertToBytes()
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
	} else if !cdc.IsNumeric.Valid() { //TODO:why?
		err = cdc.PopulateFromBytes(dataCategoryBytes)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
	}
	if cdc.Column.CategoriesBucket == nil {
		err = errors.New(fmt.Sprintf("Bucket for data categories has not been initialized! Column %v", cdc.Column.Id.Value()))
		tracelog.Error(err, packageName, funcName)
		return
	}

	cdc.CurrentHashBucket = nil
	cdc.BitsetBucket = nil
	cdc.HashValuesBucket = nil
	cdc.CategoryBucket = cdc.Column.CategoriesBucket.Bucket(dataCategoryBytes)
	if cdc.CategoryBucket == nil {
		if cdc.Column.CurrentTx.Writable() {
			cdc.CategoryBucket, err = cdc.Column.CategoriesBucket.CreateBucket(dataCategoryBytes)
			if err != nil {
				tracelog.Error(err, packageName, funcName)
				return
			}
			if cdc.CategoryBucket == nil {
				err = errors.New(fmt.Sprintf("Could not create bucket for column id %v and category %v. Got empty value", cdc.Column.Id, dataCategoryBytes))
				tracelog.Error(err, packageName, funcName)
				return
			} else {
				newInstance = true
				tracelog.Trace(packageName, funcName, "Bucket for column id %v and category %v created", cdc.Column.Id, dataCategoryBytes)
			}
		}
	}
	return
}

func (cdc *ColumnDataCategoryStatsType) OpenBitsetBucket() (err error) {
	funcName := "ColumnDataCategoryStatsType.OpenBitsetBucket"
	tracelog.Started(packageName, funcName)
	if cdc.BitsetBucket == nil {
		cdc.BitsetBucket = cdc.CategoryBucket.Bucket(columnInfoCategoryBitsetBucket)
		if cdc.BitsetBucket == nil {
			if cdc.Column.CurrentTx.Writable() {
				cdc.BitsetBucket, err = cdc.CategoryBucket.CreateBucket(columnInfoCategoryBitsetBucket)
				if err != nil {
					tracelog.Error(err, packageName, funcName)
					return
				}
			}
		}
	}
	return
}

func (cdc *ColumnDataCategoryStatsType) OpenHashValuesBucket() (err error) {
	funcName := "ColumnDataCategoryStatsType.OpenHashValuesBucket"
	tracelog.Started(packageName, funcName)
	if cdc.HashValuesBucket == nil {
		cdc.HashValuesBucket = cdc.CategoryBucket.Bucket(columnInfoCategoryHashBucket)
		if cdc.HashValuesBucket == nil {
			if cdc.Column.CurrentTx.Writable() {
				cdc.HashValuesBucket, err = cdc.CategoryBucket.CreateBucket(columnInfoCategoryHashBucket)
				if err != nil {
					tracelog.Error(err, packageName, funcName)
					return
				}
			}
		}
	}
	tracelog.Completed(packageName, funcName)
	return
}


func (cdc ColumnDataCategoryStatsType) String() (result string) {
	if !cdc.IsNumeric.Value() {
		result = fmt.Sprintf("char[%v]", cdc.ByteLength.Value())
	} else {
		if cdc.IsNegative.Value() {
			result = "-"
		} else {
			result = "+"
		}
		if cdc.FloatingPointScale.Value() != 0 {
			result = fmt.Sprintf("%vF[%v,%v]",
				result,
				cdc.ByteLength,
				cdc.FloatingPointScale.Value(),
			)
		} else {
			result = fmt.Sprintf("%vI[%v]",
				result,
				cdc.ByteLength,
			)
		}
	}
	if cdc.IsSubHash.Value() {
		result = fmt.Sprintf("%v(%v)", result, cdc.SubHash.Value())
	}
	return
}


func (ci *ColumnDataCategoryStatsType) PopulateFromBytes(k []byte) (err error) {
	kLen := len(k)
	if kLen < 2 {
		err = errors.New(fmt.Sprintf("Can not explan category for chain of bytes %v. Too short.", k))
		return
	}

	ci.ByteLength = jsnull.NewNullInt64(
		int64(binary.LittleEndian.Uint16(k[1:])),
	)

	ci.IsNumeric = jsnull.NewNullBool(k[0] == 0)
	if !ci.IsNumeric.Value() {
		ci.IsSubHash = jsnull.NewNullBool(kLen > 3)
		if ci.IsSubHash.Value() {
			ci.SubHash = jsnull.NewNullInt64(int64(k[3]))
		}
	} else {
		ci.IsNegative = jsnull.NewNullBool(((k[0] >> 0) & 0x01) > 0)
		isFp := ( k[0] >>1 ) & 0x01 == 0
		if isFp {
			ci.FloatingPointScale = jsnull.NewNullInt64(int64(k[3]))
			ci.IsSubHash = jsnull.NewNullBool(kLen > 4)
			if ci.IsSubHash.Value() {
				ci.SubHash = jsnull.NewNullInt64(int64(k[4]))
			}
		} else {
			ci.FloatingPointScale = jsnull.NewNullInt64(int64(0))
			ci.IsSubHash = jsnull.NewNullBool(kLen > 3)
			if ci.IsSubHash.Value() {
				ci.SubHash = jsnull.NewNullInt64(int64(k[3]))
			}
		}
	}
	return
}

func (cd *ColumnDataCategoryStatsType) RowIntersectionCount(hash, rows1Bytes []byte) (result uint64) {

	oneAgainstMany := func(one,many []byte) bool {
		value, _ := utils.B8ToUInt64(one)
		bs := sparsebitset.New(0);
		bs.ReadFrom(bytes.NewBuffer(many));
		return bs.Test(value)
	}
	rows2Bytes := cd.CategoryBucket.Get(hash)

	if len(rows1Bytes) == 8 && len(rows2Bytes) == 8 {
		found := true
		for index := 0; index<len(rows1Bytes); index++ {
			if rows1Bytes[index] != rows2Bytes[index] {
				found = false
				break;
			}
		}
		if found {
			result ++
		}
	} else if len(rows1Bytes) == 8 && len(rows2Bytes)>8 {
		if oneAgainstMany(rows1Bytes,rows2Bytes) {
			result++
		}
	} else if len(rows2Bytes) > 8 && len(rows1Bytes) == 8 {
		if oneAgainstMany(rows2Bytes,rows1Bytes) {
			result++
		}
	} else {
		bs1 :=  sparsebitset.New(0);
		bs1.ReadFrom(bytes.NewBuffer(rows1Bytes));
		bs2 :=  sparsebitset.New(0);
		bs2.ReadFrom(bytes.NewBuffer(rows2Bytes));
		cardinality, _ := bs1.IntersectionCardinality(bs2)
		result = result + cardinality
	}
	return
}

