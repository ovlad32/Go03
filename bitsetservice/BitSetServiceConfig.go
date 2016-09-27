package bitsetservice

import ("fmt"
	"./../metadata"
	)


type BitsetServiceConfig struct {
	DumpRootPath string
	BSRootPath string
	HRORootPath string
}

func(conf BitsetServiceConfig) checkBSRootPath() {
	if conf.BSRootPath == "" {
		panic("The path to bitset location is empty")
	}
}

func (conf BitsetServiceConfig) GetBSPath(column *metadata.ColumnInfoType)  (string) {
	conf.checkBSRootPath()
	column.СheckId()
	column.СheckTableInfo()
	column.TableInfo.СheckId()

	return fmt.Sprintf(
		"%v/%v/",
		conf.BSRootPath,
		column.TableInfo.Id.Int64,
	)
}


func (conf BitsetServiceConfig) GetBSPathFileName(column *metadata.ColumnInfoType, category string)  (string) {
	conf.checkBSRootPath()
	column.СheckId()
	column.СheckTableInfo()
	column.TableInfo.СheckId()

	return fmt.Sprintf(
		"%v/%v/%v/%v.bitset",
		conf.BSRootPath,
		column.TableInfo.Id.Int64,
		column.Id.Int64,
		category,
	)
}

func(conf BitsetServiceConfig) GetHROPath(column *metadata.ColumnInfoType) (string) {
	conf.checkBSRootPath()
	column.СheckId()
	column.СheckTableInfo()
	column.TableInfo.СheckId()

	return fmt.Sprintf(
		"%v/%v/%v/",
		conf.HRORootPath,
		column.TableInfo.Id.Int64,
		column.Id.Int64,
	)

}

func AlignInt64 (x,y int64) (int64,int64) {
	if x > y {
		return x, y
	} else {
		return y, x
	}
}


func AlignUInt64 (x,y uint64) (uint64,uint64) {
	if x > y {
		return x, y
	} else {
		return y, x
	}
}

func(bc * BitsetServiceConfig) DataIntersectionBitSetFullFileName(p *Pair, category string) (path,filename string) {
	return intersectionBitSetFullFileName(bc,p,"di",category)
}

func(bc * BitsetServiceConfig) RowsIntersectionBitSetFullFileName(p *Pair, category string) (path,filename string) {
	return intersectionBitSetFullFileName(bc,p,"ri",category)
}

func intersectionBitSetFullFileName(bc * BitsetServiceConfig, p *Pair, suffix string, category string) (path,filename string) {
	i1,i2 := AlignInt64(p.LeftColumnToRowBitSet.Column.TableInfo.Id.Int64,
		p.RightColumnToRowBitSet.Column.TableInfo.Id.Int64,
	)

	path = fmt.Sprintf("%v/%v-%v/",
		p.BSConfig.HRORootPath,
		i1,i2,
	)

	i1,i2 = AlignInt64(p.LeftColumnToRowBitSet.Column.Id.Int64,
		p.RightColumnToRowBitSet.Column.Id.Int64)

	filename = fmt.Sprintf("%v-%v-%v.%v.bitset",
		i1,i2,suffix,
		category,
	)

	return
}

