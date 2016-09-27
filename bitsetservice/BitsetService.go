package bitsetservice

import (
	"fmt"
	"io"
	"math"
	"database/sql"
	"strconv"
	"os"
	"log"
	"bufio"
	"../sparsebitset"
	"bytes"
	"hash"
	"./../metadata"
	"hash/fnv"
	"sync"
	"io/ioutil"
	"compress/gzip"
	"github.com/boltdb/bolt"
)
type DumpConfigurationType struct {
	GZIP bool
	FieldSeparator byte
	LineSeparator byte
	InputBufferSize int
}
type PeerInfo struct{
	LeftColumnRowsIntersection *sparsebitset.BitSet
	RightColumnRowsIntersection *sparsebitset.BitSet
	LeftColumnRowsIntersectionCardinality uint64
	RightColumnRowsIntersectionCardinality uint64
	LeftColumnRowsIsSuperSet bool
	LeftColumnRowsIsSubSet bool
	RightColumnRowsIsSuperSet bool
	RightColumnRowsIsSubSet bool
}

func(peerInfo PeerInfo) String() (string) {
	result:= fmt.Sprintf(
		"(RIC:%v, SubSet:%v, SuperSet:%v) <-> (RIC:%v, SubSet:%v, SuperSet:%v)",
//		"%v|%v|%v|%v|%v|%v",
		peerInfo.LeftColumnRowsIntersectionCardinality,
		peerInfo.LeftColumnRowsIsSubSet,
		peerInfo.LeftColumnRowsIsSuperSet,
		peerInfo.RightColumnRowsIntersectionCardinality,
		peerInfo.RightColumnRowsIsSubSet,
		peerInfo.RightColumnRowsIsSuperSet,
	)
	return result
}

type ColumnToBitSet struct{
	Column *metadata.ColumnInfoType
	RowNumbers *sparsebitset.BitSet
	RowNumbersCardinality uint64
	//PeersCardinality map[*JoinBit][]uint64
	//Cardinality uint64
}
type Pair struct {
	BSConfig *BitsetServiceConfig
	LeftColumnToRowBitSet ColumnToBitSet
	RightColumnToRowBitSet ColumnToBitSet
	Intersections map[string]bool
	IntersectionCardinality uint64
	HashedRows *sparsebitset.BitSet
	Peers map[*Pair] *PeerInfo
	TruePositive bool
}



func(p *Pair) SaveDataIntersectionBitSet(category string,bitset *sparsebitset.BitSet) {

		path, filename := p.BSConfig.DataIntersectionBitSetFullFileName(p,category)
		if err := os.MkdirAll(path, 0); err != nil {
			panic(err)
		}
		if file, err := os.Create(path + filename); err != nil {
			panic(err)
		} else {
			defer file.Close()
			bitset.WriteTo(file)
		}
}

func(p *Pair) LoadDataIntersectionBitSet(category string) *sparsebitset.BitSet{
	path, filename := p.BSConfig.DataIntersectionBitSetFullFileName(p, category)
	if file, err := os.Open(path + filename); err != nil {
		panic(err)
	} else {
		defer file.Close()
		bitset := sparsebitset.New(0)
		bitset.ReadFrom(file)
		return bitset
	}
}

func(p *Pair) BuildRowsIntersectionBitSet(hashChannel chan uint64, category string) {
	type THRO map[uint64][]uint64
	var rowsL, rowsR []uint64
	hasher := fnv.New64()
	hashRowsL := make(THRO)
	hashRowsR := make(THRO)
	load := func(hrox *THRO, buffer *[]byte) {
		//var arr64 []uint64
		reader := bytes.NewReader(*buffer)
		hro := &HRO{}
		for {
			_, err := hro.ReadFrom(reader)
			if err == nil {
				_, found := (*hrox)[hro.Hash]
				if !found {
					(*hrox)[hro.Hash] = make([]uint64, 0, 100)
				}
				(*hrox)[hro.Hash] = append((*hrox)[hro.Hash], hro.Data1)
			} else if err == io.EOF {

				return
			} else {
				panic(err)
			}
		}
	}

	for hash := range hashChannel {
		var found bool
		key := HROKey(hash)
		if rowsL, found = hashRowsL[hash]; !found {
			buffer, err := ioutil.ReadFile(p.BSConfig.GetHROPath(p.LeftColumnToRowBitSet.Column) + category + "/" + key);
			if err != nil {
				panic(err)
			} else {
				load(&hashRowsL, &buffer)
				rowsL = hashRowsL[hash]
			}
		}
		if rowsR, found = hashRowsR[hash]; !found {
			buffer, err := ioutil.ReadFile(p.BSConfig.GetHROPath(p.RightColumnToRowBitSet.Column) + category + "/" + key);
			if err != nil {
				panic(err)
			} else {
				load(&hashRowsR, &buffer)
				rowsR = hashRowsR[hash]
			}
		}
		for _, rowL := range rowsL {
			for _, rowR := range rowsR {
				hasher.Reset()
				if p.LeftColumnToRowBitSet.Column.Id.Int64 >
					p.RightColumnToRowBitSet.Column.Id.Int64 {
					 rowR, rowL = rowL, rowR
				}
				hasher.Write([]byte(fmt.Sprintf("%v:%v",rowL, rowR)))
				p.HashedRows.Set(hasher.Sum64())
			}
		}

	}

}


func(p *Pair) String() (string) {
	var result string
	result = fmt.Sprintf("%v",p.LeftColumnToRowBitSet.Column)
	result = result + fmt.Sprintf(" <%v> ",p.IntersectionCardinality)
	result = result + fmt.Sprintf("%v",p.RightColumnToRowBitSet.Column)
	return result
}
const (
	BSMessageTable  int = 1
	BSMessageNewBucket int  = 2
	BSMessagePayload  int = 3
)

type DataBSMessageType struct {
  	table *metadata.TableInfoType
	column *metadata.ColumnInfoType
	lineNumber uint64
	dataImage *[]byte
	command byte
	//dataHash *[]byte
}

func TableDataExtractor(
	in chan DataBSMessageType,
	out chan DataBSMessageType,
	dumpConfig DumpConfigurationType,
	) {
	for message := range in {
		if BSMessageTable != message.command {
			continue
		}
		file, err := os.Open(message.table.String)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		if dumpConfig.GZIP {
			file, err = gzip.NewReader(file)
			if err != nil {
				panic(err)
			}
			defer file.Close()
		}

		rawData := bufio.NewReaderSize(file, dumpConfig.InputBufferSize)
		metadataColumnCount := len(message.table.Columns)

		lineNumber := uint64(0)

		for {
			line, err := rawData.ReadSlice(dumpConfig.LineSeparator)
			if err == io.EOF {
				close(out)
				break
			} else if err != nil {
				panic(err)
			}
			lineNumber++

			//line = strings.TrimSuffix(line, string(byte(0xD)))

			lineColumns := bytes.Split(line,dumpConfig.FieldSeparator)

			lineColumnCount := len(lineColumns)
			if metadataColumnCount != lineColumnCount {
				panic(fmt.Sprintf("Number of column mismatch in line %v. Expected #%v; Actual #%v",
					lineNumber,
					metadataColumnCount,
					lineColumnCount,
				))
			}

			for columnIndex := range message.table.Columns {
				if lineNumber == 1 {
					out <- DataBSMessageType{
						column:message.table.Columns[columnIndex],
						command:BSMessageNewBucket,
					}
				}
				out <- DataBSMessageType{
					column:message.table.Columns[columnIndex],
					lineNumber:lineNumber,
					dataImage:lineColumns[columnIndex],
					command:BSMessagePayload,
				}
			}

		}
	}
}

func DataBitSetBuilder (
	in chan DataBSMessageType,
	out chan DataBSMessageType,
	){
	for message := range in {
		switch message.command {
		case BSMessageNewBucket: {
			appConfig.Db.Update(func(tx *bolt.Tx) error {
				bucketName := []byte(message.column.Id)
				tx.DeleteBucket("")
			})
		}

		}
		message.column.Id.Int64
	}


}
func TableBitSetProcessor(
	in <- chan *metadata.TableInfoType,
	done chan <- bool,
	bsConf *BitsetServiceConfig) {
	var totalBytesRead uint64 = 0
	var rowCounter uint64 = 0
	var hasher hash.Hash64;

	hasher = fnv.New64()
	for tableInfo := range in {
		tableInfo.СheckId()
		tableInfo.СheckColumns()
		datFile, err := os.Open(bsConf.DumpRootPath + tableInfo.DumpFileName.String)
		if err != nil {
			panic(err)
			//log.Fatal(err)
		}
		dumpBufferedReader := bufio.NewReaderSize(datFile, 4096)
		columnAuxs := make([]ColumnAuxiliaries, 0, len(tableInfo.Columns))
		rowCounter = uint64(0);
		//var columnWaiter  sync.WaitGroup
		fileReading:
		for {
			rowBuffer, error := dumpBufferedReader.ReadSlice(lineDelimiter[0])

			switch  true {
			case error == nil: {}
			case error == io.EOF: {
				fmt.Println("Done building bitsets for",tableInfo.DumpFileName.String,len(rowBuffer))
				for num, _:= range columnAuxs {
					for category,_:= range columnAuxs[num].bitsets {
						err := os.MkdirAll(bsConf.GetBSPath(columnAuxs[num].columnInfo), 0)
						if err != nil {
							panic(err)
						}

						outputFile, err := os.Create(
							bsConf.GetBSPathFileName(columnAuxs[num].columnInfo, category),
						)

						if err != nil {
							panic(err)
						}

						columnAuxs[num].bitsets[category].WriteTo(outputFile)
						columnAuxs[num].bitsets[category] = nil
					}
					//ToDO:save stats
					//fmt.Println(columnAuxs[num].columnInfo.ColumnName.String)
					/*fmt.Println(columnAuxs[num].columnInfo.MaxNumericValue.String)
					fmt.Println(columnAuxs[num].columnInfo.MinNumericValue.String)*/

					//fmt.Println(columnAuxs[num].columnInfo.NullCount.Int64)
					close(columnAuxs[num].statsChannel)
					close(columnAuxs[num].hroChannel)
					datFile.Close()
				}

				for num, _:= range columnAuxs {
					<- columnAuxs[num].statsBackChannel
				}

				for num, _:= range columnAuxs {
					<- columnAuxs[num].hroBackChannel
				}

				break fileReading
			}
			default: {
				panic(error)
				//log.Fatal(error)
			}
			}


			if rowCounter == 0 {
				fmt.Println("Start building bitsets for ",tableInfo.DumpFileName.String,len(rowBuffer))
				for index := range tableInfo.Columns {

					columnAux := ColumnAuxiliaries{
						columnInfo : &tableInfo.Columns[index],
						bitsets : make(map[string] *sparsebitset.BitSet),
						//bitset: sparsebitset.New(0),
						hroChannel: make(chan *HRO, 1024),
						hroBackChannel: make(chan  bool),
						statsChannel: make(chan string, 1024),
						statsBackChannel: make(chan bool),
					}
					go StatisticsProcessor(
						columnAux.statsChannel,
						columnAux.statsBackChannel,
						&tableInfo.Columns[index],
					);
					go HRODumpingProcessor(
						columnAux.hroChannel,
						columnAux.hroBackChannel,
						&tableInfo.Columns[index],
						bsConf,
					)

					columnAuxs = append(columnAuxs, columnAux);
				}
			}

			rowCounter ++

			columnDataBuffers := bytes.Split(rowBuffer, fieldDelimiter)

			if len(columnDataBuffers) != len(tableInfo.Columns) {
				panic(
					fmt.Sprintf(
						"Column mismatch! " +
							"Table %v has %v columns. " +
							"Gotten %v colums in line %v in %v",
						tableInfo,
						len(tableInfo.Columns),
						len(columnDataBuffers),
						rowCounter,
						tableInfo.DumpFileName,
					),
				)
			}
			for columnNumber := range columnAuxs {
				if len(columnDataBuffers[columnNumber]) == 0 {
					continue
				}
				ival := int64(0)
				hro := new(HRO)
				sval := string(columnDataBuffers[columnNumber])
				hro.Category,ival = getHashCategory(&sval)
				if hro.Category == "P" {
					hro.Hash = uint64(ival)
				} else if hro.Category == "N" {
					hro.Hash = uint64(-ival)
				} else {
					hasher.Reset()
					hasher.Write(columnDataBuffers[columnNumber])
					hro.Hash = hasher.Sum64()
				}
				hro.Category = fmt.Sprintf("%v%v",hro.Category,len(columnDataBuffers[columnNumber]))
				hro.Data1    = rowCounter
				hro.Data2    = totalBytesRead
				/*hro := HRO{
					Hash: hasher.Sum64(),
					FileOffset: totalBytesRead,
					RowNumber: rowCounter,
				}*/

				if bitset,bFound := columnAuxs[columnNumber].bitsets[hro.Category];!bFound {
					bitset = sparsebitset.New(0)
					bitset.Set(hro.Hash)
					columnAuxs[columnNumber].bitsets[hro.Category] = bitset
					if columnAuxs[columnNumber].columnInfo.HashCategories == nil {
						columnAuxs[columnNumber].columnInfo.HashCategories = make(map[string]bool)
					}
					columnAuxs[columnNumber].columnInfo.HashCategories[hro.Category]=true
				} else {
					bitset.Set(hro.Hash)
				}
				columnAuxs[columnNumber].hroChannel <- hro
			}

		}
	}
	done <- true

}

func getHashCategory(sval *string) (cat string,ival int64){

	ival,ierr := strconv.ParseInt(*sval,10,64)
	if ierr == nil {
		if ival < 0 {
			cat = "N"
		} else {
			cat = "P"
		}
		return
	}
	_, ferr := strconv.ParseFloat(*sval,64)
	if ferr == nil {
		return "F", 0
	}
	return "S", 0
}

func HRODumpingProcessor(
	in <- chan *HRO,
	done  chan <- bool  ,
	column *metadata.ColumnInfo,
	conf *BitsetServiceConfig,
	) {
	files := make(map[string] *bufio.Writer)
	//cnt:=uint32(0)
	for hro := range in {
	//	cnt += (*hr).RowNumber

		key := hro.Category + "/" + hro.getKey()
		var writer *bufio.Writer;
		if stored, found := files[key]; !found {
			hroPath := conf.GetHROPath(column)
			os.MkdirAll(hroPath + hro.Category + "/",0)
			created, error := os.Create(hroPath + key)
//			fmt.Println(hroPath + key)
//			cnt++
			if error != nil {
				log.Fatal(error)
			}
			defer created.Close()
			writer = bufio.NewWriterSize(created,4096)
			//writer = file
			files[key] = writer
		} else {
			writer = stored;
		}
		_,err := hro.WriteTo(writer)
		if err != nil {
			panic(err)
		}
	}
	for _,w := range files {
		w.Flush()

	}
	//notifee.Done()
	/*if column.ColumnName.String=="CONTRACT_ID" {
		fmt.Println(cnt)
	}*/
	fmt.Println(column,"dump done")
	done <-true
}


func StatisticsProcessor(
	in <-chan string,
	done chan <- bool,
	column *metadata.ColumnInfo,
	) {
	var smin,smax string

	fmin,fmax := math.MaxFloat64,-math.MaxFloat64
	lmin,lmax := math.MaxInt64,math.MinInt64
	isNumeric, isFloat := true, false
	isEmpty := true
	nullCounter := uint64(0);
	for sval := range in {

		isEmpty = false;

		//sval, _ := sqlValue.Value();

		slen := len(sval)
		if slen == 0 {
			nullCounter++
		}

		if  slen < lmin {
			lmin = slen
		}
		if slen > lmax {
			lmax = slen
		}

		if smin == "" && sval != ""{
			smin = sval
		}
		if sval < smin {
			smin = sval
		}
		if sval > smax {
			smax = sval
		}
		if isNumeric {
			fval, err := strconv.ParseFloat(sval,64);
			if err != nil {
				isNumeric = false
			}

			if isNumeric && !isFloat {
				if float64(math.Trunc(fval)) != fval {
					isFloat = true
				}
			}

			if isNumeric {
				if fval<fmin {
					fmin = fval
				}
				if fval>fmax {
					fmax = fval
				}
			}
		}

	}

	if !isEmpty {
		column.NullCount = sql.NullInt64{Valid:false}
		column.MinStringValue = sql.NullString{Valid:false}
		column.MaxStringValue = sql.NullString{Valid:false}
		column.MinStringLength = sql.NullInt64{Valid:false}
		column.MaxStringLength = sql.NullInt64{Valid:false}
		column.MinNumericValue = sql.NullString{Valid:false}
		column.MaxNumericValue = sql.NullString{Valid:false}
	} else {
		column.NullCount = sql.NullInt64{Int64:int64(nullCounter), Valid:true}
		column.MinStringValue = sql.NullString{String:smin, Valid:true}
		column.MaxStringValue = sql.NullString{String:smax, Valid:true}
		column.MinStringLength = sql.NullInt64{Int64:int64(lmin),Valid:true}
		column.MaxStringLength = sql.NullInt64{Int64:int64(lmax),Valid:true}
		if isNumeric {
			column.MinNumericValue = sql.NullString{String:strconv.FormatFloat(fmin,'f',-1,32), Valid:true}
			column.MaxNumericValue = sql.NullString{String:strconv.FormatFloat(fmax,'f',-1,32), Valid:true}
		} else {
			column.MinNumericValue = sql.NullString{Valid:false}
			column.MaxNumericValue = sql.NullString{Valid:false}
		}
	}

	done<-true

}

func JoinRowBitSetProcessor(in <- chan *Pair, done chan <-bool, bsConf *BitsetServiceConfig) {


	var hasher hash.Hash64;

	hasher = fnv.New64()

	pairs := make([]*Pair,0)
	for pair := range in {
		pair.LeftColumnToRowBitSet.RowNumbers = sparsebitset.New(0)
		pair.RightColumnToRowBitSet.RowNumbers = sparsebitset.New(0)
		pair.HashedRows = sparsebitset.New(0)
		loadRowBitSet := func(hashChannel chan uint64,
		//		     done chan bool,
				     pair *Pair,
				     category string,
			){
			type THRO map[uint64][]uint64
			var rowsL,rowsR []uint64
			way := pair.LeftColumnToRowBitSet.Column.Id.Int64 >
				  pair.RightColumnToRowBitSet.Column.Id.Int64

			hashRowsL := make(THRO)
			hashRowsR := make(THRO)
			load := func (hrox *THRO, buffer *[]byte) {
				//var arr64 []uint64
				reader := bytes.NewReader(*buffer)
				hro := &HRO{}
				for {
					_, err := hro.ReadFrom(reader)
					if err == nil {
						_,found := (*hrox)[hro.Hash]
						if !found {
							(*hrox)[hro.Hash] = make([]uint64,0,100)
						}
						(*hrox)[hro.Hash] = append((*hrox)[hro.Hash],hro.Data1)
					} else if err == io.EOF {

						return
					} else {
						panic(err)
					}
				}
			}

			for hash := range hashChannel {
				var found bool
				key := HROKey(hash)
				if rowsL, found = hashRowsL[hash]; !found {
					buffer, err := ioutil.ReadFile(bsConf.GetHROPath(pair.LeftColumnToRowBitSet.Column) + category + "/" + key);
					if  err != nil {
						panic(err)
					} else {
						load(&hashRowsL, &buffer)
						rowsL = hashRowsL[hash]
					}
				}
				if rowsR, found = hashRowsR[hash]; !found {
					buffer, err := ioutil.ReadFile(bsConf.GetHROPath(pair.RightColumnToRowBitSet.Column) + category + "/" + key);
					if  err != nil {
						panic(err)
					} else {
						load(&hashRowsR, &buffer)
						rowsR = hashRowsR[hash]
					}
				}
				for _, rowL := range rowsL {
					for _, rowR := range rowsR {
						hasher.Reset()
						if way {
							hasher.Write([]byte(fmt.Sprintf("%v:%v", rowL, rowR)))
						} else {
							hasher.Write([]byte(fmt.Sprintf("%v:%v", rowR, rowL)))
						}
						pair.HashedRows.Set(hasher.Sum64())
					}
				}

			}
		//	done <- true
		}



		//log.Printf("loading rows for %v\n",pair)
		//done := make(chan bool)
		_=loadRowBitSet
		//cnt := 0
		//start := time.Now()
		//fmt.Println("\n",pair," -> ")

		for category, isPresent := range pair.Intersections {
			//fmt.Print(" ",category)
			if isPresent {
				bs := pair.LoadDataIntersectionBitSet(category)
				loadRowBitSet(bs.BitChan(), pair, category)
			}
		}

		//fmt.Println(float64(cnt)/float64(time.Since(start).Nanoseconds()))
		//log.Printf("loaded rows for %v\n",pair)
		//pair.LeftColumnToRowBitSet.RowNumbersCardinality = pair.LeftColumnToRowBitSet.RowNumbers.Cardinality();
		//pair.RightColumnToRowBitSet.RowNumbersCardinality = pair.RightColumnToRowBitSet.RowNumbers.Cardinality();
		pairs = append(pairs, pair)
	}


	for pairIndex,_ := range pairs {
		fmt.Println("",pairs[pairIndex]," -> ")
			for peerIndex,_ := range pairs  {
				if pairIndex == peerIndex {
					continue
				}
				if pairs[pairIndex].LeftColumnToRowBitSet.Column == pairs[peerIndex].LeftColumnToRowBitSet.Column ||
					pairs[pairIndex].RightColumnToRowBitSet.Column == pairs[peerIndex].RightColumnToRowBitSet.Column {
					continue
				}

				if pairs[pairIndex].Peers == nil{
					pairs[pairIndex].Peers = make(map[*Pair]*PeerInfo)
				}
				if pairs[peerIndex].Peers == nil{
					pairs[peerIndex].Peers = make(map[*Pair]*PeerInfo)
				}

				if _,found := pairs[pairIndex].Peers[pairs[peerIndex]];found{
					continue
				}

				if pairs[pairIndex].HashedRows != nil {
					IntersectionResult := pairs[pairIndex].HashedRows.Intersection(
						pairs[peerIndex].HashedRows,
					)
					fmt.Println(fmt.Sprintf("  - %v hrc: %v", pairs[peerIndex], IntersectionResult.Cardinality()))
				}
				/*					pairs[pairIndex].LeftColumnToRowBitSet.Column.ColumnName.String,



									pairs[peerIndex].LeftColumnToRowBitSet.Column.ColumnName.String,
									leftIntersectionResult.Cardinality())
								rightIntersectionResult := pairs[pairIndex].RightColumnToRowBitSet.RowNumbers.Intersection(
									pairs[peerIndex].RightColumnToRowBitSet.RowNumbers,
								)
								leftIntersectionResultCardinality := leftIntersectionResult.Cardinality()
								rightIntersectionResultCardinality := rightIntersectionResult.Cardinality()

								if leftIntersectionResultCardinality>0 && rightIntersectionResultCardinality>0 {
									pairs[pairIndex].Peers[pairs[peerIndex]] = &PeerInfo{
										LeftColumnRowsIntersection: leftIntersectionResult,
										LeftColumnRowsIntersectionCardinality: leftIntersectionResultCardinality,
										RightColumnRowsIntersection: rightIntersectionResult,
										RightColumnRowsIntersectionCardinality: rightIntersectionResultCardinality,
										LeftColumnRowsIsSuperSet:pairs[peerIndex].LeftColumnToRowBitSet.RowNumbers.IsSuperSet(
											pairs[pairIndex].LeftColumnToRowBitSet.RowNumbers,
										),
										LeftColumnRowsIsSubSet:pairs[pairIndex].LeftColumnToRowBitSet.RowNumbers.IsSuperSet(
											pairs[peerIndex].LeftColumnToRowBitSet.RowNumbers,
										),
										RightColumnRowsIsSuperSet:pairs[peerIndex].RightColumnToRowBitSet.RowNumbers.IsSuperSet(
											pairs[pairIndex].RightColumnToRowBitSet.RowNumbers,
										),
										RightColumnRowsIsSubSet:pairs[pairIndex].RightColumnToRowBitSet.RowNumbers.IsSuperSet(
											pairs[peerIndex].RightColumnToRowBitSet.RowNumbers,
										),
									}

									pairs[peerIndex].Peers[pairs[pairIndex]] = &PeerInfo{
										LeftColumnRowsIntersection: leftIntersectionResult,
										LeftColumnRowsIntersectionCardinality: leftIntersectionResultCardinality,
										RightColumnRowsIntersection: rightIntersectionResult,
										RightColumnRowsIntersectionCardinality: rightIntersectionResultCardinality,
										LeftColumnRowsIsSuperSet:pairs[pairIndex].LeftColumnToRowBitSet.RowNumbers.IsSuperSet(
											pairs[peerIndex].LeftColumnToRowBitSet.RowNumbers,
										),
										LeftColumnRowsIsSubSet:pairs[peerIndex].LeftColumnToRowBitSet.RowNumbers.IsSuperSet(
											pairs[pairIndex].LeftColumnToRowBitSet.RowNumbers,
										),
										RightColumnRowsIsSuperSet:pairs[pairIndex].RightColumnToRowBitSet.RowNumbers.IsSuperSet(
											pairs[peerIndex].RightColumnToRowBitSet.RowNumbers,
										),
										RightColumnRowsIsSubSet:pairs[peerIndex].RightColumnToRowBitSet.RowNumbers.IsSuperSet(
											pairs[pairIndex].RightColumnToRowBitSet.RowNumbers,
										),
									}
								}
				*/
			}
		//break;

	}
	if false {
		//	jbPeerResult := make(map[uint64][]*JoinBit)
		convBool := func(b bool) string {
			if b {
				return "Y"
			} else {
				return "N"
			}
		}
		for _, pair := range pairs {
			fmt.Printf("%v, TruePositive:%v; Row Cardinality: %v - %v   Peers:\n",
				pair,
				convBool(pair.TruePositive),
				pair.LeftColumnToRowBitSet.RowNumbersCardinality,
				pair.RightColumnToRowBitSet.RowNumbersCardinality,
			)

			for peer, info := range pair.Peers {
				score := 0

				if info.LeftColumnRowsIsSuperSet && info.RightColumnRowsIsSuperSet {
					score += 1
				}
				if info.LeftColumnRowsIsSubSet && info.RightColumnRowsIsSubSet {
					score += 1
				}

				if info.LeftColumnRowsIsSuperSet && info.RightColumnRowsIsSuperSet &&
					info.LeftColumnRowsIsSubSet && info.RightColumnRowsIsSubSet {
					score += 1
				}
				if info.LeftColumnRowsIntersectionCardinality == info.RightColumnRowsIntersectionCardinality {
					score += 1
					if pair.IntersectionCardinality == info.LeftColumnRowsIntersectionCardinality {
						score += 6
					}
				}
				//fmt.Printf("%v|%v|",pair,convBool(pair.TruePositive))
				//fmt.Printf("%v|%v|%v",convBool(peer.TruePositive), score, peer)
				//fmt.Printf("|%v\n", info)
				fmt.Printf("  TP:%v  Score:%v Pair:%v", convBool(peer.TruePositive), score, peer)
				fmt.Printf("; Row Number Info:%v\n", info)

			}
			fmt.Println("*************")
		}
		fmt.Println("----------------------------\n\n")

	}
	done <-true
}

func JoinDataBitSetProcessor(in <- chan *Pair, out chan <-*Pair, bsConf *BitsetServiceConfig) {
	type col2bs struct {
		column *metadata.ColumnInfo
		bitset *sparsebitset.BitSet
	}
	previousData := make([]col2bs ,2)
	loadDataBitSet := func(cb *col2bs , pc *col2bs,category string, wg *sync.WaitGroup ) {
		//if pc.column != cb.column || pc.column ==nil {
			pc.bitset = sparsebitset.New(0)
			pc.column = cb.column
			if file,err := os.Open(bsConf.GetBSPathFileName(cb.column, category)) ; err==nil {
				defer file.Close()
				pc.bitset.ReadFrom(file)
			}else {
				panic(err)
			}
		//}
		wg.Done()
	}

	for pair := range in {
		//log.Printf("loading %v\n",pair)
		for category,_ := range pair.LeftColumnToRowBitSet.Column.HashCategories {
			if _,found := pair.RightColumnToRowBitSet.Column.HashCategories[category]; found {
				left,right := sync.WaitGroup{},sync.WaitGroup{};
				left.Add(1)
				go loadDataBitSet(
					&col2bs{column:pair.LeftColumnToRowBitSet.Column},
					&previousData[0],
					category,
					&left,
				)
				right.Add(1)
				go loadDataBitSet(
					&col2bs{column:pair.RightColumnToRowBitSet.Column},
					&previousData[1],
					category,
					&right,
				)
				left.Wait()
				right.Wait()
				if pair.Intersections == nil{
					pair.Intersections = make(map[string]bool)
				}
				res := previousData[0].bitset.Intersection(previousData[1].bitset)
				card := res.Cardinality()
				if card > 0 {
					pair.SaveDataIntersectionBitSet(category,res)
					pair.Intersections[category] = true
					pair.IntersectionCardinality += card
				}



				//fmt.Println(pair.Intersections[category].Cardinality())
			}
		}

		//log.Printf("loaded\n")
		//log.Printf("Waited\n")
		//log.Printf("Cardinality\n")

		if pair.IntersectionCardinality>0 {

			fmt.Printf(
				//"%v|%v|%.0f%%|%v|%.0f%%|%v|%v\n",
				" %v %v <-[ (%.0f%%) %v (%.0f%%) ]-> %v %v\n",
				previousData[0].bitset.Cardinality(),
				previousData[0].column,
				100*float32(pair.IntersectionCardinality)/float32(previousData[0].bitset.Cardinality()),
				pair.IntersectionCardinality,
				100*float32(pair.IntersectionCardinality) / float32(previousData[1].bitset.Cardinality()),
				previousData[1].column,
				previousData[1].bitset.Cardinality(),
			)
			out<-pair
		}

	}
	close(out)
}


