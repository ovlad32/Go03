package main

import (
	"astra/dataflow"
	"astra/metadata"
	"astra/nullable"
	"context"
	"encoding/json"
	"flag"
	"github.com/goinggo/tracelog"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"
	//	 _ "net/http/pprof"
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

//-workflow_id 57 -metadata_id 331 -cpuprofile cpu.prof.out

var packageName = "main"
var repo *dataflow.Repository

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

var pathToConfigFile = flag.String("configfile", "./config.json", "path to config file")
var argMetadataIds = flag.String("metadata_id", string(-math.MaxInt64), "")
var argWorkflowIds = flag.String("workflow_id", string(-math.MaxInt64), "")

func readConfig() (*dataflow.DumpConfigType, error) {
	funcName := "readConfig"
	if _, err := os.Stat(*pathToConfigFile); os.IsNotExist(err) {
		tracelog.Error(err, funcName, "Specify correct path to config.json")
		return nil, err
	}

	conf, err := os.Open(*pathToConfigFile)
	if err != nil {
		tracelog.Errorf(err, funcName, "Opening config file %v", *pathToConfigFile)
		return nil, err
	}
	jd := json.NewDecoder(conf)
	var result dataflow.DumpConfigType
	err = jd.Decode(&result)
	if err != nil {
		tracelog.Errorf(err, funcName, "Decoding config file %v", *pathToConfigFile)
		return nil, err
	}
	return &result, nil
}

func f1() {
	i := 0
	start := time.Now()
	file, err := os.Open("F:/home/data.253.4/data/100020/86/ORCL.CRA.LIABILITIES.dat")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	bfile := bufio.NewReaderSize(file, 4096)
	zfile, err := gzip.NewReader(bfile)
	if err != nil {
		panic(err)
	}
	defer zfile.Close()
	buf := bufio.NewReaderSize(zfile, 4096*5)
	for {
		b, err := buf.ReadSlice(byte(10))
		if err == nil {
			s := string(bytes.Split(b, []byte{31})[0])
			f, err := strconv.ParseFloat(s, 64)
			if err == nil {
				bi := math.Float64bits(f)
				frac, exp := math.Frexp(f)
				fmt.Printf("%v, %v  %v ^ %v\n", f, bi, frac, math.Pow(2, float64(exp)))
			}

			i += len(s)

		} else if err == io.EOF {
			break
		} else {
			if err != nil {
				panic(err)
			}
		}
	}
	fmt.Println(i)
	tracelog.Info(packageName, "f1", "Elapsed time: %v", time.Since(start))
}

func main() {
	funcName := "main"
	tracelog.Start(tracelog.LevelInfo)
	/*
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()*/

	for i := 1; i <= 10; i++ {
		fmt.Printf("%v, %v, %v, %v \n", i,
			math.Float64bits(float64(i-1)),
			math.Float64bits(float64(i)),
			math.Float64bits(float64(i-1))-math.Float64bits(float64(i)),
		)
		i1, i2 := math.Modf(32 + float64(i)*1000000000000)
		fmt.Printf("%v, [%v, %v] \n", i, i1, i2)

	}
	return
	f1()
	return
	flag.Parse()

	conf, err := readConfig()
	if err != nil {
		os.Exit(1)
	}
	conf.HashValueLength = 8
	if len(conf.LogBaseFile) > 0 {
		tracelog.StartFile(tracelog.LogLevel(), conf.LogBaseFile, conf.LogBaseFileKeepDay)
		defer tracelog.Stop()
	}

	metadataIds := make(map[int64]bool)
	workflowIds := make(map[int64]bool)

	if *argMetadataIds != string(-math.MaxInt64) {
		values := strings.Split(*argMetadataIds, ",")
		for _, value := range values {
			if value != "" {
				if iValue, cnvErr := strconv.ParseInt(value, 10, 64); cnvErr != nil {
					tracelog.Errorf(err, packageName, funcName, "Cannot convert %v to integer ", value)
				} else {
					metadataIds[iValue] = true
				}
			}
		}
	}

	if *argWorkflowIds != string(-math.MaxInt64) {
		values := strings.Split(*argWorkflowIds, ",")
		for _, value := range values {
			if value != "" {
				if iValue, cnvErr := strconv.ParseInt(value, 10, 64); cnvErr != nil {
					tracelog.Errorf(err, packageName, funcName, "Cannot convert %v to integer ", value)
				} else {
					workflowIds[iValue] = true
				}
			}
		}
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}

	dr := dataflow.DataReaderType{
		Config: conf,
	}

	tracelog.Started(packageName, funcName)
	astraRepo, err := metadata.ConnectToAstraDB(
		&metadata.RepositoryConfig{
			Login:        conf.AstraH2Login,
			DatabaseName: conf.AstraH2Database,
			Host:         conf.AstraH2Host,
			Password:     conf.AstraH2Password,
			Port:         conf.AstraH2Port,
		},
	)
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		os.Exit(2)
	}
	repo = &dataflow.Repository{Repository: astraRepo}
	repo.CreateDataCategoryTable()

	tracelog.Started(packageName, funcName)
	start := time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())

	for id, _ := range workflowIds {
		metadataId1, metadataId2, err := repo.MetadataByWorkflowId(nullable.NewNullInt64(id))
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}
		if metadataId1.Valid() {
			metadataIds[metadataId1.Value()] = true
		}
		if metadataId2.Valid() {
			metadataIds[metadataId2.Value()] = true
		}
	}
	tablesToProcess := make([]*dataflow.TableInfoType, 0, 100)

	for id, _ := range metadataIds {
		meta, err := repo.MetadataById(nullable.NewNullInt64(id))
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}

		tables, err := repo.TableInfoByMetadata(meta)
		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}

		for _, table := range tables {
			tablesToProcess = append(tablesToProcess, dataflow.ExpandFromMetadataTable(table))
		}
	}

	var processTableChan chan *dataflow.TableInfoType

	processTable := func(runContext context.Context) (err error) {
		funcName := "processTable"
		tracelog.Started(packageName, funcName)
	outer:
		for {
			select {
			case <-runContext.Done():
				break outer
			case inTable, open := <-processTableChan:
				if !open && inTable == nil {
					break outer
				}
				tracelog.Info(packageName, funcName, "Start processing table %v", inTable)
				var colChans1 []chan *dataflow.ColumnDataType
				colChans1, ec1 := dr.ReadSource(
					runContext,
					inTable,
				)

				ec3 := dr.StoreByDataCategory(
					runContext,
					colChans1,
					dr.Config.CategoryWorkersPerTable,
				)

				go func() {
					select {
					case <-runContext.Done():
					case err, open = <-ec1:
						if err != nil {
							ec3 <- err
						}
					}
				}()

				select {
				case <-runContext.Done():
				case err, open = <-ec3:
					if err != nil {
						tracelog.Error(err, packageName, funcName)
						return err
					}
				}

				for _, col := range inTable.Columns {
					err = col.CloseStorage(runContext)
					if err != nil {
						tracelog.Error(err, packageName, funcName)
						break
					}

					err = repo.SaveColumnCategories(col)
					if err != nil {
						tracelog.Error(err, packageName, funcName)
						break
					}
				}
				if err == nil {
					tracelog.Info(packageName, funcName, "Table processing %v has been done", inTable)
				}

			}
		}
		tracelog.Completed(packageName, funcName)
		return
	}

	var wg sync.WaitGroup
	if len(tablesToProcess) > 0 {
		processTableChan = make(chan *dataflow.TableInfoType, dr.Config.TableWorkers)
		processTableContext, processTableContextCancelFunc := context.WithCancel(context.Background())
		for index := 0; index < dr.Config.TableWorkers; index++ {
			wg.Add(1)
			go func() {
				err = processTable(processTableContext)
				wg.Done()
				if err != nil {
					//tracelog.Info(packageName,funcName,"Сancel сontext called ")
					processTableContextCancelFunc()
				}
				return
			}()
		}
		for _, table := range tablesToProcess {
			processTableChan <- table
		}
		close(processTableChan)
		wg.Wait()
		tracelog.Info(packageName, funcName, "All tables processed")
		dr.CloseStores()
	}
	tracelog.Info(packageName, funcName, "Elapsed time: %v", time.Since(start))
	tracelog.Completed(packageName, funcName)
}
