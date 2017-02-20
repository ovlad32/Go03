package main

import (
	"astra/dataflow"
	"astra/metadata"
	"astra/nullable"
	"context"
	"fmt"
	"github.com/goinggo/tracelog"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
	"flag"
	"runtime/trace"
	"runtime/pprof"
)

var packageName = "main"
var recreate bool = true
var isTrace bool = true
var repo *metadata.Repository

func init() {
	var err error
	funcName := "init"
	tracelog.Start(tracelog.LevelInfo)
	tracelog.Started(packageName, funcName)
	repo, err = metadata.ConnectToAstraDB(
		&metadata.RepositoryConfig{
			Login:        "edm",
			DatabaseName: "edm",
			Host:         "localhost",
			Password:     "edmedm",
			Port:         "5435",
		},
	)
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		panic(err)
	}

	boltDbName := "./hashStorage.bolt.db"
	if recreate {
		os.Remove(boltDbName)
	}
	/*var err error
	metadata.HashStorage, err = bolt.Open(boltDbName,0600,nil)
	if err != nil {
		panic(err)
	}
	*/
	tracelog.Completed(packageName, funcName)
}
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	funcName := "main"
	flag.Parse()
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

	var trfile *os.File
	if !isTrace {
		trfile, _ = os.Create("./trace.out")
		trace.Start(trfile)
	}
	tracelog.Started(packageName, funcName)
	start := time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())
	//

	/*router := mux.NewRouter()
	router.HandleFunc("/databaseConfigurations/",controller.GetDC)

	n := negroni.Classic() // Includes some default middlewares
	n.Use(negroni.Wrap(router))
	http.ListenAndServe(":3000", n)*/

	dr := dataflow.DataReaderType{
		Config: &dataflow.DumpConfigType{
			BasePath:        "C:/home/data.151/",
			TankPath:        "G:/BINDATA/",
			StoragePath:     "G:/BINDATA/",
			InputBufferSize: 5 * 1024,
			GZipped:         true,
			FieldSeparator:  31,
			LineSeparator:   10,
			HashValueLength: 8,
		},
	}

	metadataId1, metadataId2, err := repo.MetadataByWorkflowId(nullable.NewNullInt64(int64(67)))

	if err != nil {
		panic(err)
	}

	mtd1, err := repo.MetadataById(metadataId1)
	if err != nil {
		panic(err)
	}

	mtd2, err := repo.MetadataById(metadataId2)
	if err != nil {
		panic(err)
	}

	tables, err := repo.TableInfoByMetadata(mtd1)
	if err != nil {
		panic(err)
	}
	tables2, err := repo.TableInfoByMetadata(mtd2)
	if err != nil {
		panic(err)
	}
	_ = tables2
	var wg sync.WaitGroup

	for _, table := range tables {
		wg.Add(1)
		go func(inTable *dataflow.TableInfoType) {
			fmt.Print(inTable)
			colDataPool := make(chan *dataflow.ColumnDataType,len(inTable.Columns)*100)

			//var drainChan chan *dataflow.ColumnDataType
			var colChan1 chan *dataflow.ColumnDataType
			//var colChan1 chan *dataflow.ColumnDataType
			//var colChan2 chan *dataflow.ColumnDataType

			var ctxf context.CancelFunc
			ctx, ctxf := context.WithCancel(context.Background())

			colChan1, ec1 := dr.ReadSource(
				ctx,
				inTable,
				colDataPool,
			)

			colChan2, ec3 := dr.StoreByDataCategory(
				ctx,
				colChan1,
				len(inTable.Columns),
			)



		outer:
			for {
				select {
				case value,closed := <-colChan2:
					if !closed {
						break outer
					}
					if cap(value.RawData)<1024 {
						select {
						case colDataPool <- value:
						default:
						}

					} else {
						fmt.Println("!")
					}

					_=value

				case err := <-ec1:
					if err != nil {
						fmt.Println(err.Error())
						ctxf()
					}
				case err := <-ec3:
					if err != nil {
						fmt.Println(err.Error())
						ctxf()
					}
				}
			}
			for _,col := range inTable.Columns{
				fmt.Println(col.ColumnName.String())
				fmt.Println("----------------")
				for k,_ := range(col.Categories) {
					fmt.Printf("%v,",k)
				}
				fmt.Println("\n----------------\n")
				err = col.CloseStorage()
			}
			wg.Done()
			fmt.Println(". Done")

		}(dataflow.ExpandFromMetadataTable(table))

		//break;
	}
	wg.Wait()

	/*var err error
	da.Repo, err = cayley.NewGraph("bolt","./dfd.cayley.db",nil)
	if err != nil{
		panic(err)
	}*/

	if recreate {
		//		da.LoadStorage(jsnull.NewNullInt64(int64(67)))
	}
	//fetchPairs(da);
	//metadata.ReportHashStorageContents()
	//da.MakeTablePairs(nil,nil)
	log.Printf("%v", time.Since(start))
	if trfile != nil {
		trace.Stop()
	}
	tracelog.Completed(packageName, funcName)
}

/*func fetchPairs(da metadata.DataAccessType) {


	da.MakeColumnPairs(jsnull.NewNullInt64(int64(67)));
}*/
