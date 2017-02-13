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
_	"runtime/trace"
	"runtime/trace"
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

func main() {
	funcName := "main"
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
			TankPath:        "./BINDATA/",
			InputBufferSize: 5 * 1024,
			GZipped:         true,
			FieldSeparator:  31,
			LineSeparator:   10,
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
	//var RowData chan *dataflow.ColumnDataType
	for _, table := range tables {
		wg.Add(1)
		func(inTable *dataflow.TableInfoType) {
			fmt.Print(inTable)
			//var drainChan chan *dataflow.ColumnDataType
			var rowChan chan *dataflow.RowDataType
			//var colChan1 chan *dataflow.ColumnDataType
			//var colChan2 chan *dataflow.ColumnDataType

			var ctxf context.CancelFunc
			ctx, ctxf := context.WithCancel(context.Background())
			defer ctxf()

			rowChan, ec1 := dr.ReadSource(
			ctx,
				inTable,
			)
			colChan1, ec2 := dr.SplitToColumns(
				ctx,
				rowChan,
			)
			colChan2, ec3 := dr.StoreByDataCategory(
				ctx,
				colChan1,
				len(table.Columns),
			)


		outer:
			for {
				select {
				case value,closed := <-colChan2:
					if !closed {
						break outer
					}
					_ = value

				case err := <-ec1:
					if err != nil {
						fmt.Println(err.Error())
					}
				case err:= <-ec2:
					if err != nil {
						fmt.Println(err.Error())
					}
				case err := <-ec3:
					if err != nil {
						fmt.Println(err.Error())
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
