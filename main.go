package main

import (
	"astra/metadata"
	"github.com/goinggo/tracelog"
	"os"
	"time"
	"runtime"
	"astra/dataflow"
	"astra/nullable"
	"fmt"
	"log"
	"context"
)


var packageName = "main"
var recreate bool = true;
var repo *metadata.Repository
func init() {
	var err error
	funcName := "init"
	tracelog.Start(tracelog.LevelInfo)
	tracelog.Started(packageName,funcName)
	repo, err = metadata.ConnectToAstraDB(
		&metadata.RepositoryConfig{
			Login:"edm",
			DatabaseName:"edm",
			Host:"localhost",
			Password:"edmedm",
			Port:"5435",
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
	tracelog.Completed(packageName,funcName)
}


func main() {
	funcName := "main"
	tracelog.Started(packageName,funcName)
	start := time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())
	//

	/*router := mux.NewRouter()
	router.HandleFunc("/databaseConfigurations/",controller.GetDC)

	n := negroni.Classic() // Includes some default middlewares
	n.Use(negroni.Wrap(router))
	http.ListenAndServe(":3000", n)*/

	dr := dataflow.DataReaderType{
		Config:&dataflow.DumpConfigType{
			BasePath: "C:/home/data.151/",
			TankPath: "./BINDATA/",
			InputBufferSize:5 * 1024,
			GZipped:true,
			FieldSeparator:31,
			LineSeparator:10,
		},
	}


	metadataId1,metadataId2, err := repo.MetadataByWorkflowId(nullable.NewNullInt64(int64(67)))

	if err != nil {
		panic(err)
	}

	mtd1,err := repo.MetadataById(metadataId1)
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

	var chin chan *dataflow.RowDataType;
	var cher chan error
	for _,table := range tables {
		var ctxf context.CancelFunc
		ctx,ctxf := context.WithCancel(context.Background())
		chin,cher = dr.ReadSource(
			ctx,
			&dataflow.TableInfoType{
				TableInfoType:table,
			},
		)
		outer:
		for {
			select {
				case r := <-chin:
				_ = r
				//	fmt.Print(r.LineNumber)
				case err := <-cher:
				if err != nil{
					fmt.Println(err.Error())
				}
				break outer;
			}
		}
		ctxf();
	}

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
	log.Printf("%v",time.Since(start))
	tracelog.Completed(packageName,funcName)
}

/*func fetchPairs(da metadata.DataAccessType) {


	da.MakeColumnPairs(jsnull.NewNullInt64(int64(67)));
}*/