package main

import (
	//"net/http"
	//"github.com/urfave/negroni"
	//"github.com/gorilla/mux"
	"./metadata"
	utils "./utils"
	jsnull "./jsnull"
	"os"
	"runtime"
	"time"
	"log"

	"github.com/goinggo/tracelog"
	"fmt"
)

var recreate bool = true

func init() {
	metadata.H2 = metadata.H2Type{
		Login:"edm",
		DatabaseName:"edm",
		Host:"localhost",
		Password:"edmedm",
		Port:"5435",
	}
	metadata.H2.InitDb();
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
	tracelog.Start(tracelog.LevelInfo)
}


func main() {
	fmt.Println(os.Getpagesize())
	start := time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())
	//

	/*router := mux.NewRouter()
	router.HandleFunc("/databaseConfigurations/",controller.GetDC)

	n := negroni.Classic() // Includes some default middlewares
	n.Use(negroni.Wrap(router))
	http.ListenAndServe(":3000", n)*/

	da := metadata.DataAccessType{
		DumpConfiguration: metadata.DumpConfigurationType{
			DumpBasePath: "C:/home/data.225/",
			InputBufferSize:5 * 1024,
			IsZipped:true,
			FieldSeparator:124,
			LineSeparator:10,
		},
		SubHashByteLengthThreshold: 6,
		TransactionCountLimit:1000*50,
		ColumnBucketsCache:utils.New(50),

	}
	/*var err error
	da.Repo, err = cayley.NewGraph("bolt","./dfd.cayley.db",nil)
	if err != nil{
		panic(err)
	}*/

/*	if recreate {
		da.LoadStorage()
	}*/
	fetchPairs(da);
	//metadata.ReportHashStorageContents()
	da.MakeTablePairs(nil,nil)
	log.Printf("%v",time.Since(start))
}

func fetchPairs(da metadata.DataAccessType) {

	mtd1,err:= metadata.H2.MetadataById(jsnull.NewNullInt64(10))
	if err !=nil {
		panic(err)
	}
	mtd2,err:= metadata.H2.MetadataById(jsnull.NewNullInt64(11))
	if err !=nil {
		panic(err)
	}
	da.MakeColumnPairs(mtd1,mtd2,"");
}