package main

import (
	//"net/http"
	//"github.com/urfave/negroni"
	//"github.com/gorilla/mux"
	"./metadata"
	utils "./utils"
	jsnull "src/jsnull"
	"os"
	"runtime"
	"time"
	"log"

	"github.com/goinggo/tracelog"
	"fmt"
)

var recreate bool = true;

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
			DumpBasePath: "C:/home/data.151/",
			InputBufferSize:5 * 1024,
			IsZipped:true,
			//FieldSeparator:124,
			FieldSeparator:31,
			LineSeparator:10,
		},
		SubHashByteLengthThreshold: 10,
		TransactionCountLimit:1000*50,
		ColumnBucketsCache:utils.New(50),

	}
	/*var err error
	da.Repo, err = cayley.NewGraph("bolt","./dfd.cayley.db",nil)
	if err != nil{
		panic(err)
	}*/

	if recreate {
		da.LoadStorage(jsnull.NewNullInt64(int64(67)))
	}
	//fetchPairs(da);
	//metadata.ReportHashStorageContents()
	//da.MakeTablePairs(nil,nil)
	log.Printf("%v",time.Since(start))
}

func fetchPairs(da metadata.DataAccessType) {


	da.MakeColumnPairs(jsnull.NewNullInt64(int64(67)));
}