package main

import (
	//"net/http"
	"github.com/boltdb/bolt"
	//"github.com/urfave/negroni"
	//"github.com/gorilla/mux"
	"./metadata"
	scm "./scm"
	utils "./utils"
	jsnull "./jsnull"
	"os"
	"runtime"
	"time"
	"log"

	"github.com/goinggo/tracelog"
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
	var err error
	metadata.HashStorage, err = bolt.Open(boltDbName,0600,nil)
	if err != nil {
		panic(err)
	}

	tracelog.Start(tracelog.LevelInfo)
}
func main() {
	start := time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())
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
		TransactionCountLimit:500*1000,
		ColumnBucketsCache:utils.New(50),

	}
	if recreate {
		loadStorage(da)
	}
	//fetchPairs(da);
	//metadata.ReportHashStorageContents()
	log.Printf("%v",time.Since(start))
}

func loadStorage(da metadata.DataAccessType) {
	table := scm.NewChannel();
	TableData := scm.NewChannelSize(1024*100);
	TableCalculatedData := scm.NewChannelSize(1024*100);
	done := scm.NewChannel();

	go da.ReadTableDumpData(table, TableData);
	go da.CollectMinMaxStats(TableData, TableCalculatedData)
	go da.SplitDataToBuckets(TableCalculatedData, done)


	err := metadata.H2.CreateDataCategoryTable()
	if err != nil {
		panic(err)
	}

	mtd1,err := metadata.H2.MetadataById(jsnull.NewNullInt64(10))
	if err != nil {
		panic(err)
	}

	mtd2,err := metadata.H2.MetadataById(jsnull.NewNullInt64(11))
	if err != nil {
		panic(err)
	}

	tables,err := metadata.H2.TableInfoByMetadata(mtd1)
	for _,tableInfo := range tables {
		//if tableInfo.Id.Value() == int64(268) {
			table <- scm.NewMessage().Put(tableInfo)
		//}
	}
	tables,err = metadata.H2.TableInfoByMetadata(mtd2)
	for _,tableInfo := range tables {
		//if tableInfo.Id.Value() == int64(291) {
			table <- scm.NewMessage().Put(tableInfo)
		//}
	}

	close(table)

	<-done
	for _,t := range tables {
		for _,c := range t.Columns {
			err = metadata.H2.SaveColumnCategory(c)
			if err != nil {
				panic(err)
			}
		}
	}
}
func fetchPairs(da metadata.DataAccessType) {
	mtd := scm.NewChannel();
	pairs := scm.NewChannel();
	done := scm.NewChannel();

	go da.MakePairs(mtd, pairs);
	go da.BuildDataBitsets(pairs,done)

	mtd1,err:= metadata.H2.MetadataById(jsnull.NewNullInt64(10))
	if err !=nil {
		panic(err)
	}
	mtd2,err:= metadata.H2.MetadataById(jsnull.NewNullInt64(11))
	if err !=nil {
		panic(err)
	}
	mtd <-scm.NewMessageSize(2).Put("2MD").PutN(0,mtd1).PutN(1,mtd2)
	close(mtd)
	<-done
}