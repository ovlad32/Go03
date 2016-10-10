package main

import (
	//"net/http"
	"github.com/boltdb/bolt"
	//"github.com/urfave/negroni"
	//"github.com/gorilla/mux"
	"./metadata"
//	"./webapp/controller"
	scm "./scm"
	jsnull "./jsnull"
	"database/sql"
	"fmt"
)

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
	//os.Remove(boltDbName)

	var err error
	metadata.HashStorage, err = bolt.Open(boltDbName,0600,nil)
	if err != nil {
		panic(err)
	}

}
func main() {
	/*router := mux.NewRouter()
	router.HandleFunc("/databaseConfigurations/",controller.GetDC)

	n := negroni.Classic() // Includes some default middlewares
	n.Use(negroni.Wrap(router))
	http.ListenAndServe(":3000", n)*/

	table := scm.NewChannel();
	TableData := scm.NewChannel();
	TableCalculatedData := scm.NewChannel();
	done := scm.NewChannel();
	da := metadata.DataAccessType{
		DumpConfiguration: metadata.DumpConfigurationType{
			DumpBasePath: "C:/home/data.225/",
			InputBufferSize:5*1024,
			IsZipped:true,
			FieldSeparator:124,
			LineSeparator:10,
		},
	}


	go da.ReadTableDumpData(table,TableData);
	go da.CollectMinMaxStats(TableData,TableCalculatedData)
	go da.SplitDataToBuckets(TableCalculatedData,done)

	///for _,id := range []int{261,262,263,264,265,266,267,268,269,270,271,272,273} {
	for _,id := range []int{274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291}{
		ti,err := metadata.H2.TableInfoById(jsnull.NullInt64{sql.NullInt64 {Int64:int64(id),Valid:true}})
		fmt.Println(ti)
		if err != nil{
			panic(err)
		}
		table <- scm.NewMessage().Put(ti)

	}


	close(table)

	<- done


}

