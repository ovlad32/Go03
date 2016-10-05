package main

import (
	"net/http"

	"github.com/urfave/negroni"
	"github.com/gorilla/mux"
	"./metadata"
	"./webapp/controller"
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


}
func main() {
	router := mux.NewRouter()
	router.HandleFunc("/databaseConfigurations/",controller.GetDC)

	n := negroni.Classic() // Includes some default middlewares
	n.Use(negroni.Wrap(router))
	http.ListenAndServe(":3000", n)
}

