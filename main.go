package main

import (
	"fmt"
	"net/http"

	"github.com/urfave/negroni"
	"github.com/gorilla/mux"
)

func main() {
	//mux := http.NewServeMux()
//	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
//		fmt.Fprintf(w, "Welcome to the home page!")
	//})
	router := mux.NewRouter()
	router.HandleFunc("/databaseConfigurations/",databaseConfigurations)

	n := negroni.Classic() // Includes some default middlewares
	n.Use(negroni.Wrap(router))
	http.ListenAndServe(":3000", n)
}
func databaseConfigurations(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Welcome to the home page!")
}