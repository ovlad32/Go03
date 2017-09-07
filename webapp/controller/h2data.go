package controller

import (
	"../../metadata"
	"net/http"
	"encoding/json"
	"astra/dataflow"
)

func GetDC(w http.ResponseWriter, r *http.Request){
	result,err := metadata.H2.DatabaseConfigAll()
	if err != nil {
		panic(err)
	}
	json.NewEncoder(w).Encode(result)
}


func TableBuildBitset(w http.ResponseWriter, r *http.Request) {
	dr, err := dataflow.NewInstance()

	dr.BuildHashBitset()



}