package controller

import (
	"../../metadata"
	"net/http"
	"encoding/json"
)

func GetDC(w http.ResponseWriter, r *http.Request){
	result,err := metadata.H2.DatabaseConfigAll()
	if err != nil {
		panic(err)
	}
	json.NewEncoder(w).Encode(result)
}


