package controller

import (
	"../../metadata"
	"encoding/json"
	"net/http"
)

func GetDC(w http.ResponseWriter, r *http.Request) {
	result, err := metadata.H2.DatabaseConfigAll()
	if err != nil {
		panic(err)
	}
	json.NewEncoder(w).Encode(result)
}
