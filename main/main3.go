package main

import (
	"math"
	"fmt"
	"runtime"
	"time"
)

type Domino struct {
	Spot[2]int;
}


func main() {
	start := time.Now()
	fmt.Println(time.Since(start))
}


