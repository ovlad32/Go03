package main

import (
	"fmt"
	"math"
	"runtime"
	"time"
)

type Domino struct {
	Spot [2]int
}

func main() {
	start := time.Now()
	fmt.Println(time.Since(start))
}
