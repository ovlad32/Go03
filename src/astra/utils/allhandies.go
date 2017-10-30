package utils

import (
	"fmt"
	"github.com/goinggo/tracelog"
	"strconv"
	"strings"
)

var packageName = "utils"

func ParseToInt64Array(separatedValues, separator string) (result []int64, err error) {
	return parseToInt64Array(separatedValues,separator,false)
}
func ParseToInt64UniqueArray(separatedValues, separator string) (result []int64, err error) {
	return parseToInt64Array(separatedValues,separator,true)
}
func parseToInt64Array(separatedValues, separator string, unique bool) (result []int64, err error) {
	funcName := "parseToInt64Array"
	var iMap map[int64] bool

	if unique {
		iMap = make(map[int64] bool)
	} else {
		result = make([]int64, 0, 3)
	}


	for _, value := range strings.Split(separatedValues, separator) {
		if value != "" {
			if iValue, cnvErr := strconv.ParseInt(value, 10, 64); cnvErr != nil {
				err = fmt.Errorf("Conversion separated string %v value to int64: %v", value, cnvErr)
				tracelog.Error(err, packageName, funcName)
				return nil, err
			} else {
				if unique {
					iMap[iValue]=true
				} else {
					result = append(result, iValue)
				}
			}
		}
	}
	if unique {
		result = make([]int64, 0, len(iMap))
		for iValue,_ := range iMap {
			result = append(result, iValue)
		}
	}
	return result, nil
}
