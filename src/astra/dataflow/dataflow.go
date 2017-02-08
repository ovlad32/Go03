package dataflow

import (
	"astra/metadata"
	"os"
)

var packageName = "astra/dataflow"

type TableInfoType struct {
	*metadata.TableInfoType
	Tank os.File
}
