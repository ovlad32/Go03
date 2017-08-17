package dataflow

import (
	"encoding/binary"
	"github.com/goinggo/tracelog"
	"io"
)


type RowDataType [][]byte;


func(rowData RowDataType) WriteToBinaryDump(writer io.Writer) (offset uint64,err error) {
	funcName := "RowDataType.WriteToBinaryDump"
	tracelog.Started(packageName,funcName)
	offset = 0
	columnCount := len(rowData)
	// persisting count of columns per a line
	if writer != nil {
		binary.Write(writer, binary.LittleEndian, uint16(columnCount)) //
	}
	// 2 bytes of columnCount
	offset = 2
	for colNumber, colData := range rowData {
		colDataLength := len(colData)
		if colDataLength > 0xFFFF {
			colData = colData[:int(0xFFFFF)]
		}

		if writer != nil {
			err = binary.Write(writer, binary.LittleEndian, uint16(colDataLength))
			if err != nil {
				tracelog.Errorf(err, packageName, funcName, "Error while writing column length for column %v", colNumber)
				return 0, err
			}
		}
		offset = offset + 2

		if writer != nil {
			_, err = writer.Write(colData)
			if err != nil {
				tracelog.Errorf(err, packageName, funcName, "Error while writing column data for column %v", colNumber)
				return 0, err
			}
		}
		offset = offset + uint64(colDataLength)
	}
	return offset, nil
}
