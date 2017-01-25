package tabledriver

import (
	"io"
	"bufio"
	"bytes"
	"encoding/binary"
)

type chunkBytesType []byte
type TableDriverType struct {
	PathToFile string
	Filename string
	writer io.Writer
}


func (td *TableDriverType) SaveRow(data []chunkBytesType) {

	chunkCount := uint16 (len(data));

	binary.Write(td.writer,binary.LittleEndian,chunkCount);
	binary.Write(td.writer,)

	for _,chunk := range(data) {
		if chunk == nil {

		}
	}

	td.writer.Write()
}





