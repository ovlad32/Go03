package metadata

import (
	"fmt"
	"github.com/goinggo/tracelog"
	"errors"
	"astra/nullable"
	"astra/dataflow"
	"bufio"
	"compress/gzip"
	"io"
	"bytes"
	"os"
	"context"
)

type TableDumpConfig struct {
	Path           string
	GZip           bool
	FieldSeparator byte
	LineSeparator  byte
	BufferSize     int
}

func defaultTableDumpConfig () (*TableDumpConfig){
	return &TableDumpConfig{
		Path:"./",
		GZip:true,
		FieldSeparator:0x1F,
		LineSeparator:0x0A,
		BufferSize:4096,
	}
}

type TableInfoType struct {
	Id            nullable.NullInt64  `json:"tale-id"`
	MetadataId    nullable.NullInt64  `json:"metadata-id"`
	DatabaseName  nullable.NullString `json:"database-name"`
	SchemaName    nullable.NullString `json:"schema-name"`
	TableName     nullable.NullString `json:"table-name"`
	RowCount      nullable.NullInt64  `json:"row-count"`
	Dumped        nullable.NullString `json:"data-dumped"`
	Indexed       nullable.NullString `json:"data-indexed"`
	PathToFile    nullable.NullString `json:"path-to-file"`
	PathToDataDir nullable.NullString `json:"path-to-data-dir"`
	Metadata      *MetadataType
	Columns       []*ColumnInfoType `json:"columns"`
}


func (t TableInfoType) СheckId() error {
	var funcName = "TableInfoType.СheckId"
	tracelog.Started(packageName, funcName)

	if !t.Id.Valid() {
		err := errors.New("Table Id has not been initialized!")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	tracelog.Completed(packageName, funcName)
	return nil
}

func (t TableInfoType) СheckTableName() (err error) {
	var funcName = "TableInfoType.СheckTableName"
	tracelog.Started(packageName, funcName)

	if !t.TableName.Valid() {
		err := errors.New("Table name has not been initialized!")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	if t.TableName.Value() == "" {
		err := errors.New("Table name is empty!")
		tracelog.Error(err, packageName, funcName)
		return err
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (t TableInfoType) CheckMetadata() (err error)  {
	var funcName = "TableInfoType.CheckMetadata"
	tracelog.Started(packageName, funcName)
	if t.Metadata == nil {
		err := errors.New("Metadata reference has not been initialized!")
		tracelog.Error(err, packageName, funcName)
		return err
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (t TableInfoType) СheckColumnListExistence() (err error)  {
	var funcName = "TableInfoType.СheckColumnListExistence"
	tracelog.Started(packageName, funcName)

	var errorMessage string
	if t.Columns == nil {
		errorMessage = "Column list has not been inititalized!"
	} else if len(t.Columns) == 0 {
		errorMessage = "Column list is empty!"
	}
	if len(errorMessage) > 0 {
		if err := t.СheckId(); err != nil {
			err = fmt.Errorf("Table %v. %v", t, errorMessage)
			tracelog.Error(err, packageName, funcName)
			return err
		} else {
			err = errors.New(errorMessage)
			tracelog.Error(err, packageName, funcName)
			return err
		}
	}
	tracelog.Completed(packageName, funcName)
	return
}


func (t TableInfoType) СheckDumpFileName() (err error) {
	var funcName = "TableInfoType.СheckDumpFileName"
	tracelog.Started(packageName, funcName)

	if !t.PathToFile.Valid() {
		err:= errors.New("Dump filename has not been initialized!")
		tracelog.Error(err,packageName,funcName)
		return err
	}
	if t.PathToFile.Value() == "" {
		err:= errors.New("Dump filename is empty!")
		tracelog.Error(err,packageName,funcName)
		return err
	}
	tracelog.Completed(packageName, funcName)
	return
}

func (t TableInfoType) String() string {
	var funcName = "TableInfoType.String"
	tracelog.Started(packageName, funcName)

	var result = ""

	if t.SchemaName.Value() != "" {
		result = result +t.SchemaName.Value() + "."
	}

	if t.TableName.Value() != "" {
		result = result + t.TableName.Value()
	}
	if false && t.Id.Valid() {
		result = result + " (Id=" + fmt.Sprintf("%v", t.Id.Value()) + ")"
	}
	tracelog.Completed(packageName, funcName)
	return result
}


func (t TableInfoType) GoString() string {
	var funcName = "TableInfoType.GoString"
	tracelog.Started(packageName, funcName)

	var result = "TableInfo["

	if t.SchemaName.Value() != "" {
		result = result + "SchemaName=" + t.SchemaName.Value() + "; "
	}

	if t.TableName.Value() != "" {
		result = result + "TableName=" + t.TableName.Value() + "; "
	}
	if t.Id.Valid() {
		result = result + "Id=" + fmt.Sprintf("%v", t.Id.Value()) + "; "
	}
	result = result + "]"
	tracelog.Completed(packageName, funcName)
	return result
}


func (t TableInfoType) ReadAstraDump(ctx context.Context, handler func(*ColumnInfoType,[][]byte),cfg *TableDumpConfig) (uint64,error){
	funcName := "TableInfoType.ReadAstraDump"
	var x0D = []byte{0x0D}

	gzFile, err := os.Open(cfg.Path + t.PathToFile.Value())

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "for table %v", t)
		return
	}
	defer gzFile.Close()
	if cfg == nil {
		cfg = defaultTableDumpConfig();
	}
	bf := bufio.NewReaderSize(gzFile, cfg.BufferSize)
	file, err := gzip.NewReader(bf)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "for table %v", t)
		return
	}
	defer file.Close()
	bufferedFile := bufio.NewReaderSize(file, cfg.BufferSize)

	lineNumber := uint64(0)
	lineOffset := uint64(0)
	for {

		select {
		case <-ctx.Done():
			return lineNumber,nil
		default:
		line, err := bufferedFile.ReadSlice(cfg.LineSeparator)
		if err == io.EOF {
			return lineNumber,nil
		} else if err != nil {
			return lineNumber,err
		}

		lineLength := len(line)

		if line[lineLength-1] == cfg.LineSeparator {
			line = line[:lineLength-1]
		}
		if line[lineLength-2] == x0D[0]{
			line = line[:lineLength-2]
		}


	image := make([]byte,len(line))

	copy(image,line)



metadataColumnCount := len(table.Columns)

lineColumns := bytes.Split(image, []byte{dr.Config.AstraFieldSeparator})
lineColumnCount := len(lineColumns)

if metadataColumnCount != lineColumnCount {
err = fmt.Errorf("Number of column mismatch in line %v. Expected #%v; Actual #%v",
lineNumber,
metadataColumnCount,
lineColumnCount,
)
return err
}

lineNumber++
if lineNumber == 1 && dr.Config.BuildBinaryDump {
closeContext, closeContextCancelFunc := context.WithCancel(context.Background())
err = table.OpenBinaryDump(closeContext, dr.Config.BinaryDumpPath, os.O_CREATE)
if err != nil {
return err
}
defer closeContextCancelFunc()
}

if dr.Config.EmitRawData {
wg.Add(len(lineColumns))
for columnIndex := range  lineColumns{
go splitToColumns(lineNumber, lineOffset, columnIndex, &lineColumns[columnIndex])
}
wg.Wait()
}

lineOffset = lineOffset + uint64(writeToTank(lineColumns))
//	}
}