package dataflow

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"github.com/goinggo/tracelog"
	"io"
	"os"
	"strings"
)

type DumpReaderConfigType struct {
	PathToDumpDirectory string
	TableName           string
	TableDumpFileName   string
	TableColumnCount    int
	GZip                bool
	ColumnSeparator     byte
	LineSeparator       byte
	BufferSize          int
	MoveToByte          struct {
		Position    uint64
		FirstLineAs uint64
	}
	MoveToLine uint64
}
type DumpReaderActionType int

const (
	DumpReaderActionContinue DumpReaderActionType = 0
	DumpReaderActionAbort    DumpReaderActionType = 1
)

type DumpReaderResultType int

const (
	DumpReaderResultOk                     DumpReaderResultType = 0
	DumpReaderResultError                  DumpReaderResultType = 1
	DumpReaderResultAbortedByContext       DumpReaderResultType = 2
	DumpReaderResultAbortedByRowProcessing DumpReaderResultType = 3
)

func DefaultDumpReaderConfig() *DumpReaderConfigType {
	return &DumpReaderConfigType{
		GZip:            true,
		ColumnSeparator: 0x1F,
		LineSeparator:   0x0A,
		BufferSize:      4096,
	}
}
func (c *DumpReaderConfigType) PopulateWithTableInfo(table *TableInfoType) {
	c.TableName = table.String()
	c.TableColumnCount = len(table.Columns)
	c.TableDumpFileName = table.PathToFile.Value()
}

func ReadAstraDump(
	ctx context.Context,
	cfg *DumpReaderConfigType,
	rowProcessingFunc func(
		context.Context,
		uint64,
		uint64,
		[][]byte,
	) (DumpReaderActionType, error),
) (result DumpReaderResultType, lineNumber uint64, err error) {
	funcName := "DataReaderType.readAstraDump"
	var x0D = []byte{0x0D}
	lineNumber = uint64(0)
	dataPosition := uint64(0)
	fullPathToFile := fmt.Sprintf(
		"%v%c%v",
		cfg.PathToDumpDirectory,
		os.PathSeparator,
		cfg.TableDumpFileName,
	)
	suffixErrorMessage := func() (message string) {
		return fmt.Sprintf(
			"dump file %v of table %v",
			fullPathToFile,
			cfg.TableName,
		)
	}

	suffixErrorMessageWithLineNumber := func() (message string) {
		return fmt.Sprintf(
			"%v at line %v",
			suffixErrorMessage(),
			lineNumber,
		)
	}

	if strings.TrimSpace(cfg.TableName) == "" {
		err = errors.New("parameter TableName is empty")
		tracelog.Error(err, packageName, funcName)
		return DumpReaderResultError, 0, err
	}

	if rowProcessingFunc == nil {
		err = fmt.Errorf(
			"row processing function must be defined to read dump file of table %v",
			cfg.TableName,
		)
		tracelog.Error(err, packageName, funcName)
		return DumpReaderResultError, 0, err
	}

	if strings.TrimSpace(cfg.PathToDumpDirectory) == "" {
		err = fmt.Errorf(
			"parameter PathToDumpDirectory is empty to read dump file of table %v",
			cfg.TableName,
		)
		tracelog.Error(err, packageName, funcName)
		return DumpReaderResultError, 0, err
	}

	if strings.TrimSpace(cfg.TableDumpFileName) == "" {
		err = fmt.Errorf(
			"parameter TableDumpFileName is empty to read dump file of table %v",
			cfg.TableName,
		)
		tracelog.Error(err, packageName, funcName)
		return DumpReaderResultError, 0, err
	}

	if cfg.TableColumnCount <= 0 {
		err = fmt.Errorf(
			"parameter TableColumnCount is not valid to read dump file of table %v",
			cfg.TableName,
		)
		tracelog.Error(err, packageName, funcName)
		return DumpReaderResultError, 0, err
	}

	if cfg.MoveToLine > 0 && cfg.MoveToByte.Position > 0 {
		err = fmt.Errorf(
			"mixture of mutually exceptional parameters: "+
				"cfg.MoveToLine > %v && cfg.MoveToByte.Position>%v",
			cfg.MoveToLine,
			cfg.MoveToByte.Position,
		)
		err = fmt.Errorf("wrong parameters to process %v: %v",
			suffixErrorMessage(),
			err,
		)
		tracelog.Error(err, packageName, funcName)
		return DumpReaderResultError, 0, err
	}

	gzFile, err := os.Open(fullPathToFile)

	if err != nil {
		err = fmt.Errorf("error while opening %v: %v",
			suffixErrorMessage(),
			err,
		)
		tracelog.Error(err, packageName, funcName)
		return DumpReaderResultError, 0, err
	}
	defer gzFile.Close()

	defaultConfig := DefaultDumpReaderConfig()

	if cfg.BufferSize == 0 {
		cfg.BufferSize = defaultConfig.BufferSize
	}

	if cfg.ColumnSeparator == 0 {
		cfg.ColumnSeparator = defaultConfig.ColumnSeparator
	}

	if cfg.LineSeparator == 0 {
		cfg.LineSeparator = defaultConfig.LineSeparator
	}

	bf := bufio.NewReaderSize(gzFile, cfg.BufferSize)
	file, err := gzip.NewReader(bf)
	if err != nil {
		err = fmt.Errorf("error while opening GZIP stream of %v: %v",
			suffixErrorMessage(),
			err,
		)
		tracelog.Error(err, packageName, funcName)
		return DumpReaderResultError, 0, err
	}
	defer file.Close()

	bufferedFile := bufio.NewReaderSize(file, cfg.BufferSize)

	if cfg.MoveToByte.Position > 0 {
		discarded, err := bufferedFile.Discard(int(cfg.MoveToByte.Position))
		if err != nil {
			err = fmt.Errorf("error while discarding %v to position %v: %v",
				suffixErrorMessage(),
				int(cfg.MoveToByte.Position),
				err,
			)
			return DumpReaderResultError, 0, err
		}

		if uint64(discarded) != cfg.MoveToByte.Position {
			err = fmt.Errorf("expected %v, discarded %v",
				cfg.MoveToByte.Position,
				uint64(discarded),
			)
			err = fmt.Errorf("error while discarding %v: %v",
				suffixErrorMessage(),
				err,
			)
			tracelog.Error(err, packageName, funcName)
			return DumpReaderResultError, 0, err
		}

		lineNumber = cfg.MoveToByte.FirstLineAs
		dataPosition = uint64(discarded)
	}

	for {
		select {
		case <-ctx.Done():
			tracelog.Info(
				packageName,
				funcName,
				"Context.Done signalled while reading %v",
				suffixErrorMessageWithLineNumber(),
			)
			return DumpReaderResultAbortedByContext, lineNumber, nil
		default:
			line, err := bufferedFile.ReadSlice(cfg.LineSeparator)
			if err == io.EOF {
				return DumpReaderResultOk, lineNumber, nil
			} else if err != nil {
				err = fmt.Errorf(
					"error while reading data slice from %v: %v",
					suffixErrorMessageWithLineNumber(),
					err,
				)
				tracelog.Error(err, packageName, funcName)
				return DumpReaderResultError, lineNumber, err
			}

			originalLineLength := len(line)

			if line[originalLineLength-1] == cfg.LineSeparator {
				line = line[:originalLineLength-1]
			}
			if line[originalLineLength-2] == x0D[0] {
				line = line[:originalLineLength-2]
			}

			lineColumns := bytes.Split(line, []byte{cfg.ColumnSeparator})
			lineColumnCount := len(lineColumns)

			if cfg.TableColumnCount != lineColumnCount {
				err = fmt.Errorf("actual #%v, expected #%v",
					lineColumnCount,
					cfg.TableColumnCount,
				)
				err = fmt.Errorf(
					"number of columns mismatch while reading %v: %v",
					suffixErrorMessageWithLineNumber(),
					err,
				)
				return DumpReaderResultError, lineNumber, err
			}

			var rowResult DumpReaderActionType
			if lineNumber >= cfg.MoveToLine {
				rowResult, err = rowProcessingFunc(ctx, lineNumber, dataPosition, lineColumns)
				if err != nil {
					err = fmt.Errorf(
						"error while processing a row of %v : %v",
						suffixErrorMessageWithLineNumber(),
						err,
					)
					tracelog.Error(err, packageName, funcName)
					return DumpReaderResultError, lineNumber, err
				}
				if rowResult == DumpReaderActionContinue {
				} else if rowResult == DumpReaderActionAbort {
					return DumpReaderResultAbortedByRowProcessing, lineNumber, nil
				} else {
					err = fmt.Errorf(
						"unsupported rowProcessingFunc return value %v",
						rowResult,
					)
					err = fmt.Errorf(
						"error while analyzing result of processing a row of %v: %v",
						suffixErrorMessageWithLineNumber(),
						err,
					)
				}

				lineNumber++
				dataPosition += uint64(originalLineLength)
			}

		}
	}

	return DumpReaderResultOk, lineNumber, nil
}
