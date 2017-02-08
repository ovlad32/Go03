package dataflow

import (
	"astra/metadata"
	"errors"
	"fmt"
	"github.com/goinggo/tracelog"
	"golang.org/x/net/context"
	"os"
	"bufio"
)

var packageName = "astra/dataflow"

type TableInfoType struct {
	*metadata.TableInfoType
	TankWriter *bufio.Writer
	tankFile os.File
}

func (ti *TableInfoType) OpenTank(ctx context.Context, pathToTankDir string, flags int) (err error) {
	funcName := "TableInfoType.OpenTank"
	tracelog.Started(packageName, funcName)

	if pathToTankDir == "" {
		err = errors.New("Given path is empty")
		tracelog.Error(err, packageName, funcName)
		return err
	}

	err = os.MkdirAll(pathToTankDir, 700)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Making directories for path %v", pathToTankDir)
		return err
	}

	pathToTankFile := fmt.Sprintf("%v%v%v.tank",
		pathToTankDir,
		os.PathSeparator,
		ti.Id.String(),
	)

	file , err := os.OpenFile(pathToTankFile, flags, 0666)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Opening file %v", pathToTankFile)
		return err
	}
	ti.TankWriter =  bufio.NewWriter(file)

	go func() {
		if ti.TankWriter != nil {
			select {
			case <-ctx.Done():
				ti.TankWriter.Flush();
				ti.tankFile.Close()
			}
		}
	}()
	return
}
