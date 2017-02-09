package dataflow

import (
	"astra/metadata"
	"errors"
	"fmt"
	"github.com/goinggo/tracelog"
	"golang.org/x/net/context"
	"os"
	"bufio"
	"sync"
)

var packageName = "astra/dataflow"

type TableInfoType struct {
	*metadata.TableInfoType
	TankWriter *bufio.Writer
	tankFile *os.File
	tankFileLock sync.Mutex
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

	if ti.tankFile == nil && ((flags & os.O_CREATE) == os.O_CREATE) {
		ti.tankFileLock.Lock()
		defer ti.tankFileLock.Unlock()
		if ti.tankFile == nil && ((flags & os.O_CREATE) == os.O_CREATE) {
			file, err := os.OpenFile(pathToTankFile, flags, 0666)
			if err != nil {
				tracelog.Errorf(err, packageName, funcName, "Opening file %v", pathToTankFile)
				return err
			}
			ti.TankWriter = bufio.NewWriter(file)
			ti.tankFile = file
			go func() {
				funcName := "TableInfoType.OpenTank.closeTank"
				tracelog.Started(packageName,funcName)
				if ti.TankWriter != nil {
					select {
					case <-ctx.Done():
						ti.TankWriter.Flush();
						ti.tankFile.Close()
					}
				}
				tracelog.Completed(packageName,funcName)
			}()
		}
	}

	return
}
