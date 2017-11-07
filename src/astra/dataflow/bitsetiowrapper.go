package dataflow

import (
	"bufio"
	"context"
	"fmt"
	"github.com/goinggo/tracelog"
	"os"
	"sparsebitset"
)

type BitsetIOWrapperInterface interface {
	FileName() (string, error)
	BitSet() (*sparsebitset.BitSet, error)
	Description() string
}

func WriteBitsetToFile(
	cancelContext context.Context,
	pathToDir string,
	wrapper BitsetIOWrapperInterface,
) (err error) {
	funcName := "DataFlow::WriteBitsetToFile"
	tracelog.Started(packageName, funcName)

	description := fmt.Errorf("writting %v bitset data", wrapper.Description())
	if pathToDir == "" {
		err = fmt.Errorf("%v: given path is empty ", description)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	fileName, err := wrapper.FileName()
	if err != nil {
		err = fmt.Errorf("%v: %v", description, err)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	if fileName == "" {
		err = fmt.Errorf("%v: given full file name is empty", description)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	if cancelContext == nil {
		err = fmt.Errorf("%v: given cancel context is not initialized", description)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	bitSet, err := wrapper.BitSet()
	if err != nil {
		err = fmt.Errorf("%v: %v", description, err)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	if bitSet == nil {
		err = fmt.Errorf("%v: bitset is not initialized", description)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	err = os.MkdirAll(pathToDir, 700)

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "making directories for path %v: %v", pathToDir, err)
		return err
	}

	fullPathFileName := fmt.Sprintf("%v%c%v", pathToDir, os.PathSeparator, fileName)

	file, err := os.Create(fullPathFileName)
	if err != nil {
		err = fmt.Errorf("creating file %v: %v: %v ", fullPathFileName, description, err)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	defer file.Close()

	buffered := bufio.NewWriter(file)

	defer buffered.Flush()

	_, err = bitSet.WriteTo(cancelContext, buffered)

	if err != nil {
		err = fmt.Errorf("persisting to file %v: %v: %v", fullPathFileName, description, err)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	tracelog.Completed(packageName, funcName)

	return err
}

func ReadBitsetFromFile(cancelContext context.Context, pathToDir string, wrapper BitsetIOWrapperInterface) (err error) {
	funcName := "DataFlow::ReadBitSetFromFile"
	tracelog.Started(packageName, funcName)

	description := fmt.Errorf("reading %v bitset data", wrapper.Description())

	if pathToDir == "" {
		err = fmt.Errorf("%v: given path is empty ", description)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	fileName, err := wrapper.FileName()
	if err != nil {
		err = fmt.Errorf("%v: %v", description, err)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	if fileName == "" {
		err = fmt.Errorf("%v: given full file name is empty", description)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	if cancelContext == nil {
		err = fmt.Errorf("%v: given cancel context is not initialized", description)
		tracelog.Error(err, packageName, funcName)
		return err
	}
	bitSet, err := wrapper.BitSet()
	if err != nil {
		err = fmt.Errorf("%v: %v", description, err)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	if bitSet == nil {
		err = fmt.Errorf("%v: bitset is not initialized", description)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	fullPathFileName := composeBistsetFileFullPath(pathToDir, fileName)
	file, err := os.Open(fullPathFileName)
	if err != nil {
		err = fmt.Errorf("opening file %v: %v: %v ", fullPathFileName, description, err)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	defer file.Close()

	buffered := bufio.NewReader(file)
	_, err = bitSet.ReadFrom(cancelContext, buffered)

	if err != nil {
		err = fmt.Errorf("restoring from file %v: %v: %v", fullPathFileName, description, err)
		tracelog.Error(err, packageName, funcName)
		return err
	}

	tracelog.Completed(packageName, funcName)

	return err
}
