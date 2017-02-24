package vlcdb

import (
	"github.com/colinmarc/cdb"
	"log"
	"os"
	"strconv"
)

type Writer struct {
	indexWriter *cdb.Writer
	dataWriter  *cdb.Writer
}

var indexCounter = 0
var indexCounterString = strconv.Itoa(indexCounter)

var dataCounter = 0
var dataCounterString = strconv.Itoa(dataCounter)

func Create(path string) (*Writer, error) {

	if _, err := os.Stat(path); os.IsNotExist(err) {

		err := os.Mkdir(path, 0777)
		if err != nil {
			return nil, err
		}
	} else { // dir does exist
		log.Println("Deleting all files")
		err := deleteAllFilesInDirectory(path)
		if err != nil {
			return nil, err
		}
	}

	//
	// index
	indexWriter, err := cdb.Create(path + pathSep + "index" + indexCounterString + ".cdb")
	if err != nil {
		return nil, err
	}
	indexCounter += 1
	indexCounterString = strconv.Itoa(indexCounter)

	//
	// data
	dataWriter, err := cdb.Create(path + pathSep + "data" + strconv.Itoa(dataCounter) + ".cdb")
	if err != nil {
		return nil, err
	}
	dataCounter += 1
	dataCounterString = strconv.Itoa(dataCounter)
	writer := Writer{indexWriter: indexWriter, dataWriter: dataWriter}
	return &writer, nil

}

var empty = []byte("")

func (writer *Writer) Put(key, value []byte) error {
	err := writer.indexWriter.Put(key, empty)
	if err != nil {
		return err
	}
	return writer.dataWriter.Put(key, value)

}

func (writer *Writer) Close() error {
	err1 := writer.indexWriter.Close()
	err2 := writer.dataWriter.Close()
	if err1 != nil {
		return err1
	}
	return err2

}
