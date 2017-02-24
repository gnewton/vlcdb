package vlcdb

import (
	"github.com/colinmarc/cdb"
	"log"
	"os"
	"strconv"
)

type Writer struct {
	path        string
	indexWriter *cdb.Writer
	dataWriter  *cdb.Writer

	indexCounter      int
	indexCounterBytes []byte

	dataCounter      int
	dataCounterBytes []byte
}

const startCounter = 0

var startCounterBytes = []byte(strconv.Itoa(startCounter))

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

	writer := Writer{path: path, indexCounter: startCounter, dataCounter: startCounter, indexCounterBytes: startCounterBytes, dataCounterBytes: startCounterBytes}

	//
	// index
	err := writer.nextIndex()
	if err != nil {
		return nil, err
	}

	//
	// data
	err = writer.nextData()
	if err != nil {
		return nil, err
	}

	//writer := Writer{indexWriter: indexWriter, dataWriter: dataWriter}
	return &writer, nil

}

func (writer *Writer) Put(key, value []byte) error {
	err := writer.indexWriter.Put(key, writer.dataCounterBytes)
	if err != nil {
		if err == cdb.ErrTooMuchData {
			err = writer.nextIndex()
		} else {
			return err
		}
	}

	err = writer.dataWriter.Put(key, value)
	if err != nil {
		if err == cdb.ErrTooMuchData {
			err = writer.nextData()
		} else {
			return err
		}
	}
	return nil
}

func (writer *Writer) nextData() error {
	if writer.dataWriter != nil {
		log.Println("Closing data")
		err := writer.dataWriter.Close()
		if err != nil {
			return nil
		}
	}
	log.Println("Opening new data")
	dataWriter, err := newWriter(writer.path, "data", writer.dataCounter)
	if err != nil {
		return nil
	}

	writer.dataCounter += 1
	writer.dataCounterBytes = []byte(strconv.Itoa(writer.dataCounter))
	writer.dataWriter = dataWriter

	return nil
}

func (writer *Writer) nextIndex() error {
	if writer.indexWriter != nil {
		log.Println("Closing index")
		err := writer.indexWriter.Close()
		if err != nil {
			return nil
		}
	}

	log.Println("Opening new index")

	indexWriter, err := newWriter(writer.path, "index", writer.indexCounter)
	if err != nil {
		return err
	}
	writer.indexCounter += 1
	writer.indexCounterBytes = []byte(strconv.Itoa(writer.indexCounter))
	writer.indexWriter = indexWriter

	return nil

}

func (writer *Writer) Close() error {
	err1 := writer.indexWriter.Close()
	err2 := writer.dataWriter.Close()
	if err1 != nil {
		return err1
	}
	return err2

}

var cdbSuffix = ".cdb"

func newWriter(path, name string, counter int) (*cdb.Writer, error) {
	return cdb.Create(path + pathSep + name + "_" + strconv.Itoa(counter) + cdbSuffix)
}
