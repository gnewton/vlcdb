package vlcdb

import (
	"github.com/colinmarc/cdb"
	"log"
	"strconv"
)

type Writer struct {
	path string
	//keyIndexWriter *cdb.Writer
	keyIndexWriter *cdb.Writer
	dataWriter     *cdb.Writer

	keyIndexCounter      int
	keyIndexCounterBytes []byte

	dataCounter      int
	dataCounterBytes []byte
}

const baseKeyIndexFileName = "index"
const baseDataFileName = "data"
const startCounter = 0

var cdbSuffix = ".cdb"
var startCounterBytes = []byte(strconv.Itoa(startCounter))

func (writer *Writer) nextData() error {
	if writer.dataWriter != nil {
		log.Println("Closing data ", writer.dataCounter-1)
		err := writer.dataWriter.Close()
		if err != nil {
			return nil
		}
	}
	log.Println("Opening new data ", writer.dataCounter)
	dataWriter, err := newWriter(writer.path, baseDataFileName, writer.dataCounter)
	if err != nil {
		return nil
	}

	writer.dataCounter += 1
	writer.dataCounterBytes = []byte(strconv.Itoa(writer.dataCounter))
	writer.dataWriter = dataWriter

	return nil
}

var keyIndexCounter = 0

func (writer *Writer) nextKeyIndex() error {
	if writer.keyIndexWriter != nil {
		log.Println("Closing index ", writer.keyIndexCounter-1)
		err := writer.keyIndexWriter.Close()
		if err != nil {
			return err
		}
	}

	newKeyIndexWriter, err := newWriter(writer.path, baseKeyIndexFileName, writer.keyIndexCounter)
	if err != nil {
		return err
	}

	log.Println("Opened new index ", writer.keyIndexCounter)
	keyIndexCounter += 1

	writer.keyIndexCounter += 1
	writer.keyIndexCounterBytes = []byte(strconv.Itoa(writer.keyIndexCounter))
	writer.keyIndexWriter = newKeyIndexWriter

	return nil

}

func newWriter(path, name string, counter int) (*cdb.Writer, error) {
	return cdb.Create(makeFileName(path, name, counter))
}

func makeFileName(path, name string, counter int) string {
	return path + pathSep + name + "_" + strconv.Itoa(counter) + cdbSuffix
}
