package vlcdb

import (
	"bufio"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"

	"github.com/colinmarc/cdb"
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

const cdbSuffix = ".cdb"

var startCounterBytes = []byte(strconv.Itoa(startCounter))

func (writer *Writer) nextData() error {
	if writer.dataWriter != nil {
		log.Println("Closing data ", writer.dataCounter-1)
		err := writer.dataWriter.Close()
		if err != nil {
			log.Println(err)
			return err
		}
	}

	log.Println("Opening new data ", writer.dataCounter)
	dataWriter, err := newWriter(writer.path, baseDataFileName, writer.dataCounter)
	if err != nil {
		log.Println(err)
		return err
	}
	writer.dataWriter = dataWriter
	writer.dataCounter += 1
	writer.dataCounterBytes = []byte(strconv.Itoa(writer.dataCounter))

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
	writer.keyIndexWriter = newKeyIndexWriter
	log.Println("Opened new index ", writer.keyIndexCounter)

	keyIndexCounter += 1

	writer.keyIndexCounter += 1
	writer.keyIndexCounterBytes = []byte(strconv.Itoa(writer.keyIndexCounter))

	return nil

}

func (writer *Writer) writeConfig() (*Config, error) {
	if writer == nil {
		return nil, errors.New("Writer is nil")
	}

	configPath := writer.path + pathSep + ConfigFileName
	f, err := os.Create(configPath)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer f.Close()

	config := Config{Version: version}

	config.KeyIndexFiles = make([]*FileInfo, writer.keyIndexCounter)

	for i := 0; i < writer.keyIndexCounter; i++ {

		fileInfo := new(FileInfo)
		fileInfo.Filename = makeFileName(baseKeyIndexFileName, i)

		var err error
		fileInfo.Sha512, err = makeSha512(makePathFileName(writer.path, baseKeyIndexFileName, i))
		if err != nil {
			log.Println(err)
			return nil, err
		}
		f, err := exists(makePathFileName(writer.path, baseKeyIndexFileName, i))
		if err != nil {
			log.Println(err)
			return nil, err
		}
		fileInfo.Size = f.Size()

		config.KeyIndexFiles[i] = fileInfo
	}

	config.DataFiles = make([]*FileInfo, writer.dataCounter)
	for i := 0; i < writer.dataCounter; i++ {
		fileInfo := new(FileInfo)
		fileInfo.Filename = makeFileName(baseDataFileName, i)
		fileInfo.Sha512, err = makeSha512(makePathFileName(writer.path, baseDataFileName, i))
		if err != nil {
			log.Println(err)
			return nil, err
		}

		f, err := exists(makePathFileName(writer.path, baseDataFileName, i))
		if err != nil {
			log.Println(err)
			return nil, err
		}
		fileInfo.Size = f.Size()
		config.DataFiles[i] = fileInfo
	}

	w := bufio.NewWriter(f)
	b, err := json.MarshalIndent(config, "", "\t")
	if err != nil {
		log.Println(err)
		return nil, err
	}

	_, err = w.Write(b)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	w.Flush()
	log.Println("Config written to", configPath)
	return &config, nil
}

func newWriter(path, name string, counter int) (*cdb.Writer, error) {
	f := makePathFileName(path, name, counter)
	log.Println("Opening new writer", f)
	return cdb.Create(f)
}

func makePathFileName(path, name string, counter int) string {
	return path + pathSep + makeFileName(name, counter)
}

func makeFileName(name string, counter int) string {
	return name + "_" + strconv.Itoa(counter) + cdbSuffix
}
