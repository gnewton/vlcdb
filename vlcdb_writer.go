package vlcdb

import (
	"github.com/colinmarc/cdb"
	"log"
	"os"
)

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

	writer := Writer{path: path, keyIndexCounter: startCounter, dataCounter: startCounter, keyIndexCounterBytes: startCounterBytes, dataCounterBytes: startCounterBytes}

	//
	// index
	err := writer.nextKeyIndex()
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
	err := writer.keyIndexWriter.Put(key, writer.dataCounterBytes)
	if err != nil {
		if err == cdb.ErrTooMuchData {
			log.Println(err)
			err = writer.nextKeyIndex()
			if err != nil {
				return err
			}
		} else {
			log.Println(err)
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
func (writer *Writer) Close() error {
	err := writer.keyIndexWriter.Close()
	log.Println("Closing data ", writer.dataCounter-1)
	if err != nil {
		log.Println(err)
		return err
	}
	err = writer.dataWriter.Close()
	if err != nil {
		log.Println(err)
		return err
	}

	writer.writeConfig()

	return nil

}
