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
	// write [key,value] to the current data file
	err := writer.dataWriter.Put(key, value)
	if err != nil {
		// Spilling over max file length of cdb for data?
		if err == cdb.ErrTooMuchData {
			// make new cdb for data
			err = writer.nextData()
			if err != nil {
				log.Println(err)
				return err
			} else {
				// write data by calling Put again
				return writer.Put(key, value)
			}
		}
	}

	err = writer.keyIndexWriter.Put(key, writer.dataCounterBytes)
	if err != nil {
		// Spilling over max file length of cdb for index?
		if err == cdb.ErrTooMuchData {
			log.Println(err)
			// make new cdb for index
			err = writer.nextKeyIndex()
			if err != nil {
				// write index by calling Put again
				return writer.Put(key, value)
			}
		} else {
			log.Println(err)
			return err
		}
	}

	return nil
}
func (writer *Writer) Close() (*Config, error) {
	err := writer.keyIndexWriter.Close()
	log.Println("Closing data ", writer.dataCounter-1)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	err = writer.dataWriter.Close()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	config, err := writer.writeConfig()
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return config, nil

}
