package vlcdb

import (
	"github.com/colinmarc/cdb"

	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
)

const ConfigFileName = "vlcdb.json"
const version = "1.0"

type VerifyLevel int

const (
	VerifyNone VerifyLevel = iota
	VerifyHash             = iota
	VerifyAll              = 99999
)

type Config struct {
	Version       string
	KeyIndexFiles []*FileInfo
	DataFiles     []*FileInfo
	path          string `json:"-"`
}

type FileInfo struct {
	Filename string
	Size     int64
	Sha512   string `json:",omitempty"`
}

func (v VerifyLevel) String() string {
	switch v {
	case VerifyNone:
		return "VerifyNone"
	case VerifyHash:
		return "VerifyHash"
	}
	return "VerifyAll"
}

func open(path string, verifyLevel VerifyLevel) (*CDB, error) {
	if path == "" {
		return nil, errors.New("Path is empty")
	}

	_, err := existsAndIsDir(path)
	if err != nil {
		return nil, err
	}

	config, err := loadConfigFile(path, verifyLevel)
	if err != nil {
		return nil, err
	}

	mcdb := new(CDB)

	mcdb.keyIndexes = make([]*cdb.CDB, len(config.KeyIndexFiles))

	for i, f := range config.KeyIndexFiles {
		fullName := config.path + string(os.PathSeparator) + f.Filename
		log.Println("++Opening", fullName)
		realCdb, err := cdb.Open(fullName)
		if err != nil {
			return nil, err
		}
		mcdb.keyIndexes[i] = realCdb
	}

	mcdb.data = make([]*cdb.CDB, len(config.DataFiles))
	for i, f := range config.DataFiles {
		fullName := config.path + string(os.PathSeparator) + f.Filename
		log.Println("++Opening", fullName)
		realCdb, err := cdb.Open(fullName)
		if err != nil {
			return nil, err
		}
		mcdb.data[i] = realCdb
	}

	return mcdb, nil
}

func loadConfigFile(path string, verifyLevel VerifyLevel) (*Config, error) {
	if path == "" {
		return nil, errors.New("Path is empty")
	}

	configFile := path + pathSep + ConfigFileName
	log.Println("Loading config file:", configFile)
	jsonBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	var config Config
	config.path = path
	err = json.Unmarshal(jsonBytes, &config)
	if err != nil {
		log.Println(configFile)
		log.Println(err)
		return nil, err
	}

	err = filesExistAndSane(path, config.KeyIndexFiles)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	err = filesExistAndSane(path, config.DataFiles)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	log.Println("Loading config file:", verifyLevel)
	if verifyLevel >= VerifyNone {
		err = verifyConfig(verifyLevel, &config)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}

	return &config, nil

}

func verifyConfig(verifyLevel VerifyLevel, c *Config) error {
	if c == nil {
		return errors.New("Config is nil")
	}

	log.Println(verifyConfig)

	switch verifyLevel {
	case VerifyHash:

		err := checkHash(c.path, c.KeyIndexFiles)
		if err != nil {
			return err
		}

		err = checkHash(c.path, c.DataFiles)
		if err != nil {
			return err
		}
		if verifyLevel >= VerifyAll {

		}
		fallthrough
	case VerifyAll:
		// should iterate all keys of all files; in parallel?
	}
	return nil

}
