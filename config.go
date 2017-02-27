package vlcdb

import (
	"github.com/colinmarc/cdb"

	"bufio"
	"encoding/json"
	"io/ioutil"
	//"errors"
	"log"
	"os"
)

const ConfigFileName = "vlcdb.json"
const version = "1.0"

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

func open(path string, verify bool) (*CDB, error) {
	_, err := existsAndIsDir(path)
	if err != nil {
		return nil, err
	}

	config, err := loadConfigFile(path, verify)
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

func loadConfigFile(path string, verify bool) (*Config, error) {
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
	log.Println("Loading config file:", verify)
	if verify {
		err = verifyConfig(&config)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}

	return &config, nil

}

func verifyConfig(c *Config) error {
	log.Println(verifyConfig)
	err := checkHash(c.path, c.KeyIndexFiles)
	if err != nil {
		return err
	}

	err = checkHash(c.path, c.DataFiles)
	if err != nil {
		return err
	}
	return nil
}

func (writer *Writer) writeConfig() (*Config, error) {
	log.Println("WriteConfig")
	f, err := os.Create(writer.path + pathSep + ConfigFileName)
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

	return &config, nil
}
