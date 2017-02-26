package vlcdb

import (
	"bufio"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

const configFileName = "vlcdb.json"
const version = "1.0"

type Config struct {
	Version       string
	KeyIndexFiles []*FileInfo
	DataFiles     []*FileInfo
}

type FileInfo struct {
	Filename   string
	FileLength string
	Sha512     string
}

func (writer *Writer) writeConfig() error {
	log.Println("WriteConfig")
	f, err := os.Create(writer.path + pathSep + configFileName)
	if err != nil {
		log.Println(err)
		return err
	}
	defer f.Close()

	config := Config{Version: version}

	config.KeyIndexFiles = make([]*FileInfo, writer.keyIndexCounter)

	for i := 0; i < writer.keyIndexCounter; i++ {
		fileInfo := new(FileInfo)
		fileInfo.Filename = makeFileName(writer.path, baseKeyIndexFileName, i)
		var err error
		fileInfo.Sha512, err = makeSha512(fileInfo.Filename)
		if err != nil {
			log.Println(err)
			return err
		}
		config.KeyIndexFiles[i] = fileInfo
	}

	config.DataFiles = make([]*FileInfo, writer.dataCounter)
	for i := 0; i < writer.dataCounter; i++ {
		fileInfo := new(FileInfo)
		fileInfo.Filename = makeFileName(writer.path, baseDataFileName, i)
		fileInfo.Sha512, err = makeSha512(fileInfo.Filename)
		if err != nil {
			log.Println(err)
			return err
		}
		config.DataFiles[i] = fileInfo
	}

	w := bufio.NewWriter(f)
	b, err := json.MarshalIndent(config, "", "\t")
	if err != nil {
		log.Println(err)
		return err
	}

	_, err = w.Write(b)
	if err != nil {
		log.Println(err)
		return err
	}
	w.Flush()

	return nil
}

func makeSha512(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := sha512.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
	//log.Printf("%x\n", h.Sum(nil))
	//return "7hasldkhfaweuih", nil
}
