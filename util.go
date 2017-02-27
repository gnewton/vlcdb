package vlcdb

import (
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

var pathSep = string(os.PathSeparator)

func deleteAllFilesInDirectory(path string) error {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, f := range files {
		err := os.Remove(path + pathSep + f.Name())
		if err != nil {
			return err
		}
	}
	return nil
}

func existsAndIsDir(path string) (os.FileInfo, error) {
	fileInfo, err := exists(path)
	if err != nil {
		return nil, err
	}

	if !fileInfo.IsDir() {
		return nil, errors.New(path + " is not a directory")
	}
	return fileInfo, nil
}

func existsAndIsFile(path string) (os.FileInfo, error) {
	fileInfo, err := exists(path)
	if err != nil {
		return nil, err
	}

	if !fileInfo.IsDir() && fileInfo.Mode().IsRegular() {
		return fileInfo, nil
	}
	return nil, errors.New(path + " is not a file or has some other problem")
}

func exists(path string) (os.FileInfo, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}

func filesExistAndSane(path string, files []*FileInfo) error {
	if len(files) == 0 {
		return errors.New("There are no files. There should be at least one")
	}

	for _, f := range files {
		fileInfo, err := exists(path + string(os.PathSeparator) + f.Filename)
		if err != nil {
			return err
		}
		if fileInfo.Size() != f.Size {
			return errors.New("File size does not config match for " + f.Filename + "; actual:" + strconv.FormatInt(fileInfo.Size(), 10) + " vs config:" + strconv.FormatInt(f.Size, 10))
		}
	}

	return nil
}

func checkHash(path string, files []*FileInfo) error {
	for _, f := range files {
		filename := path + pathSep + f.Filename
		sha512, err := makeSha512(filename)
		if err != nil {
			return err
		}
		if sha512 != f.Sha512 {
			return errors.New("SHA512 does not match for file:" + filename)
		}
	}
	return nil
}

func makeSha512(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		log.Println(err)
		return "", err
	}
	defer f.Close()

	h := sha512.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
