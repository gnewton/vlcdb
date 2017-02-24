package vlcdb

import (
	"io/ioutil"
	"os"
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
