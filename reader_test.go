package vlcdb_test

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/gnewton/vlcdb"
)

const badDir = "/adsfasdfasdasfdasdf/adsfasfd"

func TestBadPath(t *testing.T) {
	_, err := vlcdb.Open(badDir, vlcdb.VerifyNone)
	if err == nil {
		log.Println(err)
		t.Fail()
	}
}

func TestNoConfigFile(t *testing.T) {
	dir, err := tmpDir()
	defer cleanup(dir)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	_, err = vlcdb.Open(dir, vlcdb.VerifyNone)
	if err == nil {
		t.Fail()
	}
}

func TestGoodConfigFile(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	dir, err := tmpDir()
	defer cleanup(dir)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	configFile := dir + string(os.PathSeparator) + vlcdb.ConfigFileName
	err = ioutil.WriteFile(configFile, []byte(validJsonConfig), 0777)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	_, err = vlcdb.Open(dir, vlcdb.VerifyNone) //	_, err = vlcdb.LoadConfig(dir)
	if err == nil {
		t.Fail()
	}
}

func TestMissingStoreFiles(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	dir, err := tmpDir()
	defer cleanup(dir)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	configFile := dir + string(os.PathSeparator) + vlcdb.ConfigFileName
	err = ioutil.WriteFile(configFile, []byte(validJsonConfig), 0777)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	_, err = vlcdb.Open(dir, vlcdb.VerifyNone)
	if err == nil {
		t.Fail()
	}

}

func TestBadConfigFile(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	dir, err := tmpDir()
	defer cleanup(dir)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	configFile := dir + string(os.PathSeparator) + vlcdb.ConfigFileName
	err = ioutil.WriteFile(configFile, []byte("foo bnar \"\" / ,,,,"), 0777)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	_, err = vlcdb.Open(dir, vlcdb.VerifyNone)
	if err == nil {
		log.Println(err)
		t.Fail()

	}
}

func TestGoodIndexesUnVerified(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	_, dir, err := writeIndex([]string{smallString}, []string{largeString}, 1000)
	defer cleanup(dir)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	//_, err = vlcdb.Open(dir, true)
	_, err = vlcdb.Open(dir, vlcdb.VerifyNone)

	if err != nil {
		log.Println(err)
		t.Fail()
	}

}

func TestGoodIndexesVerified(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	_, dir, err := writeIndex([]string{smallString}, []string{largeString}, 1000)
	defer cleanup(dir)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	err = removeConfigFile(dir)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	_, err = vlcdb.Open(dir, vlcdb.VerifyAll)

	if err != nil {
		log.Println(err)
		t.Fail()
	}

}

func TestWriteRead(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var num uint64 = 80000000
	_, dir, err := writeIndex([]string{smallString}, []string{smallString}, num)
	//defer cleanup(dir)
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	err = readIndex(dir, []string{smallString}, []string{smallString}, num)

	if err != nil {
		log.Println(err)
		t.Fail()
	}

}

func tmpDir() (string, error) {
	dir, err := ioutil.TempDir("", "vlcdb_test")
	if err != nil {
		return "", nil
	}
	log.Println("Created tmp dir:", dir)
	return dir, nil
}

func removeConfigFile(dir string) error {
	if dir == "" {
		return errors.New("Empty directory")
	}

	return nil
}

func readIndex(dir string, keys []string, values []string, n uint64) error {

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("Opening dir: ", dir)
	cdb, err := vlcdb.Open(dir, vlcdb.VerifyAll)
	if err != nil {
		log.Println(err)
		return err
	}
	c := kvGenerator(keys, values, n)

	for kv := range c {
		value := cdb.Get(kv.k)
		if err != nil {
			log.Println(err)
			return err
		}
		if value == nil {
			log.Println("Value:", string(kv.k))
			return errors.New("Unable to find value that should be foundable")
		}

		if string(kv.v) != string(value) {
			log.Println("Incorrect value", string(value), "!=", string(kv.v))
		}

	}
	err = cdb.Close()
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

const validJsonConfig = `
{
	"Version": "1.0",
	"KeyIndexFiles": [
		{
			"Filename": "index_0.cdb",
			"FileLength": "",
			"Sha512": "53d8fd41ebf1eb8036f28ac412fac4aad3341bcf6420b045cb4b2b07d1e44b079ef05560b472c85831f2d2c1e054310b84617237dd2465cc44fbb7ba539ba3cf"
		}
	],
	"DataFiles": [
		{
			"Filename": "data_0.cdb",
			"FileLength": "",
			"Sha512": "9a591d379bbe53cd1969b365512425a2ece6290785ee2e63b67440dc54d5137226b3b7d86447370324db14b57a898c34e3dd24582c6f0d48d2ad1d1a4b7335e1"
		}
	]
}
`
