package vlcdb_test

import (
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/gnewton/vlcdb"
)

func TestWritesRandom(t *testing.T) {
	dir, err := tmpDir()
	if err != nil {
		log.Println(err)
		t.Fail()
	}

	_, err = vlcdb.Create(dir)
	if err != nil {
		log.Println(err)
		t.Fail()
	}
	cleanup(dir)
}

func writeIndex(keys []string, values []string, n uint64) (*vlcdb.Config, string, error) {

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	dir, err := tmpDir()
	if err != nil {
		return nil, "", err
	}
	log.Println("Opening dir: ", dir)
	writer, err := vlcdb.Create(dir)
	if err != nil {
		log.Println(err)
		return nil, "", err
	}
	c := kvGenerator(largeString, largeString, n)

	for kv := range c {
		err := writer.Put(kv.k, kv.v)
		if err != nil {
			log.Println(err)
			return nil, "", err
		}
	}

	config, err := writer.Close()
	if err != nil {
		log.Println(err)
		return nil, "", err
	}
	return config, dir, nil
}

func Test_SmallKey_LargeData(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	_, dir, err := writeIndex([]string{smallString}, []string{largeString}, 10569693)
	defer cleanup(dir)
	if err != nil {
		//t.Fail()
		log.Fatal(err)
	}

}

func Test_SmallKey_SmallData(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	_, dir, err := writeIndex([]string{smallString}, []string{smallString}, 10569693)
	defer cleanup(dir)
	if err != nil {
		t.Fail()
	}
}

func Test_LargeKey_SmallData(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	_, dir, err := writeIndex([]string{largeString}, []string{smallString}, 8569693)
	defer cleanup(dir)
	if err != nil {
		t.Fail()
	}
}

func Test_LargeKey_LargeData(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	_, dir, err := writeIndex([]string{largeString}, []string{largeString}, 18123456)
	defer cleanup(dir)
	if err != nil {
		t.Fail()
	}
}

func TestVeryLargeKey_VeryLargeData(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	_, dir, err := writeIndex([]string{veryLargeString}, []string{veryLargeString}, 8123456)
	defer cleanup(dir)
	if err != nil {
		t.Fail()
	}
}

func TestVeryLargeKey_SmallData(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	_, dir, err := writeIndex([]string{veryLargeString}, []string{smallString}, 123456)
	defer cleanup(dir)
	if err != nil {
		t.Fail()
	}
}

func cleanup(dir string) {
	log.Println("   Removing tmp dir:", dir)
	if dir == "" {
		return
	}
	err := os.RemoveAll(dir)
	if err != nil {
		log.Println(err)
	}
}

////////////////////////////////////////////

var smallString = "01234567" // 8

var mediumString = smallString + smallString + smallString + smallString    // 32
var largeString = mediumString + mediumString + mediumString + mediumString // 128

var veryLargeString = largeString + largeString + largeString + largeString + largeString + largeString + largeString + largeString + largeString + largeString + largeString + largeString + largeString + largeString + largeString + largeString

type keyValue struct {
	k, v []byte
}

func kvGenerator(baseKey, baseValue string, n uint64) chan *keyValue {
	kvChan := make(chan *keyValue, 100)

	go func() {
		var i uint64
		for i = 0; i < n; i++ {
			si := strconv.FormatUint(i, 10)
			key := []byte(baseKey + si)
			value := []byte(baseValue + si)
			kv := keyValue{k: key, v: value}

			kvChan <- (&kv)
		}
		close(kvChan)
	}()

	return kvChan
}
