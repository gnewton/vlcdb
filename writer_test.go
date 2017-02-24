package vlcdb_test

import (
	"github.com/gnewton/vlcdb"
	"log"
	"strconv"
	"testing"
)

func TestWritesRandom(t *testing.T) {
	_, err := vlcdb.Create("foo")
	if err != nil {
		log.Println(err)
		log.Fatal(err)
	}
}

func TestMany(t *testing.T) {
	writer, err := vlcdb.Create("foo")
	if err != nil {
		log.Println(err)
		log.Fatal(err)
	}
	for i := 0; i < 65855000; i++ {
		si := strconv.Itoa(i)
		//writer.Put([]byte("Alice"+si), []byte("Practice"))
		err := writer.Put([]byte("Ai mmmmmnnnoocemmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"+si), []byte("foo nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"+si))
		if err != nil {
			log.Println(si + " *************************")
			log.Fatal(err)
		}
	}

	err = writer.Close()
	if err != nil {
		log.Fatal(err)
	}
}
