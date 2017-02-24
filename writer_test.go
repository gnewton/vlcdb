package vlcdb_test

import (
	"github.com/gnewton/vlcdb"
	"log"
	"testing"
)

func TestWritesRandom(t *testing.T) {
	_, err := vlcdb.Create("foo")
	if err != nil {
		log.Println(err)
		log.Fatal(err)
	}
}
