package vlcdb

import (
	"github.com/colinmarc/cdb"

	"errors"
	"log"
	"strconv"
)

type CDB struct {
	keyIndexes []*cdb.CDB
	data       []*cdb.CDB
	cache      Cache
}

type Cache interface {
	New(size int) (*Cache, error)
	Contains(key interface{}) bool
	Get(key interface{}) (interface{}, bool)
}

func (c *CDB) contains(key []byte) (int, bool) {
	for i, _ := range c.keyIndexes {
		v, err := c.keyIndexes[i].Get(key)
		if err == nil && v != nil {
			intVal, err := strconv.Atoi(string(v))
			if err != nil {
				log.Println(err)
				return -1, false
			}
			return intVal, true
		}
	}
	return -1, false
}

func (c *CDB) get(key []byte, index int) []byte {
	index = index - 1

	if index > len(c.data) || index < 0 {
		log.Println(errors.New("Index requested out of range"))
		return nil
	}
	v, err := c.data[index].Get(key)
	if err != nil {
		log.Println(errors.New("Unable in index " + strconv.Itoa(index) + " to find key " + string(key)))
		return nil
	}
	return v
}
