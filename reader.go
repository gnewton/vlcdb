package vlcdb

import (
	"github.com/colinmarc/cdb"
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
