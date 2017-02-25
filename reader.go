package vlcdb

import (
	"github.com/colinmarc/cdb"
)

type CDB struct {
	keyIndexes []*cdb.CDB
	data       []*cdb.CDB
}
