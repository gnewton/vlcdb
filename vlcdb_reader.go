package vlcdb

import (
	"log"
)

func Open(path string, verifyLevel VerifyLevel) (*CDB, error) {
	return open(path, verifyLevel)
}

func (cdb *CDB) Get(key []byte) []byte {
	var dataIndex int
	var ok bool
	if dataIndex, ok = cdb.contains(key); !ok {
		log.Println("Not found", string(key), "in", dataIndex)
		return nil
	}
	return cdb.get(key, dataIndex)
}

//func (cdb *CDB) Iter() *Iterator {
//	return nil
//}

func (cdb *CDB) Close() error {
	return nil
}
