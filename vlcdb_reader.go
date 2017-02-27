package vlcdb

import ()

func Open(path string, verify bool) (*CDB, error) {
	return open(path, verify)
}

func (cdb *CDB) Get(key []byte) []byte {
	var dataIndex int
	var ok bool
	if dataIndex, ok = cdb.contains(key); ok {
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
