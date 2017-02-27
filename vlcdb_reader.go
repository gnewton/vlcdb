package vlcdb

import ()

func Open(path string, verify bool) (*CDB, error) {
	return open(path, verify)
}

func (cdb *CDB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

//func (cdb *CDB) Iter() *Iterator {
//	return nil
//}

func (cdb *CDB) Close() error {
	return nil
}
