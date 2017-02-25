package vlcdb

import ()

func Open(path string) (*CDB, error) {
	// cdb, err := cdb.Open(path)
	// if err != nil {
	// 	return nil, err
	// }
	// mcdb := CDB{cdbm: cdb}
	// return &mcdb, nil
	return nil, nil

}

func (cdb *CDB) Close() error {
	//return cdb.cdbm.Close()
	return nil
}

func (cdb *CDB) Get(key []byte) ([]byte, error) {
	// val, err := cdb.cdbm.Get(key)
	// if err != nil {
	// 	return nil, err
	// }
	// return val, err
	return nil, nil

}
