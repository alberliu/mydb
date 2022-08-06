package mydb

import (
	"errors"
	"sync"
)

var (
	ErrRecordTooLarge = errors.New("error key value too large")
	ErrRecordNotExist = errors.New("error record not exist")
)

type myDB struct {
	tree *tree
	m    sync.RWMutex
}

func Open(fileName string, opts ...Option) (*myDB, error) {
	options := getOptions(opts...)

	fm, err := newFileManager(fileName, options.pageSize)
	if err != nil {
		return nil, err
	}

	return &myDB{tree: newTree(fm)}, nil
}

func (m *myDB) checkParam(key, value []byte) error {
	r := record{Key: key, Value: value}
	if r.needSpaceLen() > recordMaxSize(uint16(m.tree.fm.pageSize)) {
		return ErrRecordTooLarge
	}
	return nil
}

func (m *myDB) Set(key, value []byte) error {
	err := m.checkParam(key, value)
	if err != nil {
		return err
	}

	m.m.Lock()
	defer m.m.Unlock()

	m.tree.set(key, value)
	return nil
}

func (m *myDB) Delete(key []byte) error {
	m.m.Lock()
	defer m.m.Unlock()

	ok := m.tree.delete(key)
	if !ok {
		return ErrRecordNotExist
	}
	return nil
}

func (m *myDB) Get(key []byte) ([]byte, error) {
	m.m.RLock()
	defer m.m.RUnlock()

	value, ok := m.tree.get(key)
	if !ok {
		return nil, ErrRecordNotExist
	}
	return value, nil
}

func (m *myDB) Range(min, max []byte) []*record {
	m.m.RLock()
	defer m.m.RUnlock()

	return m.tree.query(min, max)
}
