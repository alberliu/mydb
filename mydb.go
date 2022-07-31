package mydb

import (
	"errors"
	"sync"
)

var (
	ErrRecordTooLarge = errors.New("err key value too large")
	ErrRecordNotExist = errors.New("err record not exist")
)

type myDB struct {
	tree *tree
	m    sync.RWMutex
}

func Open(fileName string) (*myDB, error) {
	fm, err := newFileManager(fileName)
	if err != nil {
		return nil, err
	}

	return &myDB{tree: newTree(fm)}, nil
}

func (m myDB) Add(key, value []byte) error {
	r := record{Key: key, Value: value}
	if err := r.check(); err != nil {
		return err
	}

	m.m.Lock()
	defer m.m.Unlock()

	m.tree.add(key, value)
	return nil
}

func (m myDB) Update(key, value []byte) error {
	r := record{Key: key, Value: value}
	if err := r.check(); err != nil {
		return err
	}

	m.m.Lock()
	defer m.m.Unlock()

	m.tree.update(key, value)
	return nil
}

func (m myDB) Delete(key []byte) error {
	m.m.Lock()
	defer m.m.Unlock()

	ok := m.tree.delete(key)
	if !ok {
		return ErrRecordNotExist
	}
	return nil
}

func (m myDB) Get(key []byte) ([]byte, error) {
	m.m.RLock()
	defer m.m.RUnlock()

	value, ok := m.tree.get(key)
	if !ok {
		return nil, ErrRecordNotExist
	}
	return value, nil
}

func (m myDB) Range(min, max []byte) []*record {
	m.m.RLock()
	defer m.m.RUnlock()

	return m.tree.query(min, max)
}
