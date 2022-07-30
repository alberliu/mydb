package mydb

type myDB struct {
	tree *tree
}

func Open(fileName string) (*myDB, error) {
	fm, err := newFileManager(fileName)
	if err != nil {
		return nil, err
	}

	return &myDB{tree: newTree(fm)}, nil
}

func (m myDB) Add(kay, value []byte) bool {
	return m.tree.add(kay, value)
}

func (m myDB) Update(kay, value []byte) bool {
	return m.tree.update(kay, value)
}

func (m myDB) Delete(kay []byte) bool {
	return m.tree.delete(kay)
}

func (m myDB) Get(kay []byte) ([]byte, bool) {
	return m.tree.get(kay)
}
