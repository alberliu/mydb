package mydb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func newDefaultTree() *tree {
	name := "data.txt"
	os.Remove(name)
	fm, err := newFileManager(name)
	if err != nil {
		panic(err)
	}
	return newTree(fm)
}

func Test_tree_add_rand(t *testing.T) {
	tree := newDefaultTree()
	var records []*Record

	seed := time.Now().Unix()
	rand.Seed(seed)
	fmt.Println("seed", seed)

	for i := 0; i < 50000; i++ {
		r := []byte(fmt.Sprintf("%d", rand.Intn(1000000000000000000)))
		if i%10000 == 0 {
			fmt.Println(i)
		}

		tree.add(r, r)

		records = appendRecord(records, &Record{Key: r, Value: r})
	}

	all := tree.all()
	isSorted(all)
	sortRecords(records)
	assertMatch(t, all, records, nil)
	tree.fm.statisticsPage()
}

func Test_tree_add_head(t *testing.T) {
	tree := newDefaultTree()
	var records []*Record
	for i := 50000; i > 0; i-- {
		data := []byte(fmt.Sprintf("%12d", i))
		if i%1000 == 0 {
			t.Log(string(data))
		}
		tree.add(data, data)

		records = appendRecord(records, &Record{Key: data, Value: data})
	}
	sortRecords(records)
	assertMatch(t, tree.all(), records, nil)
	tree.fm.statisticsPage()
}

func Test_tree_add_hea2(t *testing.T) {
	tree := newDefaultTree()

	for i := 0; i < 10; i++ {
		tree.add(toBytes(i), toBytes(i))

		sorted := isSorted(tree.all())
		if !sorted {
			t.Fatal("err")
		}
	}

	for i := 101; i < 110; i++ {
		tree.add(toBytes(i), toBytes(i))

		sorted := isSorted(tree.all())
		if !sorted {
			t.Fatal("err")
		}
	}
}

func Test_tree_add_tail(t *testing.T) {
	tree := newDefaultTree()
	var records []*Record
	for i := 0; i < 50000; i++ {
		data := []byte(fmt.Sprintf("%6d", i))
		if i%1000 == 0 {
			t.Log(string(data))
		}

		tree.add(data, data)
		records = appendRecord(records, &Record{Key: data, Value: data})
	}

	sortRecords(records)
	assertMatch(t, tree.all(), records, nil)
	tree.fm.statisticsPage()
}

func Test_tree_add_central(t *testing.T) {
	tree := newDefaultTree()
	tree.add([]byte("00001"), []byte("00001"))
	tree.add([]byte("10000"), []byte("10000"))

	var records []*Record
	records = appendRecord(records, &Record{Key: []byte("00001"), Value: []byte("00001")})
	records = appendRecord(records, &Record{Key: []byte("10000"), Value: []byte("10000")})

	for i := 100000; i > 2; i-- {
		if i%1000 == 0 {
			fmt.Println(i)
			tree.fm.statisticsPage()
		}

		data := []byte(fmt.Sprintf("%6d", i))
		tree.add(data, data)

		records = appendRecord(records, &Record{Key: data, Value: data})
	}
	sortRecords(records)
	assertMatch(t, tree.all(), records, nil)
}

func newDefaultTreeWithData() *tree {
	tree := newDefaultTree()
	for i := 0; i < 5000; i++ {
		data := []byte(fmt.Sprintf("%6d", i))
		tree.add(data, data)
	}
	return tree
}

func Test_tree_update(t *testing.T) {
	tree := newDefaultTreeWithData()

	key := []byte(fmt.Sprintf("%6d", 1))
	value := []byte(fmt.Sprintf("%1d", 100))
	tree.update(key, value)

	result, ok := tree.get(key)
	if !ok || bytes.Compare(result, value) != 0 {
		t.Fatal()
	}
}

func Test_tree_delete(t *testing.T) {
	tree := newDefaultTreeWithData()

	key := []byte(fmt.Sprintf("%6d", 1))
	ok := tree.delete(key)
	if !ok {
		t.Fatal()
	}

	_, ok = tree.get(key)
	if ok {
		t.Fatal()
	}
}
