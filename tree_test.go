package mydb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func newDefaultTree() *tree {
	name := "data.txt"
	os.Remove(name)
	fm, err := newFileManager(name, defaultPageSize)
	if err != nil {
		panic(err)
	}
	return newTree(fm)
}

// Test_tree_fuzz 模糊测试
func Test_tree_fuzz(t *testing.T) {
	seed := time.Now().Unix()
	t.Log("seed", seed)
	rand.Seed(seed)

	tree := newDefaultTree()
	mock := newRecordList()

	count := 0
	for {
		key := toBytes(rand.Intn(100000000))
		switch rand.Intn(2) {
		case 0:
			value := toBytes(rand.Intn(100000000))
			tree.set(key, value)
			mock.set(&record{Key: key, Value: value})
		case 1:
			tree.delete(key)
			mock.delete(key)
		}

		if count%100000 == 0 {
			t.Log(count)
			mock.assertMatch(t, tree.all(), nil)
			t.Log(tree.fm.statisticsPage())
		}
		count++
	}
}

func Test_tree_complex_rand_repeat(t *testing.T) {
	i := 1
	for {
		Test_tree_complex_rand(t)

		t.Log("test count:", i)
		i++
		time.Sleep(time.Second * 5)
	}
}

// Test_tree_complex_rand 综合随机测试
// bug seed 1661230801
func Test_tree_complex_rand(t *testing.T) {
	seed := time.Now().Unix()
	t.Log("seed", seed)
	rand.Seed(seed)

	const count = 100000

	tree := newDefaultTree()
	defer tree.fm.file.Close()

	mock := newRecordList()

	for i := 0; i < count; i++ {
		if i%10000 == 0 {
			t.Log(i)
		}

		key := toBytes(rand.Intn(count))
		switch rand.Intn(2) {
		case 0:
			value := toBytes(rand.Intn(count))
			tree.set(key, value)
			mock.set(&record{Key: key, Value: value})
		case 1:
			tree.delete(key)
			mock.delete(key)
		}
	}

	mock.assertMatch(t, tree.all(), nil)
	t.Log(tree.fm.statisticsPage())
}

func Test_tree_add_rand(t *testing.T) {
	seed := time.Now().Unix()
	t.Log("seed", seed)
	rand.Seed(seed)

	tree := newDefaultTree()
	mock := newRecordList()
	for i := 0; i <= 100000; i++ {
		if i%10000 == 0 {
			t.Log(i)
		}

		r := toBytes(rand.Intn(1000000000000000000))

		isNew := tree.set(r, r)
		mockIsNew := mock.set(&record{Key: r, Value: r})
		if isNew != mockIsNew {
			t.Fatal()
		}
	}

	all := tree.all()
	mock.assertMatch(t, all, nil)
	t.Log(tree.fm.statisticsPage())
}

func Test_tree_add_head(t *testing.T) {
	tree := newDefaultTree()
	mock := newRecordList()

	for i := 50000; i > 0; i-- {
		data := []byte(fmt.Sprintf("%12d", i))
		if i%10000 == 0 {
			t.Log(string(data))
		}
		tree.set(data, data)

		mock.set(&record{Key: data, Value: data})
	}

	mock.assertMatch(t, tree.all(), nil)
	t.Log(tree.fm.statisticsPage())
}

func Test_tree_add_hea2(t *testing.T) {
	tree := newDefaultTree()

	for i := 0; i < 10; i++ {
		tree.set(toBytes(i), toBytes(i))

		sorted := isSorted(tree.all())
		if !sorted {
			t.Fatal("err")
		}
	}

	for i := 101; i < 110; i++ {
		tree.set(toBytes(i), toBytes(i))

		sorted := isSorted(tree.all())
		if !sorted {
			t.Fatal("err")
		}
	}
}

func Test_tree_add_tail(t *testing.T) {
	tree := newDefaultTree()
	mock := newRecordList()

	for i := 0; i < 50000; i++ {
		data := []byte(fmt.Sprintf("%6d", i))
		if i%10000 == 0 {
			t.Log("add", string(data))
		}

		tree.set(data, data)
		mock.set(&record{Key: data, Value: data})
	}

	mock.assertMatch(t, tree.all(), nil)
	t.Log(tree.fm.statisticsPage())
}

func Test_tree_add_central(t *testing.T) {
	tree := newDefaultTree()
	tree.set([]byte("00001"), []byte("00001"))
	tree.set([]byte("10000"), []byte("10000"))

	mock := newRecordList()
	mock.set(&record{Key: []byte("00001"), Value: []byte("00001")})
	mock.set(&record{Key: []byte("10000"), Value: []byte("10000")})

	for i := 100000; i > 2; i-- {
		if i%10000 == 0 {
			t.Log(i)
		}

		data := []byte(fmt.Sprintf("%6d", i))
		tree.set(data, data)

		mock.set(&record{Key: data, Value: data})
	}
	mock.assertMatch(t, tree.all(), nil)
}

func newDefaultTreeWithData() *tree {
	tree := newDefaultTree()
	for i := 0; i < 5000; i++ {
		data := []byte(fmt.Sprintf("%6d", i))
		tree.set(data, data)
	}
	return tree
}

func Test_tree_update(t *testing.T) {
	tree := newDefaultTreeWithData()

	key := []byte(fmt.Sprintf("%6d", 1))
	value := []byte(fmt.Sprintf("%1d", 100))
	tree.set(key, value)

	result, ok := tree.get(key)
	if !ok || !bytes.Equal(result, value) {
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

func Test_tree_query(t *testing.T) {
	tree := newDefaultTreeWithData()

	result := tree.query(Infinity, []byte(fmt.Sprintf("%6d", 2)))
	t.Log(result)
	if len(result) != 3 {
		t.Fatal()
	}

	result = tree.query([]byte(fmt.Sprintf("%6d", 4997)), Infinity)
	t.Log(result)
	if len(result) != 3 {
		t.Fatal()
	}

	result = tree.query([]byte(fmt.Sprintf("%6d", 4990)), []byte(fmt.Sprintf("%6d", 4993)))
	t.Log(result)
	if len(result) != 4 {
		t.Fatal()
	}
}

func Test_tree__display(t *testing.T) {
	tree := newDefaultTree()
	for i := 0; i <= 100; i++ {
		data := []byte(fmt.Sprintf("%6d", i))
		tree.set(data, data)
	}

	tree._display()
}

func Test_tree_count(t *testing.T) {
	tree := newDefaultTree()
	for i := 0; i < 100; i++ {
		data := []byte(fmt.Sprintf("%6d", i))
		tree.set(data, data)
	}

	num := tree.count()
	if num != 100 {
		t.Fatal(num)
	}
}

// old 14345
// new 36205
func Test_tree_get_init_data(t *testing.T) {
	tree := newDefaultTree()
	now := time.Now()
	for i := 1; i <= 1000000; i++ {
		if i%100000 == 0 {
			t.Log(i)
		}

		data := []byte(strconv.Itoa(i))
		tree.set(data, data)
	}

	t.Logf("cost:%v tps:%v", time.Since(now), 1000000/time.Since(now).Seconds())
	t.Log(tree.fm.statisticsPage())
}

// old 26573
// new 45018
func Benchmark_tree_get(b *testing.B) {
	fm, err := newFileManager("data.txt", defaultPageSize)
	if err != nil {
		panic(err)
	}
	tree := newTree(fm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.get([]byte(strconv.Itoa(rand.Intn(1000000))))
	}
}

func TestTime(t *testing.T) {
	fmt.Println(int(time.Second / 23364))
}
