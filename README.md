# mydb

mydb是一个golang写的键值存储引擎，基于b+树，mmap

### 怎样使用：
```go
package main

import (
	"fmt"
	"strconv"

	"github.com/alberliu/mydb"
)

func toBytes(i int) []byte {
	return []byte(strconv.Itoa(i))
}

func main() {
	db, err := mydb.Open("data")
	if err != nil {
		panic(err)
	}

	fmt.Println("init: ", db.Range(mydb.Infinity, mydb.Infinity))

	for i := 1; i <= 5; i++ {
		_, _ = db.Set(toBytes(i), toBytes(i))
	}
	fmt.Println("set:  ", db.Range(mydb.Infinity, mydb.Infinity))

	_ = db.Delete(toBytes(1))
	fmt.Println("delete", db.Range(mydb.Infinity, mydb.Infinity))

	fmt.Println("range ", db.Range(toBytes(3), toBytes(4)))
}
```
### 简单性能测试
测试环境：
```azure
goos: darwin
goarch: amd64
pkg: github.com/alberliu/mydb
cpu: Intel(R) Core(TM) i5-7267U CPU @ 3.10GHz
```
#### 插入100万数据  
测试代码：
```go
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

	t.Logf("cost:%v tps:%v", time.Now().Sub(now), 1000000/time.Now().Sub(now).Seconds())
	t.Log(tree.fm.statisticsPage())
}
```
测试结果：
```
=== RUN   Test_tree_get_init_data
    tree_test.go:296: 100000
    tree_test.go:296: 200000
    tree_test.go:296: 300000
    tree_test.go:296: 400000
    tree_test.go:296: 500000
    tree_test.go:296: 600000
    tree_test.go:296: 700000
    tree_test.go:296: 800000
    tree_test.go:296: 900000
    tree_test.go:296: 1000000
    tree_test.go:303: cost:28.372586518s tps:35245.288388313376
    tree_test.go:304: fileSize:44253184B, totalPageNum:10804, branchPageNum:126, leafPageNum:10677, recyclePageNum:0, depth:3, recordNum:1000000
--- PASS: Test_tree_get_init_data (28.72s)
```
#### 100万数据下的随机查询
测试代码：  
```go
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
```
测试结果： 
```
Benchmark_tree_get
Benchmark_tree_get-6   	   49418	     23364 ns/op
```
总结：在以上描述的场景下，写入性能35245次每秒，查询性能42800次每秒
