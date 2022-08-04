# mydb

mydb是一个golang写的键值存储引擎，基于b+树，mmap

### 怎样使用：
```go
package main

import (
	"fmt"
	"github.com/alberliu/mydb"
	"strconv"
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
		db.Add(toBytes(i), toBytes(i))
	}
	fmt.Println("add:  ", db.Range(mydb.Infinity, mydb.Infinity))

	db.Update(toBytes(1), toBytes(4))
	fmt.Println("update", db.Range(mydb.Infinity, mydb.Infinity))

	db.Delete(toBytes(1))
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
cpu: Intel(R) Core(TM) i5-8500B CPU @ 3.00GHz
```
插入10万数据
```azure
=== RUN   Test_tree_get
    tree_test.go:222: 0
    tree_test.go:222: 10000
    tree_test.go:222: 20000
    tree_test.go:222: 30000
    tree_test.go:222: 40000
    tree_test.go:222: 50000
    tree_test.go:222: 60000
    tree_test.go:222: 70000
    tree_test.go:222: 80000
    tree_test.go:222: 90000
    tree_test.go:222: 100000
--- PASS: Test_tree_get (4.97s)
```
10万数据下的随机查询
```azure
Benchmark_tree_get
Benchmark_tree_get-6   	   43221	     27186 ns/op
PASS
```
总结：在以上描述的场景下，插入性能20000次每秒，查询性能36783次每秒