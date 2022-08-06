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
