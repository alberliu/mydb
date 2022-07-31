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
