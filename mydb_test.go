package mydb

import (
	"fmt"
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	os.Remove("data")
	db, err := Open("data")
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		_ = db.Set(toBytes(i), toBytes(i))
	}
	fmt.Println("add:  ", db.Range(Infinity, Infinity))

	_ = db.Delete(toBytes(1))
	fmt.Println("delete", db.Range(Infinity, Infinity))

	fmt.Println("range ", db.Range(toBytes(3), toBytes(4)))
}
