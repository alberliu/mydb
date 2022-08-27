package mydb

import (
	"log"
	"os"
	"testing"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

func TestOpen(t *testing.T) {
	os.Remove("data")
	db, err := Open("data")
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		_, _ = db.Set(toBytes(i), toBytes(i))
	}
	log.Println("set:  ", db.Range(Infinity, Infinity))

	_ = db.Delete(toBytes(1))
	log.Println("delete", db.Range(Infinity, Infinity))

	log.Println("range ", db.Range(toBytes(3), toBytes(4)))
}
