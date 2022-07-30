package mydb

import (
	"fmt"
	"testing"
)

func TestNewQueue(t *testing.T) {
	q := NewQueue[int]()
	fmt.Println(q.Pop())

	var a1 = 1
	q.Push(&a1)
	var a2 = 2
	q.Push(&a2)

	fmt.Println(*q.Pop())
	fmt.Println(*q.Pop())
	fmt.Println(*q.Pop())
}
