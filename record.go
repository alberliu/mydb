package mydb

import (
	"encoding/binary"
	"fmt"
)

type Record struct {
	SpaceLen   uint16
	Next       uint16
	Key        []byte
	Value      []byte
	Offset     uint16
	pageOffset uint64
}

func (r *Record) needSpaceLen() uint16 {
	return uint16(8) + uint16(len(r.Key)+len(r.Value))
}

func (r *Record) child() uint64 {
	return binary.BigEndian.Uint64(r.Value)
}

func (r *Record) display() {
	fmt.Printf("Next:%-5d Key:%-5s Value:%-5s    ", r.Next, string(r.Key), string(r.Value))
}

func (r *Record) String() string {
	return fmt.Sprintf("{Page:%-10d Next:%-4d Key:%-4s}",
		r.pageOffset, r.Next, string(r.Key))
}
