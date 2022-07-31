package mydb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var Infinity = []byte("")

type record struct {
	Key        []byte
	Value      []byte
	spaceLen   uint16
	next       uint16
	offset     uint16
	pageOffset uint64
}

func (r *record) check() error {
	if r.needSpaceLen() >= recordMaxSize {
		return ErrRecordTooLarge
	}
	return nil
}

func (r *record) match(min, max []byte) bool {
	if bytes.Compare(min, Infinity) != 0 && bytes.Compare(min, r.Key) > 0 {
		return false
	}
	if bytes.Compare(max, Infinity) != 0 && bytes.Compare(max, r.Key) < 0 {
		return false
	}
	return true
}

func (r *record) needSpaceLen() uint16 {
	return uint16(8) + uint16(len(r.Key)+len(r.Value))
}

func (r *record) child() uint64 {
	return binary.BigEndian.Uint64(r.Value)
}

func (r *record) display() {
	fmt.Printf("Next:%-5d Key:%-5s Value:%-5s    ", r.next, string(r.Key), string(r.Value))
}

func (r *record) String() string {
	return fmt.Sprintf("{Key:%s:Value:%s}", string(r.Key), string(r.Value))
}
