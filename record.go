package mydb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var Infinity = []byte("")

/**
record 物理存储结构
spaceLen 空间大小
pre      上一个记录位置
next     下一个记录位置
keyLen   key的长度
valueLen value的长度
key      key
value    value
*/

// record 记录
type record struct {
	Key        []byte
	Value      []byte
	spaceLen   uint16
	pre        uint16
	next       uint16
	offset     uint16
	pageOffset uint64
}

func recordMaxSize(pageSize uint16) uint16 {
	return (pageSize - recordsDefaultBegin) / 2
}

func (r *record) match(min, max []byte) bool {
	if !bytes.Equal(min, Infinity) && bytes.Compare(min, r.Key) > 0 {
		return false
	}
	if !bytes.Equal(max, Infinity) && bytes.Compare(max, r.Key) < 0 {
		return false
	}
	return true
}

func (r *record) needSpaceLen() uint16 {
	return uint16(10) + uint16(len(r.Key)+len(r.Value))
}

func (r *record) child() uint64 {
	return binary.BigEndian.Uint64(r.Value)
}

func (r *record) display() {
	fmt.Printf("Next:%-5d Key:%-5s Value:%-5s    ", r.next, string(r.Key), string(r.Value))
}

func (r *record) String() string {
	return fmt.Sprintf("{Key:%s,Value:%s,next%4d %4d}", string(r.Key), string(r.Value), r.next, r.offset)
}
