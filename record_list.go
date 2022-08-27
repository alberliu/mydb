package mydb

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"strconv"
	"testing"
)

func toBytes(i int) []byte {
	return []byte(strconv.Itoa(i))
}

type recordList struct {
	list []*record
}

func newRecordList(list ...[]*record) *recordList {
	if len(list) > 0 {
		return &recordList{list: list[0]}
	}
	return &recordList{}
}

func (l *recordList) set(r *record) bool {
	index := sort.Search(len(l.list), func(i int) bool {
		return bytes.Compare(l.list[i].Key, r.Key) >= 0
	})
	// 不存在
	if index >= len(l.list) || !bytes.Equal(l.list[index].Key, r.Key) {
		l.list = append(l.list, r)
		for i := len(l.list) - 1; i > index; i-- {
			l.list[i] = l.list[i-1]
		}
		l.list[index] = r
		return true
	}

	l.list[index] = r
	return false
}

func (l *recordList) delete(key []byte) bool {
	index := sort.Search(len(l.list), func(i int) bool {
		return bytes.Compare(l.list[i].Key, key) >= 0
	})
	if index >= len(l.list) || !bytes.Equal(l.list[index].Key, key) {
		return false
	}

	l.list = append(l.list[:index], l.list[index+1:]...)
	return true
}

func (l *recordList) get(key []byte) *record {
	index := sort.Search(len(l.list), func(i int) bool {
		return bytes.Compare(l.list[i].Key, key) >= 0
	})
	if index >= len(l.list) || !bytes.Equal(l.list[index].Key, key) {
		return nil
	}
	return l.list[index]
}

func (l *recordList) isMatch(target []*record, r []byte) (bool, string) {
	if len(l.list) != len(target) {
		return false, fmt.Sprintf("len not match sourse:%d target:%d r:%s", len(l.list), len(target), r)
	}
	for i, v := range l.list {
		if !bytes.Equal(v.Key, target[i].Key) || !bytes.Equal(v.Value, target[i].Value) {
			return false, fmt.Sprintf("record not match len:%d i:%d l1:%v l2:%v r:%s", len(l.list), i, v, target[i], string(r))
		}
	}
	return true, ""
}

func (l *recordList) assertMatch(t *testing.T, target []*record, r []byte) {
	ok, errorMsg := l.isMatch(target, r)
	if !ok {
		t.Log("l", l.list)
		t.Log("t", target)
		t.Fatal(errorMsg)
	}
}

func isSorted(list []*record) bool {
	for i := 1; i < len(list); i++ {
		if bytes.Compare(list[i-1].Key, list[i].Key) > 0 {
			log.Println("isSorted", i, string(list[i-1].Key))
			return false
		}
	}
	return true
}

func appendToSortedRecords(l []*record, r *record) ([]*record, bool) {
	index := sort.Search(len(l), func(i int) bool {
		return bytes.Compare(l[i].Key, r.Key) >= 0
	})
	if index < len(l) && bytes.Equal(l[index].Key, r.Key) {
		return l, false
	}

	l = append(l, r)
	for i := len(l) - 1; i > index; i-- {
		l[i] = l[i-1]
	}
	l[index] = r
	return l, true
}
