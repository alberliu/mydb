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

func isMatch(l1, l2 []*Record, r []byte) (ok bool, str string) {
	if len(l1) != len(l2) {
		ok = false
		str = fmt.Sprintf("len not match l1:%d l2:%d r:%s", len(l1), len(l2), r)
		return
	}
	for i := range l1 {
		if bytes.Compare(l1[i].Key, l2[i].Key) != 0 || bytes.Compare(l1[i].Value, l2[i].Value) != 0 {
			//t.Log(l1[i-10 : i+10])
			//t.Log(l2[i-10 : i+10])
			ok = false
			str = fmt.Sprintf("not match len:%d i:%d l1:%v l2:%v r:%s", len(l1), i, l1[i], l2[i], string(r))
			return
		}
	}
	return true, ""
}

func assertMatch(t *testing.T, l1, l2 []*Record, r []byte) {
	if len(l1) != len(l2) {
		t.Fatalf("len not match l1:%d l2:%d r:%s", len(l1), len(l2), r)
	}
	for i := range l1 {
		if bytes.Compare(l1[i].Key, l2[i].Key) != 0 || bytes.Compare(l1[i].Value, l2[i].Value) != 0 {
			t.Fatalf("not match len:%d i:%d l1:%v l2:%v r:%s", len(l1), i, l1[i], l2[i], string(r))
		}
	}
}

func isSorted(list []*Record) bool {
	for i := 1; i < len(list); i++ {
		if bytes.Compare(list[i-1].Key, list[i].Key) > 0 {
			log.Println("isSorted", i, string(list[i-1].Key))
			return false
		}
	}
	return true
}

func appendRecord(l []*Record, r *Record) []*Record {
	for i := range l {
		if bytes.Compare(l[i].Key, r.Key) == 0 {
			return l
		}
	}
	return append(l, r)
}

func sortRecords(l []*Record) {
	sort.Slice(l, func(i, j int) bool {
		return bytes.Compare(l[i].Key, l[j].Key) < 0
	})
}

func appendToSortedRecords(l []*Record, r *Record) []*Record {
	l = append(l, r)

	index := len(l) - 1
	for i := range l {
		if bytes.Compare(l[i].Key, r.Key) > 0 {
			index = i
			break
		}
	}

	for i := len(l) - 1; i > index; i-- {
		l[i] = l[i-1]
	}
	l[index] = r
	return l
}

func appendRecordWithSort(l []*Record, r *Record) []*Record {
	for i := range l {
		if bytes.Compare(l[i].Key, r.Key) == 0 {
			return l
		}
	}
	l = append(l, r)
	sort.Slice(l, func(i, j int) bool {
		return bytes.Compare(l[i].Key, l[j].Key) < 0
	})
	return l
}
