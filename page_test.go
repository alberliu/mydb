package mydb

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func Test_page_indexFlag2(t *testing.T) {
	page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
	pageType := page.pageType()
	if pageType != pageTypeBranch {
		t.Fatalf("index != 1, index:%d", pageType)
	}

	flag := flag2RecordBegin
	page._setIndexByFlag2(flag, 1)
	index := page._indexByFlag2(flag)
	if index != 1 {
		t.Fatalf("index != 1, index:%d", index)
	}
}

func Test_page_getSetFlag8(t *testing.T) {
	page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
	index := uint64(1000000)
	page.setParent(index)
	if page.parent() != index {
		t.Fatalf("index error, index:%d", index)
	}

	page.setPre(index)
	if page.pre() != index {
		t.Fatalf("index error, index:%d", index)
	}

	page.setNext(index)
	if page.next() != index {
		t.Fatalf("index error, index:%d", index)
	}
}

func Test_page_addIncrement(t *testing.T) {
	page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
	var want []*Record

	var offset uint16 = recordsDefaultBegin
	for i := 1; i < 100; i++ {
		buf := []byte(strconv.Itoa(i))
		page.add(buf, buf)

		r := &Record{Key: buf, Value: buf}
		want = appendRecord(want, r)

		offset += r.needSpaceLen()
	}

	got := page.all()
	sort.Slice(want, func(i, j int) bool {
		return bytes.Compare(want[i].Key, want[j].Value) < 0
	})

	if len(got) != len(want) {
		t.Fatalf("want:%d got:%d", len(want), len(got))
	}

	for i := range got {
		if bytes.Compare(got[i].Key, want[i].Key) != 0 || bytes.Compare(got[i].Value, want[i].Value) != 0 {
			t.Fatalf("%d \nwant:%v\n got:%v\n", i, want, got)
		}
	}
}

func Test_page_addRand(t *testing.T) {
	rand.Seed(time.Now().Unix())
	page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
	var want []*Record

	var offset uint16 = recordsDefaultBegin
	for i := 1; i < 100; i++ {
		buf := []byte(strconv.Itoa(rand.Intn(100)))
		page.add(buf, buf)

		r := &Record{Key: buf, Value: buf}

		want = appendRecord(want, r)

		offset += r.needSpaceLen()
	}

	got := page.all()
	sort.Slice(want, func(i, j int) bool {
		return bytes.Compare(want[i].Key, want[j].Value) < 0
	})

	if len(got) != len(want) {
		t.Fatalf("want:%d got:%d", len(want), len(got))
	}

	for i := range got {
		if bytes.Compare(got[i].Key, want[i].Key) != 0 || bytes.Compare(got[i].Value, want[i].Value) != 0 {
			t.Fatalf("%d \nwant:%v\n got:%v\n", i, want, got)
		}
	}
}

func Test_page_getFreeSpace(t *testing.T) {
	p := newPage(make([]byte, pageSize), 0, pageTypeLeaf)

	for i := 0; i < 450; i++ {
		spaceOffset, spaceLen, ok := p._getFreeSpace(10)
		if i < 403 {
			if spaceOffset != recordsDefaultBegin+uint16(i)*10 || spaceLen != 10 || !ok {
				t.Fatalf("spaceOffset error index:%d spaceOffset:%d spaceLen:%d ok:%v", i, spaceOffset, spaceLen, ok)
			}
		} else {
			if spaceOffset != 0 || spaceLen != 0 || ok {
				t.Fatalf("spaceOffset error index:%d spaceOffset:%d spaceLen:%d ok:%v", i, spaceOffset, spaceLen, ok)
			}
		}
	}
}

func Test_page_getRecycleSpace1(t *testing.T) {
	p := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
	offset := uint16(recordsDefaultBegin)
	offset, next := p._setSpace(offset, 12, 0)
	offset, next = p._setSpace(next, 11, offset)
	offset, next = p._setSpace(next, 10, offset)
	p._setIndexByFlag2(flag2RecycleBegin, offset)

	tests := []struct {
		name            string
		needSpaceLen    uint16
		wantSpaceOffset uint16
		wantSpaceLen    uint16
		wantOk          bool
	}{
		{
			needSpaceLen:    14,
			wantSpaceOffset: 0,
			wantSpaceLen:    0,
			wantOk:          false,
		},
		{
			needSpaceLen:    12,
			wantSpaceOffset: 64,
			wantSpaceLen:    12,
			wantOk:          true,
		},
		{
			needSpaceLen:    10,
			wantSpaceOffset: 87,
			wantSpaceLen:    10,
			wantOk:          true,
		},
		{
			needSpaceLen:    10,
			wantSpaceOffset: 76,
			wantSpaceLen:    11,
			wantOk:          true,
		},
		{
			needSpaceLen:    10,
			wantSpaceOffset: 0,
			wantSpaceLen:    0,
			wantOk:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSpaceOffset, gotSpaceLen, gotOk := p._getRecycleSpace(tt.needSpaceLen)
			if gotSpaceOffset != tt.wantSpaceOffset || gotSpaceLen != tt.wantSpaceLen || gotOk != tt.wantOk {
				t.Errorf("getRecycleSpace() gotSpaceOffset = %v, want %v gotSpaceLen = %v, want %v gotOk = %v, want %v",
					gotSpaceOffset, tt.wantSpaceOffset, gotSpaceLen, tt.wantSpaceLen, gotOk, tt.wantOk)
			}
		})
	}
}

func Test_page_update(t *testing.T) {
	page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)

	for i := 1; i < 10; i++ {
		buf := []byte(strconv.Itoa(i))
		page.add(buf, buf)
	}

	isCanUpdate, isEnoughSpace := page.update([]byte("1"), []byte("223"))
	if !isCanUpdate || !isEnoughSpace {
		t.Fatal()
	}
	value, ok := page.get([]byte("1"))
	if !ok || bytes.Compare(value, []byte("223")) != 0 {
		t.Fatal()
	}

	isCanUpdate, isEnoughSpace = page.update([]byte("101"), []byte("101"))
	if isCanUpdate {
		t.Fatal()
	}
}

func Test_page_delete(t *testing.T) {
	page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
	for i := 1; i < 10; i++ {
		buf := []byte(strconv.Itoa(i))
		page.add(buf, buf)
	}

	tests := []struct {
		name string
		key  []byte
		want bool
	}{
		{
			key:  []byte("1"),
			want: true,
		},
		{
			key:  []byte("9"),
			want: true,
		},
		{
			key:  []byte("1"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := page.delete(tt.key)
			_, ok := page.get(tt.key)
			if got != tt.want && !ok {
				t.Errorf("delete() = %v, want %v ok %v", got, tt.want, ok)
			}
		})
	}
}

func Test_page_min(t *testing.T) {
	page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
	for i := 1; i < 10; i++ {
		buf := []byte(strconv.Itoa(i))
		page.add(buf, buf)
	}

	key := page.min()
	if bytes.Compare(key, []byte("1")) != 0 {
		t.Fatal()
	}
}

func Test_page_updateMinKey(t *testing.T) {
	page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
	for i := 10; i < 11; i++ {
		buf := []byte(strconv.Itoa(i))
		fmt.Println("add", string(buf))
		page.add(buf, buf)
	}

	newMin := []byte("01")
	page.updateMinKey(newMin)

	key := page.min()
	if bytes.Compare(key, newMin) != 0 {
		t.Fatal()
	}
	page.display()
}

func Test_page_splitFront(t *testing.T) {
	var initSpitPage = func() *page {
		page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
		for i := 1; i < 9; i++ {
			buf := []byte(strings.Repeat(strconv.Itoa(i), 196))
			page.add(buf, buf)
		}
		return page
	}

	var args = []string{
		strings.Repeat("0", 11),
		strings.Repeat("6", 11),
		strings.Repeat("7", 11),
		strings.Repeat("8", 11),
	}
	for i := range args {
		page := initSpitPage()
		records := page.splitFront([]byte(args[i]), []byte(args[i]))
		fmt.Println("add---:", args[i])
		for _, v := range records {
			fmt.Println(v)
		}
		fmt.Println("---------------")
		page.display()
		fmt.Println("================================")
		fmt.Println()
	}
}

func Test_page_splitBehind(t *testing.T) {
	var initSpitPage = func() *page {
		page := newPage(make([]byte, pageSize), 0, pageTypeLeaf)
		for i := 1; i < 9; i++ {
			buf := []byte(strings.Repeat(strconv.Itoa(i), 196))
			page.add(buf, buf)
		}
		return page
	}

	var args = []string{
		strings.Repeat("0", 11),
		strings.Repeat("6", 11),
		strings.Repeat("7", 11),
		strings.Repeat("8", 11),
	}

	for i := range args {
		page := initSpitPage()
		records, isFront := page.splitBehind([]byte(args[i]), []byte(args[i]))
		fmt.Println("add---:", args[i], isFront)
		page.display()
		fmt.Println("---------------")
		for _, v := range records {
			fmt.Println(v)
		}
		fmt.Println("================================")
		fmt.Println()
	}
}
