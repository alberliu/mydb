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
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	pageType := page.pageType()
	if pageType != pageTypeLeaf {
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
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
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

func Test_page_set(t *testing.T) {
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)

	for i := 1; i < 10; i++ {
		buf := []byte(strconv.Itoa(i))
		page.set(buf, buf)
		//t.Log(i, page.all())
	}

	isNew, isEnoughSpace := page.set([]byte("1"), []byte("223"))
	if isNew || !isEnoughSpace {
		t.Fatalf("isNew:%v,isEnoughSpace:%v", isNew, isEnoughSpace)
	}
	t.Log(page.all())
	t.Log(page._dirAll())
	value, ok := page.get([]byte("1"))
	if !ok || !bytes.Equal(value, []byte("223")) {
		t.Fatalf("value:%s ok:%v\n", string(value), ok)
	}

	isNew, _ = page.set([]byte("101"), []byte("101"))
	if !isNew {
		t.Fatal()
	}
	value, ok = page.get([]byte("101"))
	if !ok || !bytes.Equal(value, []byte("101")) {
		t.Fatalf("value:%s ok:%v\n", string(value), ok)
	}
}

func Test_page_set_rand(t *testing.T) {
	rand.Seed(time.Now().Unix())

	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	mock := newRecordList()

	seed := time.Now().Unix()
	rand.Seed(seed)
	t.Log("seed:", seed)
	for i := 1; i < 1000; i++ {
		buf := []byte(strconv.Itoa(rand.Intn(1000)))

		isNew, isEnoughSpace := page.set(buf, buf)
		if isEnoughSpace {
			mockIsNew := mock.set(&record{Key: buf, Value: buf})
			if isNew != mockIsNew {
				t.Fatal()
			}
		}

		mock.assertMatch(t, page.all(), buf)
	}
}

func Test_page_set_increment(t *testing.T) {
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	mock := newRecordList()

	for i := 0; i < 100; i++ {
		if i == 19 {
			fmt.Println("")
		}

		buf := []byte(fmt.Sprintf("%03d", i))
		page.set(buf, buf)

		mock.set(&record{Key: buf, Value: buf})

		allDir := page._dirAll()
		dirIsSort := sort.SliceIsSorted(allDir, func(i, j int) bool {
			return bytes.Compare(allDir[i].key, allDir[j].key) < 0
		})
		//if !dirIsSort {
		t.Log("add:", i)
		t.Log(dirIsSort)
		t.Log(allDir)
		//}

		mock.assertMatch(t, page.all(), toBytes(i))
	}

	mock.assertMatch(t, page.all(), nil)
}

func Test_page_getFreeSpace(t *testing.T) {
	p := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)

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
	p := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
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

func Test_page_delete(t *testing.T) {
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	for i := 1; i < 10; i++ {
		buf := []byte(strconv.Itoa(i))
		page.set(buf, buf)
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
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	for i := 1; i < 10; i++ {
		buf := []byte(strconv.Itoa(i))
		page.set(buf, buf)
	}

	key := page.min()
	if !bytes.Equal(key, []byte("1")) {
		t.Fatal()
	}
}

func Test_page_updateMinKey(t *testing.T) {
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	for i := 10; i < 20; i++ {
		buf := []byte(strconv.Itoa(i))
		page.set(buf, buf)
	}

	newMin := []byte("01")
	page.updateMinKey(newMin)

	key := page.min()
	if !bytes.Equal(key, newMin) {
		t.Fatal()
	}
}

func Test_page_splitFront(t *testing.T) {
	var initSpitPage = func() *page {
		page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
		for i := 1; i < 9; i++ {
			buf := []byte(strings.Repeat(strconv.Itoa(i), 196))
			page.set(buf, buf)
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
		if !isSorted(records) || !isSorted(page.all()) {
			t.Fatalf("is not sorted, args:%s", args[i])
		}
	}
}

func Test_page_splitBehind(t *testing.T) {
	var initSpitPage = func() *page {
		page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
		for i := 1; i < 9; i++ {
			buf := []byte(strings.Repeat(strconv.Itoa(i), 196))
			page.set(buf, buf)
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
		if !isSorted(records) || !isSorted(page.all()) {
			t.Fatalf("is not sorted, args:%s isFront:%v", args[i], isFront)
		}
	}
}

func Test_page_query(t *testing.T) {
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	for i := 1; i < 10; i++ {
		buf := []byte(strconv.Itoa(i))
		page.set(buf, buf)
	}

	type args struct {
		min []byte
		max []byte
	}
	tests := []struct {
		name string
		args args
		want []*record
	}{
		{
			args: args{min: toBytes(0), max: toBytes(2)},
			want: []*record{
				{Key: toBytes(1), Value: toBytes(1)},
				{Key: toBytes(2), Value: toBytes(2)},
			},
		},
		{
			args: args{min: toBytes(1), max: toBytes(2)},
			want: []*record{
				{Key: toBytes(1), Value: toBytes(1)},
				{Key: toBytes(2), Value: toBytes(2)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gots := page.query(tt.args.min, tt.args.max)
			if !isEqualRecords(gots, tt.want) {
				t.Fatal(string(tt.args.min), string(tt.args.max), gots)
			}
		})
	}
}

func Test_page_count(t *testing.T) {
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	for i := 1; i < 10; i++ {
		buf := []byte(strconv.Itoa(i))
		page.set(buf, buf)
	}

	if num := page.count(); num != 9 {
		t.Fatal(num)
	}
}

func Test_page_complex_rand_repeat(t *testing.T) {
	for {
		Test_page_complex_rand(t)
	}
}

// Test_page_complex_rand page随机测试
// bug seed 1661320114471
// 3628 3608
func Test_page_complex_rand(t *testing.T) {
	seed := time.Now().UnixMilli()
	rand.Seed(seed)
	t.Log("seed:", seed)

	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	mock := newRecordList()
	for i := 1; i < 10000; i++ {
		key := toBytes(rand.Intn(10000))
		switch rand.Intn(2) {
		case 0:
			value := toBytes(rand.Intn(10000))
			//if bytes.Equal(key, toBytes(9949)) {
			//t.Log(string(key), string(value))
			//}
			isNew, isEnoughSpace := page.set(key, value)
			if isEnoughSpace {
				mockIsNew := mock.set(&record{Key: key, Value: value})
				if isNew != mockIsNew {
					t.Fatal()
				}
			} else {
				if !isNew {
					mock.delete(key)
				}
			}
		case 1:
			//if bytes.Equal(key, toBytes(0)) {
			//	t.Log(string(key))
			//}
			ok := page.delete(key)
			mockok := mock.delete(key)
			if ok != mockok {
				t.Fatal()
			}
		}

		all := page.all()
		ok, errorMsg := mock.isMatch(all, key)
		if !ok {
			t.Log(page._dirAll())
			t.Log("l", mock.list)
			t.Log("t", all)
			t.Fatal(errorMsg)
		}
	}
}
