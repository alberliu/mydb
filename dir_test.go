package mydb

import (
	"fmt"
	"testing"
)

func Test_dir_needSpaceLen(t *testing.T) {
	d := dir{
		offset:       defaultPageSize,
		recordOffset: 1,
		recordNum:    uint16(1),
		key:          toBytes(111),
	}
	if d.needSpaceLen() != 9 {
		t.Fatal()
	}
}

func newDirPage(d ...*dir) *page {
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
	offset := uint16(defaultPageSize)
	for _, v := range d {
		v.offset = offset
		page._dirSet(v)

		offset -= v.needSpaceLen()
		page._setIndexByFlag2(flag2DirBegin, offset)
	}
	return page
}

func TestNewDirPage(t *testing.T) {
	page := newDirPage(
		&dir{recordOffset: 1, recordNum: 1, key: toBytes(1)},
		&dir{recordOffset: 2, recordNum: 2, key: toBytes(2)},
		&dir{recordOffset: 3, recordNum: 3, key: toBytes(3)},
	)
	fmt.Println(page._dirAll())
}

func Test_page__dirAdd(t *testing.T) {
	// 空目录
	page := newPage(make([]byte, defaultPageSize), 0, pageTypeBranch)
	page._dirAdd(nil, &record{offset: uint16(1), Key: toBytes(1)})
	if page._dirFind(toBytes(1)).recordNum != 1 {
		t.Fatal()
	}

	// 目录累加
	page = newDirPage(&dir{recordOffset: 1, recordNum: 1, key: toBytes(1)})
	page._dirAdd(page._dirFind(toBytes(1)), nil)
	if page._dirFind(toBytes(1)).recordNum != 2 {
		t.Fatal()
	}

	// 越界测试
	page = newDirPage(
		&dir{recordOffset: 1, recordNum: 7, key: toBytes(1)},
		&dir{recordOffset: 3, recordNum: 1, key: toBytes(3)},
	)
	page._setIndexByFlag2(flag2FreeBegin, 4096-14)
	page._dirAdd(page._dirFind(toBytes(2)), &record{offset: 2, Key: toBytes(21)})
	d := page._dirFind(toBytes(1))
	if !d.equal(&dir{offset: page.size, recordOffset: 1, recordNum: 8, key: toBytes(1)}) {
		t.Fatal(d)
	}

	// 目录分裂
	recordByIndexIsMock = true
	page = newDirPage(
		&dir{recordOffset: 1, recordNum: 7, key: toBytes(1)},
		&dir{recordOffset: 3, recordNum: 1, key: toBytes(3)},
	)
	page._dirAdd(page._dirFind(toBytes(2)), &record{offset: 2, Key: toBytes(21)})
	d = page._dirFind(toBytes(1))
	if !d.equal(&dir{offset: 4096, recordOffset: 1, recordNum: 4, key: toBytes(1)}) {
		t.Fatal(d)
	}
	d = page._dirFind(toBytes(2))
	if !d.equal(&dir{offset: 4087, recordOffset: 2, recordNum: 5, key: toBytes(2)}) {
		t.Fatal(d)
	}
}

func Test_page__dirDelete(t *testing.T) {
	page := newDirPage(
		&dir{recordOffset: 1, recordNum: 1, key: toBytes(1)},
		&dir{recordOffset: 2, recordNum: 2, key: toBytes(2)},
		&dir{recordOffset: 3, recordNum: 1, key: toBytes(3)},
	)

	// 最后一个元素
	page._dirDelete(*page._dirFind(toBytes(3)), record{})
	if l := len(page._dirAll()); l != 2 {
		t.Fatal(l)
	}

	page = newDirPage(
		&dir{recordOffset: 1, recordNum: 1, key: toBytes(1)},
		&dir{recordOffset: 2, recordNum: 2, key: toBytes(2)},
		&dir{recordOffset: 3, recordNum: 3, key: toBytes(3)},
	)
	page._dirDelete(*page._dirFind(toBytes(3)), record{})
	d := page._dirFind(toBytes(3))
	if !d.equal(&dir{offset: 4082, recordOffset: 3, recordNum: 2, key: toBytes(3)}) {
		t.Fatal(d)
	}

	page = newDirPage(
		&dir{recordOffset: 1, recordNum: 1, key: toBytes(1)},
		&dir{recordOffset: 2, recordNum: 8, key: toBytes(2)},
		&dir{recordOffset: 3, recordNum: 3, key: toBytes(3)},
	)
	page._dirDelete(*page._dirFind(toBytes(2)), record{})
	d = page._dirFind(toBytes(2))
	if !d.equal(&dir{offset: 4089, recordOffset: 2, recordNum: 7, key: toBytes(2)}) {
		t.Fatal(d)
	}

	page = newDirPage(
		&dir{recordOffset: 1, recordNum: 1, key: toBytes(1)},
		&dir{recordOffset: 2, recordNum: 2, key: toBytes(2)},
		&dir{recordOffset: 3, recordNum: 3, key: toBytes(3)},
	)
	page._dirDelete(*page._dirFind(toBytes(2)), record{})
	d = page._dirFind(toBytes(2))
	if !d.equal(&dir{offset: 4089, recordOffset: 2, recordNum: 4, key: toBytes(2)}) {
		t.Fatal(d)
	}
}

func Test_page__dirPre(t *testing.T) {
	page := newDirPage(
		&dir{recordOffset: 1, recordNum: 1, key: toBytes(1)},
		&dir{recordOffset: 2, recordNum: 2, key: toBytes(2)},
		&dir{recordOffset: 3, recordNum: 3, key: toBytes(3)},
	)

	d := page._dirFind(toBytes(1))
	pre := page._dirPre(d.offset)
	if pre != nil {
		t.Fatal(pre)
	}

	d = page._dirFind(toBytes(2))
	pre = page._dirPre(d.offset)
	if !pre.equal(&dir{offset: page.size, recordOffset: 1, recordNum: 1, key: toBytes(1)}) {
		t.Fatal(pre)
	}
}

func Test_page__dirRebuild(t *testing.T) {
	var initPage = func(num int) *page {
		page := newPage(make([]byte, defaultPageSize), 0, pageTypeLeaf)
		for i := 0; i < num; i++ {
			buf := []byte(fmt.Sprintf("%03d", i))
			page.set(buf, buf)
		}
		return page
	}

	tests := []struct {
		name      string
		rnum      int
		freeBegin uint16
		dnum      int
	}{
		{
			rnum:      1,
			freeBegin: 0,
		},
		{
			rnum:      10,
			freeBegin: 0,
		},
		{
			rnum:      20,
			freeBegin: 0,
		},
		{
			rnum:      20,
			freeBegin: 4073,
			dnum:      2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := initPage(tt.rnum)
			p._setIndexByFlag2(flag2FreeBegin, tt.freeBegin)
			p._dirRebuild()

			dirs := p._dirAll()

			totalLen := uint16(0)
			for i := range dirs {
				totalLen += dirs[i].needSpaceLen()
			}
			dirBegin := p._indexByFlag2(flag2DirBegin)

			t.Log(dirs, dirBegin, totalLen, p.size-dirBegin)

			dnum := tt.rnum/dirRecordMaxNum + 1
			if tt.dnum != 0 {
				dnum = tt.dnum
			}
			if len(dirs) != dnum {
				t.Fatal()
			}
		})
	}
}

func Test_page__dirDelRecordNum(t *testing.T) {
	var initPage = func() *page {
		return newDirPage(
			&dir{recordOffset: 1, recordNum: 1, key: toBytes(1)},
			&dir{recordOffset: 3, recordNum: 3, key: toBytes(3)},
			&dir{recordOffset: 5, recordNum: 5, key: toBytes(5)},
		)
	}

	// 移除目录
	page := initPage()
	dir := page._dirFind(toBytes(1))
	dirDelRecordNumIsMock = true
	page._dirDelRecordNum(*dir, record{offset: 1, Key: toBytes(1)})
	t.Log(page._dirAll())

	// 减掉目录的记录数
	page = initPage()
	dir = page._dirFind(toBytes(4))
	dirDelRecordNumIsMock = true
	page._dirDelRecordNum(*dir, record{offset: 4, Key: toBytes(4)})
	t.Log(page._dirAll())

	// d.key == r.key
	page = initPage()
	dir = page._dirFind(toBytes(4))
	dirDelRecordNumIsMock = true
	page._dirDelRecordNum(*dir, record{offset: 3, Key: toBytes(3)})
	t.Log(page._dirAll())
}
