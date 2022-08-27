package mydb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const recordsDefaultBegin = 64

const (
	pageTypeBranch  = 0 // 枝干
	pageTypeLeaf    = 1 // 叶子
	pageTypeRecycle = 2 // 被回收
)

const (
	byte2 = 2
	byte8 = 8
)

const (
	flag8Parent = 0  // 页父节点位置
	flag8Pre    = 8  // 页前置节点位置
	flag8Next   = 16 // 页后置节点位置

	flag2Type         = 32 // 页类型
	flag2RecordBegin  = 34 // 记录空间开始位置
	flag2RecycleBegin = 36 // 回收空间开始位置
	flag2FreeBegin    = 38 // 空闲空间开始位置
	flag2DirBegin     = 40 // 目录空间开始位置
)

type page struct {
	offset      uint64
	buf         []byte
	size        uint16
	minDirBegin uint16
}

func newPage(buf []byte, offset uint64, pageType uint16) *page {
	p := &page{
		offset: offset,
		buf:    buf,
		size:   uint16(len(buf)),
	}

	p.minDirBegin = (p.size - recordsDefaultBegin) / 2 / 8
	p._setIndexByFlag2(flag2Type, pageType)
	if p.size == 0 {
		panic("size == 0")
	}
	return p
}

func (p *page) display() {
	all := p.all()
	for i := range all {
		all[i].display()
		fmt.Println()
	}
}

func (p *page) pageType() uint16 {
	return p._indexByFlag2(flag2Type)
}

func (p *page) setPageType(pageType uint16) {
	p._setIndexByFlag2(flag2Type, pageType)
}

func (p *page) offsetBuf() []byte {
	buf := make([]byte, byte8)
	binary.BigEndian.PutUint64(buf, p.offset)
	return buf
}

func (p *page) parent() uint64 {
	return p._indexByFlag8(flag8Parent)
}

func (p *page) setParent(index uint64) {
	p._setIndexByFlag8(flag8Parent, index)
}

func (p *page) pre() uint64 {
	return p._indexByFlag8(flag8Pre)
}

func (p *page) setPre(index uint64) {
	p._setIndexByFlag8(flag8Pre, index)
}

func (p *page) next() uint64 {
	return p._indexByFlag8(flag8Next)
}

func (p *page) setNext(index uint64) {
	p._setIndexByFlag8(flag8Next, index)
}

func (p *page) get(key []byte) ([]byte, bool) {
	_, r := p.find(key)
	if r == nil || !bytes.Equal(r.Key, key) {
		return nil, false
	}

	return r.Value, true
}

// set 设置
// isNew 是否是新纪录
// isEnoughSpace 是否空间足够
func (p *page) set(key, value []byte) (isNew bool, isEnoughSpace bool) {
	isEnoughSpace = true
	dir, current := p.find(key)

	// record存在且原地址空间符合，直接更新
	isNew = current == nil || !bytes.Equal(current.Key, key)
	if !isNew {
		new := *current
		new.Value = value
		if new.needSpaceLen() <= current.spaceLen {
			p._setRecord(&new)
			isEnoughSpace = true
			return
		}

		p._deleteRecord(*current)

		p._dirDelete(*dir, *current)
		// 这里可能删除目录，或者目录产生移位，还可能目录合并，需要重新获取
		dir = p._dirFind(key)
	}

	// 原地址空间不符合或者记录不存在，需要添加
	// 找到添加pre位置
	preOffset := uint16(0)
	if current != nil {
		if isNew {
			preOffset = current.offset
		} else {
			preOffset = current.pre
		}
	}

	new := &record{Key: key, Value: value}
	offset, spaceLen, ok := p._getSpace(new.needSpaceLen())
	if !ok {
		// 这里存在更新的时候，空间不足的情况
		isEnoughSpace = false
		return
	}
	new.offset = offset
	new.spaceLen = spaceLen
	if preOffset == 0 {
		recordBegin := p._indexByFlag2(flag2RecordBegin)
		p._setIndexByFlag2(flag2RecordBegin, new.offset)

		new.pre = 0
		new.next = recordBegin
		p._setRecord(new)

		if recordBegin != 0 {
			next := p._record(recordBegin)
			next.pre = new.offset
			p._setRecord(next)
		}
	} else {
		pre := p._record(preOffset)
		preNext := pre.next
		pre.next = new.offset
		p._setRecord(pre)

		new.pre = pre.offset
		new.next = preNext
		p._setRecord(new)

		if preNext != 0 {
			next := p._record(preNext)
			next.pre = new.offset
			p._setRecord(next)
		}
	}

	p._dirAdd(dir, new)
	return
}

func (p *page) delete(key []byte) bool {
	d, r := p.find(key)
	if r == nil || !bytes.Equal(r.Key, key) {
		return false
	}

	p._deleteRecord(*r)
	p._dirDelete(*d, *r)
	return true
}

func (p *page) min() []byte {
	beginIndex := p._indexByFlag2(flag2RecordBegin)
	if beginIndex == 0 {
		return []byte("")
	}
	record := p._record(beginIndex)
	return record.Key
}

func (p *page) updateMinKey(key []byte) bool {
	min := p.min()
	value, _ := p.get(min)
	p.delete(min)
	_, isEnoughSpace := p.set(key, value)
	return isEnoughSpace
}

func (p *page) isNil() bool {
	return p._indexByFlag2(flag2RecordBegin) == 0
}

// splitFront 溢出前面record
func (p *page) splitFront(key, value []byte) []*record {
	all := p.all()
	all, _ = appendToSortedRecords(all, &record{Key: key, Value: value})

	p._reset()

	var useSpace uint16 = 0
	overflow := make([]*record, 0, 10)
	recordMaxSize := p._recordMaxSize()
	for i := range all {
		// i != len(all) 这里要保证，p不是一个空页
		if useSpace < recordMaxSize && i != len(all)-1 {
			overflow = append(overflow, all[i])
			useSpace += all[i].needSpaceLen()
		} else {
			p.set(all[i].Key, all[i].Value)
		}
	}
	return overflow
}

// splitBehind 分裂节点，溢出后面record
// first return 溢出的记录
// second return 新插入的记录位置是否在前置节点
func (p *page) splitBehind(key, value []byte) ([]*record, bool) {
	all := p.all()
	all, _ = appendToSortedRecords(all, &record{Key: key, Value: value})

	p._reset()

	var useSpace uint16 = 0
	overflow := make([]*record, 0, 10)
	recordMaxSize := p._recordMaxSize()
	for i := range all {
		if useSpace < recordMaxSize {
			p.set(all[i].Key, all[i].Value)
			useSpace += all[i].needSpaceLen()
		} else {
			overflow = append(overflow, all[i])
		}
	}

	isFront := true
	if len(overflow) > 0 && bytes.Compare(key, overflow[0].Key) >= 0 {
		isFront = false
	}
	return overflow, isFront
}

func (p *page) query(min, max []byte) []*record {
	offset := p._indexByFlag2(flag2RecordBegin)
	if offset == 0 {
		return nil
	}

	records := make([]*record, 0, 10)
	for {
		record := p._record(offset)
		if record.match(min, max) {
			records = append(records, record)
		}

		offset = record.next
		if offset == 0 {
			break
		}
	}
	return records
}

// preRecord 查找key所在的pre record 和 current record
// 页为空或注射小于所有元素     return == nil
// 中间                      return != nil return.key<=key
// 大于所有元素               返回最后一个元素
func (p *page) find(key []byte) (*dir, *record) {
	dir := p._dirFind(key)
	if dir == nil {
		return nil, nil
	}

	offset := dir.recordOffset
	r := p._record(offset)
	if bytes.Compare(r.Key, key) > 0 {
		return dir, nil
	}

	for {
		if r.next == 0 {
			return dir, r
		}
		next := p._record(r.next)
		if bytes.Compare(next.Key, key) > 0 {
			return dir, r
		}
		r = next
	}
}

func (p *page) all() []*record {
	offset := p._indexByFlag2(flag2RecordBegin)
	if offset == 0 {
		return nil
	}

	list := make([]*record, 0, 10)
	for {
		record := p._record(offset)
		list = append(list, record)

		offset = record.next
		if offset == 0 {
			break
		}
	}
	return list
}

func (p *page) count() int {
	offset := p._indexByFlag2(flag2RecordBegin)
	if offset == 0 {
		return 0
	}

	var num = 0
	for {
		record := p._record(offset)
		num++

		offset = record.next
		if offset == 0 {
			break
		}
	}
	return num
}

func (p *page) _indexByFlag2(flag int) uint16 {
	return binary.BigEndian.Uint16(p.buf[flag : flag+byte2])
}

func (p *page) _setIndexByFlag2(flag int, value uint16) {
	binary.BigEndian.PutUint16(p.buf[flag:flag+byte2], value)
}

func (p *page) _indexByFlag8(flag int) uint64 {
	return binary.BigEndian.Uint64(p.buf[flag : flag+byte8])
}

func (p *page) _setIndexByFlag8(flag int, value uint64) {
	binary.BigEndian.PutUint64(p.buf[flag:flag+byte8], value)
}

func (p *page) _recordMaxSize() uint16 {
	return recordMaxSize(p.size)
}

func (p *page) _reset() {
	p._setIndexByFlag2(flag2RecordBegin, 0)
	p._setIndexByFlag2(flag2RecycleBegin, 0)
	p._setIndexByFlag2(flag2FreeBegin, 0)
	p._setIndexByFlag2(flag2DirBegin, 0)
}

// _recycle 回收record空间
func (p *page) _recycle(record *record) {
	recycleBegin := p._indexByFlag2(flag2RecycleBegin)
	record.next = recycleBegin
	p._setRecord(record)
	p._setIndexByFlag2(flag2RecycleBegin, record.offset)
}

func (p *page) _setSpace(offset, spaceLen, nextIndex uint16) (uint16, uint16) {
	binary.BigEndian.PutUint16(p.buf[offset:], spaceLen)
	binary.BigEndian.PutUint16(p.buf[offset+2:], nextIndex)
	return offset, offset + spaceLen
}

func (p *page) _getSpace(needSpaceLen uint16) (spaceOffset uint16, spaceLen uint16, ok bool) {
	// 从回收空间获取
	if spaceOffset, spaceLen, ok = p._getRecycleSpace(needSpaceLen); ok {
		return
	}

	// 熊空闲空间获取
	if spaceOffset, spaceLen, ok = p._getFreeSpace(needSpaceLen); ok {
		return
	}

	ok = false
	return
}

func (p *page) _getRecycleSpace(needSpaceLen uint16) (spaceOffset uint16, spaceLen uint16, ok bool) {
	recycleBegin := p._indexByFlag2(flag2RecycleBegin)
	if recycleBegin == 0 {
		return 0, 0, false
	}

	var (
		preOffset uint16 = 0
		nextIndex uint16 = 0
	)

	spaceOffset = recycleBegin
	for {
		// 读取spaceLen
		spaceLen = binary.BigEndian.Uint16(p.buf[spaceOffset:])
		// 读取nextIndex
		nextIndex = binary.BigEndian.Uint16(p.buf[spaceOffset+4:])

		if spaceLen >= needSpaceLen {
			break
		}
		if nextIndex == 0 {
			return 0, 0, false
		}

		preOffset = spaceOffset
		spaceOffset = nextIndex
	}

	// 是第一个空闲空间
	if preOffset == 0 {
		p._setIndexByFlag2(flag2RecycleBegin, nextIndex)
		ok = true
		return
	}
	binary.BigEndian.PutUint16(p.buf[preOffset+4:], nextIndex)
	ok = true
	return
}

func (p *page) _getFreeSpace(needSpaceLen uint16) (spaceOffset uint16, spaceLen uint16, ok bool) {
	freeBegin := p._indexByFlag2(flag2FreeBegin)
	if freeBegin == 0 {
		freeBegin = recordsDefaultBegin
	}

	dirBegin := p._indexByFlag2(flag2DirBegin)
	if dirBegin == 0 {
		dirBegin = p.size
	}

	// 剩余空闲空间检查
	if dirBegin-freeBegin < needSpaceLen {
		ok = false
		return
	}

	p._setIndexByFlag2(flag2FreeBegin, freeBegin+needSpaceLen)
	return freeBegin, needSpaceLen, true
}

// _setRecord 在指定偏移量上设置记录
func (p *page) _setRecord(record *record) {
	offset := record.offset
	// 设置spaceLen
	binary.BigEndian.PutUint16(p.buf[offset:], record.spaceLen)
	// 设置pre
	offset += 2
	binary.BigEndian.PutUint16(p.buf[offset:], record.pre)
	// 设置next
	offset += 2
	binary.BigEndian.PutUint16(p.buf[offset:], record.next)
	// 设置keyLen
	offset += 2
	binary.BigEndian.PutUint16(p.buf[offset:], uint16(len(record.Key)))
	// 设置valueLen
	offset += 2
	binary.BigEndian.PutUint16(p.buf[offset:], uint16(len(record.Value)))
	// 设置key
	offset += 2
	copy(p.buf[offset:], record.Key)
	// 设置value
	offset += uint16(len(record.Key))
	copy(p.buf[offset:], record.Value)
}

// _record 在指定偏移位置
func (p *page) _record(offset uint16) *record {
	var record record
	record.offset = offset

	// 读取spaceLen
	record.spaceLen = binary.BigEndian.Uint16(p.buf[offset:])
	// 读取pre
	offset += 2
	record.pre = binary.BigEndian.Uint16(p.buf[offset:])
	// 读取next
	offset += 2
	record.next = binary.BigEndian.Uint16(p.buf[offset:])
	// 读取keyLen
	offset += 2
	keyLen := binary.BigEndian.Uint16(p.buf[offset:])
	// 读取valueLen
	offset += 2
	valueLen := binary.BigEndian.Uint16(p.buf[offset:])
	// 读取key
	offset += 2
	record.Key = make([]byte, keyLen)
	copy(record.Key, p.buf[offset:offset+keyLen])
	// 读取value
	offset += keyLen
	record.Value = make([]byte, valueLen)
	copy(record.Value, p.buf[offset:offset+valueLen])

	record.pageOffset = p.offset
	return &record
}

func (p *page) _deleteRecord(r record) {
	if r.pre != 0 {
		pre := p._record(r.pre)
		pre.next = r.next
		p._setRecord(pre)
	} else {
		p._setIndexByFlag2(flag2RecordBegin, r.next)
	}

	if r.next != 0 {
		next := p._record(r.next)
		next.pre = r.pre
		p._setRecord(next)
	}

	p._recycle(&r)
}
