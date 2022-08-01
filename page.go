package mydb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	pageSize            = 4096
	recordsDefaultBegin = 64
	recordsSpaceSize    = pageSize - recordsDefaultBegin
	recordMaxSize       = recordsSpaceSize / 2
)

const (
	pageTypeBranch  = 0 // 枝干
	pageTypeLeaf    = 1 // 叶子
	pageTypeRecycle = 2 // 被回收
)

const (
	byte2 = 2
	byte8 = 8
)

// 回收
const (
	flag8Parent = 0  // 页父节点位置
	flag8Pre    = 8  // 页前置节点位置
	flag8Next   = 16 // 页后置节点位置

	flag2Type         = 32 // 页类型
	flag2RecordBegin  = 34 // 记录开始位置
	flag2RecycleBegin = 36 // 回收空间开始位置
	flag2FreeBegin    = 38 // 空闲空间开始位置
)

type page struct {
	offset uint64
	buf    []byte
}

func newPage(buf []byte, offset uint64, t uint16) *page {
	p := &page{offset: offset, buf: buf}
	p._setIndexByFlag2(flag2Type, t)
	return p
}

func (p *page) display() {
	all := p.all()
	for i := range all {
		all[i].display()
		fmt.Println()
	}
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
	_, current := p.currentRecord(key)
	if current == nil || bytes.Compare(current.Key, key) != 0 {
		return nil, false
	}

	return current.Value, true
}

// add 添加
// isUnique 是否唯一，当page中不存在相等key,返回true
// isEnoughSpace 是否有足够空间，当page中有足够空间返回true
func (p *page) add(key, value []byte) (isUnique bool, isEnoughSpace bool) {
	r := &record{Key: key, Value: value}

	beginIndex := p._indexByFlag2(flag2RecordBegin)
	// 是一个空page
	if beginIndex == 0 {
		offset, spaceLen, _ := p._getSpace(r.needSpaceLen())
		r.spaceLen = spaceLen
		r.offset = offset
		p._setRecordOnOffset(r)
		p._setIndexByFlag2(flag2RecordBegin, offset)
		return true, true
	}

	// 找到需要插入的位置
	pre, current := p.currentRecord(key)
	if current != nil && bytes.Equal(current.Key, r.Key) {
		return false, true
	}

	// 获取空闲空间
	freeSpaceOffset, spaceLen, ok := p._getSpace(r.needSpaceLen())
	r.spaceLen = spaceLen
	r.offset = freeSpaceOffset
	if !ok {
		return true, false
	}

	// 这里处理两种情况. preRecord 为空或者不为空
	if pre == nil {
		r.next = beginIndex
		// 写入到空闲空间
		p._setRecordOnOffset(r)
		p._setIndexByFlag2(flag2RecordBegin, freeSpaceOffset)
	} else {
		r.next = pre.next
		// 写入到空闲空间
		p._setRecordOnOffset(r)
		// 更新preRecord的next
		binary.BigEndian.PutUint16(p.buf[pre.offset+2:], freeSpaceOffset)
	}
	return true, true
}

// update 更新
// isExist 是否存在
// isEnoughSpace 是否空间足够
func (p *page) update(key, value []byte) (isExist bool, isEnoughSpace bool) {
	pre, current := p.currentRecord(key)
	// record不存在
	if current == nil || bytes.Compare(current.Key, key) != 0 {
		return false, false
	}

	r := &record{
		spaceLen: current.spaceLen,
		next:     current.next,
		Key:      key,
		Value:    value,
		offset:   current.offset,
	}
	// 原地址空间符合
	if current.spaceLen >= r.needSpaceLen() {
		p._setRecordOnOffset(r)
		return true, true
	}

	// 原地址空间不符合
	offset, spaceLen, ok := p._getSpace(r.needSpaceLen())
	r.offset = offset
	r.spaceLen = spaceLen
	// 页空间符合
	if ok {
		p._setRecordOnOffset(r)
		if pre == nil {
			p._setIndexByFlag2(flag2RecordBegin, offset)
		} else {
			pre.next = offset
			p._setRecordOnOffset(pre)
		}

		// 回收空间
		p._recycle(current)
		return true, true
	}

	// 页空间不符合
	p.delete(key)
	return true, false
}

func (p *page) delete(key []byte) bool {
	pre, current := p.currentRecord(key)
	if current == nil || bytes.Compare(current.Key, key) != 0 {
		return false
	}
	if pre == nil {
		p._setIndexByFlag2(flag2RecordBegin, current.next)
	} else {
		pre.next = current.next
		p._setRecordOnOffset(pre)
	}

	p._recycle(current)
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
	_, isEnoughSpace := p.add(key, value)
	return isEnoughSpace
}

func (p *page) isNil() bool {
	if p._indexByFlag2(flag2RecordBegin) == 0 {
		return true
	}
	return false
}

// splitFront 溢出前面record
func (p *page) splitFront(key, value []byte) []*record {
	all := p.all()
	all = appendToSortedRecords(all, &record{Key: key, Value: value})

	p._reset()

	var useSpace uint16 = 0
	overflow := make([]*record, 0, 10)
	for i := range all {
		// i != len(all) 这里要保证，p不是一个空页
		if useSpace < recordMaxSize && i != len(all)-1 {
			overflow = append(overflow, all[i])
			useSpace += all[i].needSpaceLen()
		} else {
			p.add(all[i].Key, all[i].Value)
		}
	}
	return overflow
}

// splitBehind 分裂节点，溢出后面record
// first return 溢出的记录
// second return 新插入的记录位置是否在前置节点
func (p *page) splitBehind(key, value []byte) ([]*record, bool) {
	all := p.all()
	all = appendToSortedRecords(all, &record{Key: key, Value: value})

	p._reset()

	var useSpace uint16 = 0
	overflow := make([]*record, 0, 10)
	for i := range all {
		if useSpace < recordMaxSize {
			p.add(all[i].Key, all[i].Value)
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

func (p *page) _reset() {
	p._setIndexByFlag2(flag2RecordBegin, 0)
	p._setIndexByFlag2(flag2RecycleBegin, 0)
	p._setIndexByFlag2(flag2FreeBegin, 0)
}

// currentRecord 查找key所在的pre record 和 current record
// 节点为空      preRecord == nil, current == nil
// 小于所有元素   preRecord == nil, current != nil
// 中间         preRecord == nil, current != nil
// 大于所有元素   preRecord.next = 0 preRecord==current
func (p *page) currentRecord(key []byte) (pre, current *record) {
	offset := p._indexByFlag2(flag2RecordBegin)
	if offset == 0 {
		return
	}

	for {
		current = p._record(offset)
		if bytes.Compare(current.Key, key) >= 0 {
			break
		}

		pre = current
		if current.next == 0 {
			break
		}
		offset = current.next
	}
	return
}

func (p *page) preRecord(key []byte) (pre *record) {

	offset := p._indexByFlag2(flag2RecordBegin)
	if offset == 0 {
		return
	}

	var current *record
	for {
		current = p._record(offset)
		if bytes.Compare(current.Key, key) > 0 {
			break
		}

		pre = current
		if current.next == 0 {
			break
		}
		offset = current.next
	}
	return
}

// _recycle 回收record空间
func (p *page) _recycle(record *record) {
	recycleBegin := p._indexByFlag2(flag2RecycleBegin)
	record.next = recycleBegin
	p._setRecordOnOffset(record)
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
		nextIndex = binary.BigEndian.Uint16(p.buf[spaceOffset+2:])

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
	binary.BigEndian.PutUint16(p.buf[preOffset+2:], nextIndex)
	ok = true
	return
}

func (p *page) _getFreeSpace(needSpaceLen uint16) (spaceOffset uint16, spaceLen uint16, ok bool) {
	freeBegin := p._indexByFlag2(flag2FreeBegin)
	if freeBegin == 0 {
		freeBegin = recordsDefaultBegin
	}
	// 剩余空闲空间检查
	if pageSize-freeBegin < needSpaceLen {
		ok = false
		return
	}

	p._setIndexByFlag2(flag2FreeBegin, freeBegin+needSpaceLen)
	return freeBegin, needSpaceLen, true
}

// _setRecordOnOffset 在指定偏移量上设置记录
func (p *page) _setRecordOnOffset(record *record) {
	offset := record.offset
	// 设置spaceLen
	binary.BigEndian.PutUint16(p.buf[offset:], record.spaceLen)
	offset += 2
	// 设置nextIndex
	binary.BigEndian.PutUint16(p.buf[offset:], record.next)
	offset += 2
	// 设置keyLen
	binary.BigEndian.PutUint16(p.buf[offset:], uint16(len(record.Key)))
	offset += 2
	// 设置valueLen
	binary.BigEndian.PutUint16(p.buf[offset:], uint16(len(record.Value)))
	offset += 2
	// 设置key
	copy(p.buf[offset:], record.Key)
	offset += uint16(len(record.Key))
	// 设置value
	copy(p.buf[offset:], record.Value)
	offset += uint16(len(record.Value))
}

// _record 在指定偏移位置
func (p *page) _record(offset uint16) *record {
	var record record
	record.offset = offset

	// 读取spaceLen
	record.spaceLen = binary.BigEndian.Uint16(p.buf[offset:])
	offset += 2
	// 读取nextIndex
	record.next = binary.BigEndian.Uint16(p.buf[offset:])
	offset += 2
	// 读取keyLen
	keyLen := binary.BigEndian.Uint16(p.buf[offset:])
	offset += 2
	// 读取valueLen
	valueLen := binary.BigEndian.Uint16(p.buf[offset:])
	offset += 2
	// 读取key
	record.Key = make([]byte, keyLen)
	copy(record.Key, p.buf[offset:offset+keyLen])
	offset += keyLen
	// 读取value
	record.Value = make([]byte, valueLen)
	copy(record.Value, p.buf[offset:offset+valueLen])

	record.pageOffset = p.offset
	return &record
}

func (p *page) all() []*record {
	offset := p._indexByFlag2(flag2RecordBegin)
	if offset == 0 {
		return nil
	}

	list := make([]*record, 0, 10)
	for {
		record := p._record(offset)
		offset = record.next
		list = append(list, record)
		if record.next == 0 {
			break
		}
	}
	return list
}
