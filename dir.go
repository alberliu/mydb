package mydb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	dirRecordMaxNum = 8
	dirHeaderLen    = 8
	maxDirSize      = 256
)

/**
dir 物理存储结构
recordOffset      record实际存储的偏移位置
num               当天目录下record数量
keyLen            key的长度
key               key的值
keyLen            key的长度，为了从当前目录可以找到上一个目录的位置
*/

// dir 页目录
type dir struct {
	offset       uint16
	recordOffset uint16
	recordNum    uint16
	key          []byte
}

func (d *dir) String() string {
	return fmt.Sprintf("{offset:%2d, recordOffset:%2d, recordNum:%2d, key:%s  },",
		d.offset, d.recordOffset, d.recordNum, string(d.key))
}

func (d *dir) equal(dir *dir) bool {
	if d.offset != dir.offset || d.recordOffset != dir.recordOffset || d.recordNum != dir.recordNum {
		return false
	}
	if !bytes.Equal(d.key, dir.key) {
		return false
	}
	return true
}

func (d *dir) needSpaceLen() uint16 {
	return dirHeaderLen + uint16(len(d.key))
}

// _dirOutOf 检查dirBegin是否越界
func (p *page) _dirOutOf(newDirBegin uint16) bool {
	freeBegin := p._indexByFlag2(flag2FreeBegin)
	if newDirBegin < freeBegin || newDirBegin < p.minDirBegin {
		return true
	}
	return false
}

// _dirMove 移动目录,是否是正数
func (p *page) _dirMove(offset, moved uint16, isPositive bool) {
	dirBegin := p._indexByFlag2(flag2DirBegin)

	if isPositive {
		newDirBegin := dirBegin + moved
		copy(p.buf[newDirBegin:offset+moved], p.buf[dirBegin:offset])

		p._setIndexByFlag2(flag2DirBegin, newDirBegin)
	} else {
		newDirBegin := dirBegin - moved
		copy(p.buf[newDirBegin:offset-moved], p.buf[dirBegin:offset])

		p._setIndexByFlag2(flag2DirBegin, newDirBegin)
	}
}

// _dirRebuild 重做目录
func (p *page) _dirRebuild() {
	records := p.all()

	offset := p.size
	for i, r := range records {
		remainder := i % dirRecordMaxNum
		if remainder == 0 {
			recordNum := dirRecordMaxNum
			if len(records)-i < dirRecordMaxNum {
				recordNum = len(records) - i
			}

			d := &dir{
				offset:       offset,
				recordOffset: r.offset,
				recordNum:    uint16(recordNum),
				key:          r.Key,
			}

			if p._dirOutOf(offset - d.needSpaceLen()) {
				lastDir := p._dirPre(offset)
				lastDir.recordNum += uint16(recordNum)
				p._dirSet(lastDir)
			} else {
				p._dirSet(d)
				offset -= d.needSpaceLen()
			}
		}
	}

	p._setIndexByFlag2(flag2DirBegin, offset)
}

func (p *page) _dirAdd(d *dir, r *record) {
	dirBegin := p._indexByFlag2(flag2DirBegin)
	if dirBegin == 0 && d == nil {
		d := &dir{
			offset:       p.size,
			recordOffset: r.offset,
			recordNum:    1,
			key:          r.Key,
		}
		d.offset = p.size
		p._dirSet(d)

		p._setIndexByFlag2(flag2DirBegin, p.size-d.needSpaceLen())
		return
	}

	// 最小值插入
	if d == nil {
		d := &dir{
			offset:       p.size,
			recordOffset: r.offset,
			recordNum:    1,
			key:          r.Key,
		}
		newDirBegin := dirBegin - d.needSpaceLen()
		if !p._dirOutOf(newDirBegin) {
			p._dirMove(p.size, d.needSpaceLen(), false)
			p._dirSet(d)
		} else {
			p._dirRebuild()
		}
		return
	}

	// 如果当前的目录的记录数量没有超过最大值，更新记录数即可
	if d.recordNum+1 < dirRecordMaxNum {
		p._dirSetRecordNum(d.offset, d.recordNum+1)
		return
	}

	// 如果大于最大目录数，需要分裂目录，检查
	splitIndex := (d.recordNum + 1) / 2
	splitRecord := p._recordByIndex(d.recordOffset, splitIndex)
	newDir := &dir{
		offset:       d.offset - d.needSpaceLen(),
		recordOffset: splitRecord.offset,
		recordNum:    d.recordNum - splitIndex,
		key:          splitRecord.Key,
	}

	newDirBegin := dirBegin - newDir.needSpaceLen()
	// 越界检查，如果会发生越界，不做分裂
	if p._dirOutOf(newDirBegin) {
		p._dirSetRecordNum(d.offset, d.recordNum)
		return
	}

	// 分裂目录
	d.recordNum = splitIndex
	if bytes.Compare(r.Key, newDir.key) < 0 {
		d.recordNum++
	} else {
		newDir.recordNum++
	}

	p._dirSet(d)

	p._dirMove(newDir.offset, newDir.needSpaceLen(), false)
	p._dirSet(newDir)
}

var recordByIndexIsMock = false

func (p *page) _recordByIndex(offset uint16, index uint16) *record {
	if recordByIndexIsMock {
		return &record{Key: toBytes(2), offset: 2}
	}

	var r *record
	for i := uint16(0); i <= index; i++ {
		r = p._record(offset)
		offset = r.next
	}
	return r
}

func (p *page) _dirDelete(d dir, r record) {
	dirBegin := p._indexByFlag2(flag2DirBegin)
	if dirBegin == 0 {
		return
	}

	// d 是目录的第一个元素
	if d.offset == p.size {
		p._dirDelRecordNum(d, r)
		return
	}

	// 尝试与上一个目录进行合并
	preDir := p._dirPre(d.offset)
	if d.recordNum+preDir.recordNum > dirRecordMaxNum {
		p._dirDelRecordNum(d, r)
		return
	}

	// 与preDir合并
	preDir.recordNum = preDir.recordNum + d.recordNum - 1
	p._dirSet(preDir)
	p._dirMove(d.offset-d.needSpaceLen(), d.needSpaceLen(), true)
}

func (p *page) _dirSet(d *dir) {
	offset := d.offset
	binary.BigEndian.PutUint16(p.buf[offset-2:offset], d.recordOffset)
	offset -= 2
	binary.BigEndian.PutUint16(p.buf[offset-2:offset], d.recordNum)
	offset -= 2
	keyLen := uint16(len(d.key))
	binary.BigEndian.PutUint16(p.buf[offset-2:offset], keyLen)
	offset -= 2
	copy(p.buf[offset-keyLen:offset], d.key)
	offset -= keyLen
	binary.BigEndian.PutUint16(p.buf[offset-2:offset], keyLen)
}

func (p *page) _dirSetRecordNum(offset, recordNum uint16) {
	binary.BigEndian.PutUint16(p.buf[offset-4:offset-2], recordNum)
}

var dirDelRecordNumIsMock = false

func (p *page) _dirDelRecordNum(d dir, r record) {
	// 只剩下一个元素，移除
	if d.recordNum == 1 {
		p._dirMove(d.offset-d.needSpaceLen(), d.needSpaceLen(), true)
		return
	}

	if !bytes.Equal(d.key, r.Key) {
		binary.BigEndian.PutUint16(p.buf[d.offset-4:d.offset-2], d.recordNum-1)
		return
	}

	dirBegin := p._indexByFlag2(flag2DirBegin)
	var next *record
	if dirDelRecordNumIsMock {
		next = &record{Key: toBytes(44), offset: 4}
	} else {
		next = p._record(r.next)
	}

	newDirBegin := uint16(int(dirBegin) + (len(d.key) - len(next.Key)))
	if !p._dirOutOf(newDirBegin) {
		isPositive := true
		moved := newDirBegin - dirBegin
		if dirBegin > newDirBegin {
			isPositive = false
			moved = dirBegin - newDirBegin
		}

		p._dirMove(d.offset-d.needSpaceLen(), moved, isPositive)
		d.recordOffset = next.offset
		d.key = next.Key
		d.recordNum--
		p._dirSet(&d)
		return
	}
	// 溢出，重建目录
	p._dirRebuild()
}

func (p *page) _dirGet(offset uint16) *dir {
	offsetTemp := offset
	recordOffset := binary.BigEndian.Uint16(p.buf[offset-2 : offset])
	offset -= 2
	recordNum := binary.BigEndian.Uint16(p.buf[offset-2 : offset])
	offset -= 2
	keyLen := binary.BigEndian.Uint16(p.buf[offset-2 : offset])
	offset -= 2
	key := make([]byte, keyLen)
	copy(key, p.buf[offset-keyLen:offset])
	return &dir{
		offset:       offsetTemp,
		recordOffset: recordOffset,
		recordNum:    recordNum,
		key:          key,
	}
}

func (p *page) _dirPre(offset uint16) *dir {
	if offset == p.size {
		return nil
	}
	keyLen := binary.BigEndian.Uint16(p.buf[offset : offset+2])
	return p._dirGet(offset + dirHeaderLen + keyLen)
}

// _dirFind 找到对应的目录，如果目录为空，返回空，如果小于目录总的最小值，返回空
func (p *page) _dirFind(key []byte) *dir {
	dirBegin := p._indexByFlag2(flag2DirBegin)
	if dirBegin == 0 {
		return nil
	}

	var pre *dir
	offset := p.size
	for {
		dir := p._dirGet(offset)

		if bytes.Compare(dir.key, key) > 0 {
			return pre
		}

		nextOffset := dir.offset - dir.needSpaceLen()
		if nextOffset <= dirBegin {
			return dir
		}
		pre = dir
		offset -= dir.needSpaceLen()
	}
}

func (p *page) _dirAll() []*dir {
	dirBegin := p._indexByFlag2(flag2DirBegin)
	if dirBegin == 0 {
		return nil
	}

	var ds []*dir
	offset := p.size
	for offset > dirBegin {
		d := p._dirGet(offset)
		ds = append(ds, d)

		offset -= d.needSpaceLen()
	}
	return ds
}
