package mydb

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"syscall"
)

type fileManager struct {
	file *os.File
	fd   int
}

const (
	rootBegin    = 0
	rootEnd      = 8
	frontBegin   = 8
	frontEnd     = 16
	recycleBegin = 16
	recycleEnd   = 24
)

func newFileManager(name string) (*fileManager, error) {
	file, err := os.OpenFile(name, syscall.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	fm := &fileManager{
		file: file,
		fd:   int(file.Fd()),
	}

	if fm.fileSize() == 0 {
		err = syscall.Ftruncate(fm.fd, pageSize)
		if err != nil {
			return nil, err
		}

		page := fm.newPage(pageTypeLeaf)
		fm.setRoot(page.offset)
		fm.setFront(page.offset)
	}
	return fm, nil
}

func (f *fileManager) fileSize() int64 {
	info, err := f.file.Stat()
	if err != nil {
		panic(err)
	}
	return info.Size()
}

func (f *fileManager) rootPage() *page {
	buf, err := syscall.Mmap(f.fd, 0, pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	index := binary.BigEndian.Uint64(buf[rootBegin:rootEnd])
	if index == 8 {
		log.Println(buf[0:32])
		panic(index)
	}
	return f.page(index)
}

func (f *fileManager) frontPage() *page {
	buf, err := syscall.Mmap(f.fd, 0, pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	return f.page(binary.BigEndian.Uint64(buf[frontBegin:frontEnd]))
}

func (f *fileManager) setRoot(root uint64) {
	buf, err := syscall.Mmap(f.fd, 0, pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	binary.BigEndian.PutUint64(buf[rootBegin:rootEnd], root)
}

func (f *fileManager) setFront(front uint64) {
	buf, err := syscall.Mmap(f.fd, 0, pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	binary.BigEndian.PutUint64(buf[frontBegin:frontEnd], front)
}

// get 获取page
func (f *fileManager) page(offset uint64) *page {
	buf, err := syscall.Mmap(f.fd, int64(offset), pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		log.Println(err, offset)
		panic(err)
	}
	return &page{offset: offset, buf: buf}
}

// newPage 申请内存
func (f *fileManager) newPage(pageType uint16) *page {
	// 从回收空间获取
	buf, err := syscall.Mmap(f.fd, 0, pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	recycleOffset := binary.BigEndian.Uint64(buf[recycleBegin:recycleEnd])
	if recycleOffset != 0 {
		buf, err := syscall.Mmap(f.fd, int64(recycleOffset), pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			panic(err)
		}

		page := newPage(buf, recycleOffset, pageType)
		binary.BigEndian.PutUint64(buf[recycleBegin:recycleEnd], page.next())
		return page
	}

	// 申请磁盘空间
	fileSize := f.fileSize()
	err = syscall.Ftruncate(f.fd, fileSize+pageSize)
	if err != nil {
		panic(err)
	}

	buf, err = syscall.Mmap(f.fd, fileSize, pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	return newPage(buf, uint64(fileSize), pageType)
}

// recycle 回收空间
func (f *fileManager) recycle(page *page) {
	page._reset()

	buf, err := syscall.Mmap(f.fd, 0, pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	page.setNext(binary.BigEndian.Uint64(buf[recycleBegin:recycleEnd]))
	binary.BigEndian.PutUint64(buf[recycleBegin:recycleEnd], page.offset)
}

func (f *fileManager) getFromRecycle(page *page) {
	buf, err := syscall.Mmap(f.fd, 0, pageSize, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	page.setNext(binary.BigEndian.Uint64(buf[recycleBegin:recycleEnd]))
	binary.BigEndian.PutUint64(buf[recycleBegin:recycleEnd], page.offset)
}

// statistics page统计
func (f *fileManager) statisticsPage() string {
	info, err := f.file.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := uint64(info.Size())

	var branchPageNum, leafPageNum int

	offset := uint64(pageSize)
	for offset < fileSize {
		page := f.page(offset)
		if page.pageType() == pageTypeBranch {
			branchPageNum++
		} else {
			leafPageNum++
		}
		offset += pageSize
	}

	var depth = 1
	page := f.frontPage()
	for {
		parent := page.parent()
		if parent == 0 {
			break
		}

		depth++
		page = f.page(parent)
	}

	return fmt.Sprintf("branchPageNum:%d, leafPageNum:%d, depth:%d", branchPageNum, leafPageNum, depth)
}
