package mydb

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"syscall"
)

type fileManager struct {
	pageSize      uint64
	pageSizeInt   int
	pageSizeInt64 int64

	file *os.File
	fd   int
}

const (
	rootBegin    = 0
	frontBegin   = 8
	recycleBegin = 16
)

func newFileManager(name string, pageSize uint64) (*fileManager, error) {
	file, err := os.OpenFile(name, syscall.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	fm := &fileManager{
		pageSize:      pageSize,
		pageSizeInt:   int(pageSize),
		pageSizeInt64: int64(pageSize),
		file:          file,
		fd:            int(file.Fd()),
	}

	if fm.fileSize() == 0 {
		err = syscall.Ftruncate(fm.fd, fm.pageSizeInt64)
		if err != nil {
			return nil, err
		}

		page := fm.allocatePage(pageTypeLeaf)
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
	buf, err := syscall.Mmap(f.fd, 0, f.pageSizeInt, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	index := binary.BigEndian.Uint64(buf[rootBegin:])
	if index == 8 {
		log.Println(buf[0:32])
		panic(index)
	}
	return f.page(index)
}

func (f *fileManager) frontPage() *page {
	buf, err := syscall.Mmap(f.fd, 0, f.pageSizeInt, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	return f.page(binary.BigEndian.Uint64(buf[frontBegin:]))
}

func (f *fileManager) setRoot(root uint64) {
	buf, err := syscall.Mmap(f.fd, 0, f.pageSizeInt, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	binary.BigEndian.PutUint64(buf[rootBegin:], root)
}

func (f *fileManager) setFront(front uint64) {
	buf, err := syscall.Mmap(f.fd, 0, f.pageSizeInt, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	binary.BigEndian.PutUint64(buf[frontBegin:], front)
}

// page 获取page
func (f *fileManager) page(offset uint64) *page {
	buf, err := syscall.Mmap(f.fd, int64(offset), f.pageSizeInt, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		log.Println(err, offset)
		panic(err)
	}
	return &page{offset: offset, buf: buf}
}

// allocatePage 分配页空间，首先会尝试从回收空间分配，再申请新的磁盘空间
func (f *fileManager) allocatePage(pageType uint16) *page {
	// 从回收空间获取
	buf, err := syscall.Mmap(f.fd, 0, f.pageSizeInt, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	recycleOffset := binary.BigEndian.Uint64(buf[recycleBegin:])
	if recycleOffset != 0 {
		buf, err := syscall.Mmap(f.fd, int64(recycleOffset), f.pageSizeInt, syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			panic(err)
		}

		page := newPage(buf, recycleOffset, pageType)
		binary.BigEndian.PutUint64(buf[recycleBegin:], page.next())
		return page
	}

	// 申请磁盘空间
	fileSize := f.fileSize()
	err = syscall.Ftruncate(f.fd, fileSize+f.pageSizeInt64)
	if err != nil {
		panic(err)
	}

	buf, err = syscall.Mmap(f.fd, fileSize, f.pageSizeInt, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	return newPage(buf, uint64(fileSize), pageType)
}

// recycle 回收空间
func (f *fileManager) recycle(page *page) {
	page._reset()
	page.setPageType(pageTypeRecycle)

	buf, err := syscall.Mmap(f.fd, 0, f.pageSizeInt, syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	page.setNext(binary.BigEndian.Uint64(buf[recycleBegin:]))
	binary.BigEndian.PutUint64(buf[recycleBegin:], page.offset)
}

// statistics page统计
func (f *fileManager) statisticsPage() string {
	info, err := f.file.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := uint64(info.Size())

	// 统计枝干页和叶子页数量
	var branchPageNum, leafPageNum, recyclePageNum int
	offset := f.pageSize
	for offset < fileSize {
		page := f.page(offset)
		switch page.pageType() {
		case pageTypeBranch:
			branchPageNum++
		case pageTypeLeaf:
			leafPageNum++
		case pageTypeRecycle:
			recyclePageNum++
		}
		offset += f.pageSize
	}

	// 统计b+树的深度
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

	return fmt.Sprintf("filesize:%dB, totalPageNum:%d, branchPageNum:%d, leafPageNum:%d, recyclePageNum:%d, depth:%d",
		fileSize, fileSize/f.pageSize, branchPageNum, leafPageNum, recyclePageNum, depth)
}
