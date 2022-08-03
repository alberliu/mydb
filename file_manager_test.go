package mydb

import (
	"fmt"
	"os"
	"testing"
)

func newDefaultFileManager() *fileManager {
	name := "data.txt"
	os.Remove(name)
	fm, err := newFileManager(name, defaultPageSize)
	if err != nil {
		panic(err)
	}
	return fm
}

func Test_newFileManager(t *testing.T) {
	fm := newDefaultFileManager()

	rootPage := fm.rootPage()
	if rootPage.offset != fm.pageSize {
		t.Fatalf("root:%d", rootPage.offset)
	}
	fmt.Println(rootPage.pageType())

	frontPage := fm.frontPage()
	if frontPage.offset != fm.pageSize {
		t.Fatalf("front:%d", frontPage.offset)
	}
	fmt.Println(frontPage.pageType())

	fm.setRoot(fm.pageSize * 2)
	if fm.rootPage().offset != fm.pageSize*2 {
		t.FailNow()
	}

	fm.setFront(fm.pageSize * 2)
	if fm.frontPage().offset != fm.pageSize*2 {
		t.FailNow()
	}
}

func Test_fileManager_allocatePage(t *testing.T) {
	fm := newDefaultFileManager()

	if fm.allocatePage(pageTypeLeaf).offset != 8192 {
		t.Fatal()
	}
	if fm.allocatePage(pageTypeLeaf).offset != 12288 {
		t.Fatal()
	}
}

func Test_fileManager_recycle(t *testing.T) {
	fm := newDefaultFileManager()

	page := fm.allocatePage(pageTypeLeaf)
	if fm.fileSize() != int64(fm.pageSize*3) {
		t.Fail()
	}

	fm.recycle(page)
	if fm.fileSize() != fm.pageSizeInt64*3 {
		t.Fail()
	}

	fm.allocatePage(pageTypeLeaf)
	if fm.fileSize() != fm.pageSizeInt64*3 {
		t.Fail()
	}
}
