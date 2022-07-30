package mydb

import (
	"fmt"
	"os"
	"testing"
)

func newDefaultFileManager() *fileManager {
	name := "data.txt"
	os.Remove(name)
	fm, err := newFileManager(name)
	if err != nil {
		panic(err)
	}
	return fm
}

func Test_newFileManager(t *testing.T) {
	fm := newDefaultFileManager()

	rootPage := fm.rootPage()
	if rootPage.offset != pageSize {
		t.Fatalf("root:%d", rootPage.offset)
	}
	fmt.Println(rootPage.pageType())

	frontPage := fm.frontPage()
	if frontPage.offset != pageSize {
		t.Fatalf("front:%d", frontPage.offset)
	}
	fmt.Println(frontPage.pageType())

	fm.setRoot(pageSize * 2)
	if fm.rootPage().offset != pageSize*2 {
		t.FailNow()
	}

	fm.setFront(pageSize * 2)
	if fm.frontPage().offset != pageSize*2 {
		t.FailNow()
	}
}

func Test_fileManager_newPage(t *testing.T) {
	fm := newDefaultFileManager()

	if fm.newPage(pageTypeLeaf).offset != 8192 {
		t.Fatal()
	}
	if fm.newPage(pageTypeLeaf).offset != 12288 {
		t.Fatal()
	}
}

func Test_fileManager_recycle(t *testing.T) {
	fm := newDefaultFileManager()

	page := fm.newPage(pageTypeLeaf)
	if fm.fileSize() != int64(pageSize*3) {
		t.Fail()
	}

	fm.recycle(page)
	if fm.fileSize() != pageSize*3 {
		t.Fail()
	}

	fm.newPage(pageTypeLeaf)
	if fm.fileSize() != pageSize*3 {
		t.Fail()
	}
}
