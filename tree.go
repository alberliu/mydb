package mydb

import (
	"bytes"
	"fmt"
)

type tree struct {
	fm *fileManager
}

func newTree(fm *fileManager) *tree {
	return &tree{fm: fm}
}

func (b *tree) _getLeafPage(key []byte) *page {
	front := b.fm.frontPage()
	if bytes.Compare(key, front.min()) < 0 {
		return nil
	}

	page := b.fm.rootPage()
	for page.pageType() != pageTypeLeaf {
		pre := page.preRecord(key)
		page = b.fm.page(pre.child())
	}
	return page
}

// add 添加 相等
func (b *tree) add(key, value []byte) bool {
	front := b.fm.frontPage()
	if bytes.Compare(key, front.min()) < 0 {
		_, isEnoughSpace := front.add(key, value)
		if isEnoughSpace {
			// 叶子节点不分裂,添加，并且更新枝干节点最小值
			b._addToPageParent(front, nil)
		} else {
			// 叶子节点需要分裂
			newPage := b.fm.newPage(pageTypeLeaf)
			newPage.add(key, value)

			newPage.setNext(front.offset)
			front.setPre(front.offset)

			b.fm.setFront(newPage.offset)
			b._addToPageParent(front, newPage)
		}
		return true
	} else {
		leafPage := b._getLeafPage(key)
		isCanAdd, isEnoughSpace := leafPage.add(key, value)
		// 有相同数据，直接返回false
		if !isCanAdd {
			return false
		}
		if !isEnoughSpace {
			records, _ := leafPage.splitBehind(key, value)
			if len(records) == 0 {
				return true
			}

			newPage := b.fm.newPage(pageTypeLeaf)
			for i := range records {
				newPage.add(records[i].Key, records[i].Value)
			}

			newPage.setPre(leafPage.pre())
			newPage.setNext(leafPage.next())

			leafPage.setPre(newPage.offset)
			leafPage.setNext(newPage.offset)
			b._addToPageParentAfter(leafPage, newPage)
		}
		return true
	}
}

// _addToPageParent 添加到page节点的parent节点，cn.page最小值小于node节点最小值
func (b *tree) _addToPageParent(page, addedPage *page) {
	for {
		parentOffset := page.parent()
		if parentOffset == 0 && addedPage == nil {
			break
		}

		if addedPage == nil {
			parent := b.fm.page(parentOffset)
			pageMin := page.min()
			isEnoughSpace := parent.updateMinKey(pageMin)
			if isEnoughSpace {
				page = parent
				addedPage = nil
			} else {
				parent.delete(pageMin)
				records := parent.splitFront(addedPage.min(), addedPage.offsetBuf())

				newPage := b.fm.newPage(pageTypeBranch)
				for i := range records {
					newPage.add(records[i].Key, records[i].Value)

					page := b.fm.page(records[i].child())
					page.setParent(newPage.offset)
				}

				page = parent
				addedPage = newPage
			}
			continue
		}

		if parentOffset == 0 {
			// root 节点需要分裂
			newPage := b.fm.newPage(pageTypeBranch)
			newPage.add(addedPage.min(), addedPage.offsetBuf())
			newPage.add(page.min(), page.offsetBuf())

			page.setParent(newPage.offset)
			addedPage.setParent(newPage.offset)
			b.fm.setRoot(newPage.offset)
			break
		}

		parent := b.fm.page(parentOffset)
		_, isEnoughSpace := parent.add(addedPage.min(), addedPage.offsetBuf())
		if isEnoughSpace {
			addedPage.setParent(parentOffset)

			// 这里只需要更新parent节点的最小值
			page = parent
			addedPage = nil
		} else {
			// 枝干节点节点需要分裂
			newPage := b.fm.newPage(pageTypeBranch)
			newPage.add(addedPage.min(), addedPage.offsetBuf())
			addedPage.setParent(newPage.offset)

			page = parent
			addedPage = newPage
		}
	}
}

// _addToPageParentAfter 将addedPage节点添加到page的parent节点
func (b *tree) _addToPageParentAfter(page, addedPage *page) {
	for {
		parentOffset := page.parent()
		if parentOffset == 0 {
			// page是根节点
			newPage := b.fm.newPage(pageTypeBranch)
			newPage.add(page.min(), page.offsetBuf())
			newPage.add(addedPage.min(), addedPage.offsetBuf())

			page.setParent(newPage.offset)
			addedPage.setParent(newPage.offset)
			b.fm.setRoot(newPage.offset)
			return
		}

		// node是非根节点
		parent := b.fm.page(parentOffset)
		_, isEnoughSpace := parent.add(addedPage.min(), addedPage.offsetBuf())
		if isEnoughSpace {
			// parent没有分裂
			addedPage.setParent(parentOffset)
			return
		}
		// parent分裂,
		// 这里不一定要指向新节点
		records, isFront := parent.splitBehind(addedPage.min(), addedPage.offsetBuf())
		if len(records) == 0 {
			return
		}

		newPage := b.fm.newPage(pageTypeBranch)
		for i := range records {
			newPage.add(records[i].Key, records[i].Value)

			page := b.fm.page(records[i].child())
			page.setParent(newPage.offset)
		}

		if isFront {
			addedPage.setParent(parent.offset)
		} else {
			addedPage.setParent(newPage.offset)
		}

		page = parent
		addedPage = newPage
	}
}

func (b *tree) update(key, value []byte) bool {
	leafNode := b._getLeafPage(key)
	if leafNode == nil {
		return false
	}

	isCanUpdate, isEnoughSpace := leafNode.update(key, value)
	if !isCanUpdate {
		return false
	}
	if isEnoughSpace {
		return true
	}

	b.add(key, value)
	return true
}

// delete 如果没有数据了，需要删除节点
func (b *tree) delete(key []byte) bool {
	if bytes.Compare(key, b.fm.frontPage().min()) < 0 {
		return false
	}

	leafNode := b._getLeafPage(key)
	ok := leafNode.delete(key)
	if !ok {
		return false
	}

	if !leafNode.isNil() {
		return true
	}

	// 这里应该回收节点

	// 处理叶子节点
	// 是front节点
	if leafNode.pre() == 0 {
		b.fm.setFront(leafNode.next())
	} else {
		b.fm.page(leafNode.pre()).setNext(leafNode.next())
	}
	// 回收叶子节点
	b.fm.recycle(leafNode)

	// 处理枝干节点
	// 父节点不为nil,需要删除在父节点的位置
	if leafNode.parent() == 0 {
		return true
	}
	parent := b.fm.page(leafNode.parent())
	for {
		parent.delete(key)
		if !parent.isNil() {
			break
		}
		parent = b.fm.page(parent.parent())
	}

	return true
}

func (b *tree) get(key []byte) ([]byte, bool) {
	leafPage := b._getLeafPage(key)
	if leafPage == nil {
		return nil, false
	}

	return leafPage.get(key)
}

func (b *tree) query(min, max []byte) []*record {
	if bytes.Compare(min, Infinity) == 0 && bytes.Compare(max, Infinity) == 0 {
		return b.all()
	}

	if bytes.Compare(min, Infinity) == 0 {
		page := b._getLeafPage(max)
		if page == nil {
			return nil
		}

		var records []*record
		for {
			result := page.query(min, max)
			if len(result) == 0 {
				return records
			}
			records = append(records, result...)

			if page.pre() == 0 {
				break
			}
			page = b.fm.page(page.pre())
		}
		return records
	} else {
		page := b._getLeafPage(min)
		if page == nil {
			return nil
		}

		var records []*record
		for {
			result := page.query(min, max)
			if len(result) == 0 {
				return records
			}
			records = append(records, result...)

			if page.next() == 0 {
				break
			}
			page = b.fm.page(page.next())
		}
		return records
	}
}

func (b *tree) all() []*record {
	cns := make([]*record, 0, 100)

	page := b.fm.frontPage()
	for {
		cns = append(cns, page.all()...)

		if page.next() == 0 {
			break
		}
		page = b.fm.page(page.next())
	}
	return cns
}

func (b *tree) display() {
	splitPage := &page{}

	queue := NewQueue[page]()
	queue.Push(b.fm.rootPage())
	queue.Push(splitPage)

	page := queue.Pop()
	for page != nil {
		if page == splitPage {
			fmt.Println()

			if queue.list.Len() == 0 {
				break
			}

			queue.Push(splitPage)
			page = queue.Pop()
			continue
		}

		page.display()

		if page.pageType() == pageTypeBranch {
			cns := page.all()
			for i := range cns {
				queue.Push(b.fm.page(cns[i].child()))
			}
		}

		page = queue.Pop()
	}
	fmt.Println()
}
