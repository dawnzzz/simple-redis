package list

import "container/list"

// 迭代器，在[-1, ql.Len()]之间移动
type iterator struct {
	node   *list.Element
	offset int
	ql     *QuickList
}

// 获取 index 位置上的迭代器
func (ql *QuickList) find(index int) *iterator {
	var node *list.Element
	var pageBeg int
	// 首先查看 index 在快速链表的前半部分还是后半部分。
	if index < ql.size/2 {
		// 若在前半部分，则从前向后查找
		node = ql.data.Front()
		pageBeg = 0
		for {
			page := node.Value.([]interface{})
			if pageBeg+len(page) > index {
				break
			}

			pageBeg += len(page)
			node = node.Next()
		}
	} else {
		// 后半部分，从后向前查找
		node = ql.data.Back()
		pageBeg = ql.size
		for {
			page := node.Value.([]interface{})
			if pageBeg-len(page) <= index {
				pageBeg -= len(page)
				break
			}

			pageBeg -= len(page)
			node = node.Prev()
		}
	}

	// 计算这个 index 在 page 中的相对位置 offset，返回迭代器。
	pageOffset := index - pageBeg
	return &iterator{
		node:   node,
		offset: pageOffset,
		ql:     ql,
	}
}

func (iter *iterator) get() interface{} {
	return iter.page()[iter.offset]
}

func (iter *iterator) page() []interface{} {
	return iter.node.Value.([]interface{})
}

func (iter *iterator) next() bool {
	// 首先获取所在 page，若当前迭代器的元素不是 page 的最后一个元素，直接 offset + 1即可返回。
	page := iter.page()
	if iter.offset < len(page)-1 {
		iter.offset += 1
		return true
	}

	// 是page的最后一个元素，则移动到下一个 page 上
	// 移动之前首先看看当前 page 是不是最后一个 page，若是则说明迭代器当前指向的元素是整个快速链表的最后一个元素。此时将迭代器移动到 ql.Len() 位置上。
	if iter.node == iter.ql.data.Back() {
		iter.offset = len(page)
		return false
	}
	// 不是最后一个 page，就移动到下一个 page 的第一个数据上。
	iter.node = iter.node.Next()
	iter.offset = 0

	return true
}

func (iter *iterator) prev() bool {
	// 看看是不是第一个元素，不是则直接返回
	if iter.offset > 0 {
		iter.offset -= 1
		return true
	}

	// 是第一个元素，则移动到上一个page上
	// 移动之前首先看看 page 是不是第一个 page，若是则说明迭代器当前指向的元素是整个快速链表的第一个元素。此时将迭代器移动到 -1 位置上
	if iter.node == iter.ql.data.Front() {
		iter.offset = -1
		return false
	}

	// 不是第一个page，移动到上一个page的最后一个元素上
	iter.node = iter.node.Prev()
	iter.offset = len(iter.page()) - 1

	return true
}

// 是否在 ql.Len() 位置上
func (iter *iterator) atEnd() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Back() {
		return false
	}

	page := iter.page()
	return iter.offset == len(page)
}

// 是否在 -1 位置上
func (iter *iterator) atBegin() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Front() {
		return false
	}
	return iter.offset == -1
}

func (iter *iterator) set(val interface{}) {
	page := iter.page()
	page[iter.offset] = val
}

func (iter *iterator) remove() interface{} {
	page := iter.page()
	val := iter.get()
	page = append(page[:iter.offset], page[iter.offset+1:]...) // 在page上删除元素
	// 整理页面
	if len(page) > 0 {
		// page 上还剩余元素
		iter.node.Value = page
		if iter.offset == len(page) {
			// 删除的是最后一个元素，则移动到下一页的第一个元素上
			if iter.node != iter.ql.data.Back() {
				iter.node = iter.node.Next()
				iter.offset = 0
			}
		}
	} else {
		// page 上不剩元素了，删除这个page
		if iter.node != iter.ql.data.Back() {
			// 最后一页，也就是说删除的是最后一个元素
			iter.ql.data.Remove(iter.node)
			iter.node = nil
			iter.offset = 0
		} else {
			nextNode := iter.node.Next()
			iter.ql.data.Remove(iter.node)
			iter.node = nextNode
			iter.offset = 0
		}
	}

	iter.ql.size--
	return val
}
