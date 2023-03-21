package list

import (
	"container/list"
)

const (
	pageSize = 1024
)

// QuickList 快速链表，用于实现list
type QuickList struct {
	data *list.List
	size int
}

func MakeQuickList() *QuickList {
	return &QuickList{
		data: list.New(),
	}
}

func (ql *QuickList) Add(val interface{}) {
	ql.size++
	if ql.data.Len() == 0 {
		// list 为空，插入一个page
		page := make([]interface{}, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)

		return
	}

	// list不为空，找到最后一个 page，**检查最后一个 page 是否已满**。
	backNode := ql.data.Back()
	backPage := backNode.Value.([]interface{})

	if len(backPage) == pageSize {
		// 若**最后一个 page 满了则新增一个 page 到后面**，插入数据到新增 page 的第一个位置。
		page := make([]interface{}, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)

		return
	}

	// 若**最后一个 page 没有满**则直接插入到最后一个 page 的最后面。
	backPage = append(backPage, val)
	backNode.Value = backPage
}

func (ql *QuickList) Get(index int) (val interface{}) {
	iter := ql.find(index)

	return iter.get()
}

func (ql *QuickList) Set(index int, val interface{}) {
	if index < 0 || index >= ql.size {
		panic("`index` out of range")
	}

	iter := ql.find(index)
	iter.set(val)
}

func (ql *QuickList) Insert(index int, val interface{}) {
	if index < 0 || index > ql.size {
		panic("`index` out of range")
	}

	if index == ql.size { // insert at
		ql.Add(val)
		return
	}

	iter := ql.find(index)
	page := iter.page()

	// 若 page 没有满，则**直接插入**到 page 的相应位置上并返回。
	if len(page) < pageSize {
		// 直接插入
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
		iter.node.Value = page
		ql.size++

		return
	}

	// **若 page 满了**，则将一个**满的 page 分裂成两个 page**。
	nextPage := make([]interface{}, 0, pageSize)
	nextPage = append(nextPage, page[pageSize/2:]...)
	page = page[:pageSize/2]

	// 若迭代器的 offset 在前半部分则在第一个 page 上插入；否则在第二个位置上插入数据。
	if iter.offset < len(page) {
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
	} else {
		newOffset := iter.offset - pageSize/2
		nextPage = append(nextPage[:newOffset+1], nextPage[newOffset:]...)
		nextPage[newOffset] = val
	}

	iter.node.Value = page
	ql.data.InsertAfter(nextPage, iter.node)
	ql.size++
}

func (ql *QuickList) Remove(index int) (val interface{}) {
	if index < 0 || index >= ql.size {
		panic("`index` out of range")
	}

	iter := ql.find(index)

	return iter.remove()
}

func (ql *QuickList) RemoveLast() (val interface{}) {
	if ql.Len() == 0 {
		return nil
	}

	iter := ql.find(ql.size - 1) // 最后一个元素

	return iter.remove()
}

func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	if ql.size == 0 {
		return 0
	}

	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}

	return removed
}

func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}

	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		} else {
			iter.next()
		}
	}
	return removed
}

func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(ql.size - 1)
	removed := 0
	for !iter.atBegin() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		}
		iter.prev()
	}
	return removed
}

func (ql *QuickList) Len() int {
	return ql.size
}

func (ql *QuickList) ForEach(consumer Consumer) {
	if ql == nil {
		panic("list is nil")
	}

	if ql.Len() == 0 {
		return
	}

	iter := ql.find(0)
	i := 0
	for {
		goNext := consumer(i, iter.get())
		if !goNext {
			break
		}
		i++
		if !iter.next() {
			break
		}
	}
}

func (ql *QuickList) Contains(expected Expected) bool {
	contains := false
	ql.ForEach(func(i int, actual interface{}) bool {
		if expected(actual) {
			contains = true
			return false
		}
		return true
	})

	return contains
}

func (ql *QuickList) Range(start int, stop int) []interface{} {
	if start < 0 || start >= ql.Len() {
		panic("`start` out of range")
	}
	if stop < start || stop > ql.Len() {
		panic("`stop` out of range")
	}

	sliceCap := stop - start
	slice := make([]interface{}, 0, sliceCap)
	iter := ql.find(start)

	for i := 0; i < sliceCap; i++ {
		slice = append(slice, iter.get())
		iter.next()
	}

	return slice
}
