package lock

import (
	"sort"
	"sync"
)

type Locks struct {
	tables []*sync.RWMutex
}

func Make(tableSize int) *Locks {
	tables := make([]*sync.RWMutex, tableSize)
	for i := range tables {
		tables[i] = &sync.RWMutex{}
	}
	return &Locks{
		tables: tables,
	}
}

const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 检查 Locks 是否没有初始化，没有初始化则返回 true
func (locks *Locks) notInit() bool {
	return locks == nil || locks.tables == nil
}

func (locks *Locks) spread(hashcode uint32) uint32 {
	if locks.notInit() {
		panic("dict is nil")
	}

	tableSize := uint32(len(locks.tables))

	return hashcode % tableSize
}

func (locks *Locks) Lock(key string) {
	hashcode := fnv32(key)
	index := locks.spread(hashcode)

	m := locks.tables[index]
	m.Lock()
}

func (locks *Locks) Unlock(key string) {
	hashcode := fnv32(key)
	index := locks.spread(hashcode)

	m := locks.tables[index]
	m.Unlock()
}

func (locks *Locks) RLock(key string) {
	hashcode := fnv32(key)
	index := locks.spread(hashcode)

	m := locks.tables[index]
	m.RLock()
}

func (locks *Locks) RUnlock(key string) {
	hashcode := fnv32(key)
	index := locks.spread(hashcode)

	m := locks.tables[index]
	m.RUnlock()
}

// reverse is true, small to big; reverse is false, big to small
func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	// 计算每个 key 的哈希值，计算出对应的锁 index
	lockSet := make(map[uint32]struct{})
	for _, key := range keys {
		hashcode := fnv32(key)
		index := locks.spread(hashcode)
		lockSet[index] = struct{}{}
	}

	indices := make([]uint32, 0, len(lockSet))
	for index := range lockSet {
		indices = append(indices, index)
	}

	sort.Slice(indices, func(i, j int) bool {
		if reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})

	return indices
}

func (locks *Locks) Locks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		locks.tables[index].Lock()
	}
}

func (locks *Locks) Unlocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		locks.tables[index].Unlock()
	}
}

func (locks *Locks) RLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		locks.tables[index].RLock()
	}
}

func (locks *Locks) RUnlocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		locks.tables[index].RUnlock()
	}
}

func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	// 计算所有需要加锁的下标
	keys := append(readKeys, writeKeys...)
	indices := locks.toLockIndices(keys, false)
	// 计算写锁对应的下标
	writeIndices := make(map[uint32]struct{}, len(writeKeys))
	for _, writeKey := range writeKeys {
		writeIndices[locks.spread(fnv32(writeKey))] = struct{}{}
	}
	// 遍历所有加锁下标，进行加锁，若需要加写锁则优先加写锁
	for _, index := range indices {
		if _, needWrite := writeIndices[index]; needWrite {
			// 需要加写锁
			locks.tables[index].Lock()
			continue
		}
		// 加读锁
		locks.tables[index].RLock()
	}
}

func (locks *Locks) RWUnlocks(writeKeys []string, readKeys []string) {
	// 计算所有需要解锁的下标
	keys := append(readKeys, writeKeys...)
	indices := locks.toLockIndices(keys, true)
	// 计算写锁对应的下标
	writeIndices := make(map[uint32]struct{}, len(writeKeys))
	for _, writeKey := range writeKeys {
		writeIndices[locks.spread(fnv32(writeKey))] = struct{}{}
	}
	// 遍历所有加锁下标，进行解锁
	for _, index := range indices {
		if _, needWrite := writeIndices[index]; needWrite {
			// 需要解写锁
			locks.tables[index].Unlock()
			continue
		}
		// 解读锁
		locks.tables[index].RUnlock()
	}
}
