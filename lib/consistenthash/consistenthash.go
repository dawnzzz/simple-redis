package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// HashFunc defines function to generate hash code
type HashFunc func([]byte) uint32

type Map struct {
	hashFunc HashFunc // 哈希函数
	replicas int      // 虚拟节点
	keys     []int
	hashMap  map[int]string // 保存哈希值与实际节点的映射关系
}

func New(replicas int, fn HashFunc) *Map {
	hashFunc := crc32.ChecksumIEEE
	if fn != nil {
		hashFunc = fn
	}

	return &Map{
		hashFunc: hashFunc,
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
}

func (m *Map) IsEmpty() bool {
	return m.keys == nil || len(m.keys) == 0
}

// AddNodes 在哈希环上添加节点
func (m *Map) AddNodes(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}

		for i := 0; i < m.replicas; i++ { // 每一个节点都有虚拟节点
			hash := int(m.hashFunc([]byte(key + "-" + strconv.Itoa(i)))) // key = key-i
			m.keys = append(m.keys, hash)
			// 记录在哈希环中
			m.hashMap[hash] = key
		}
	}

	sort.Ints(m.keys) // 排序
}

// PickNode 计算key对应的节点
func (m *Map) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	// 计算key的节点
	hash := int(m.hashFunc([]byte(key)))

	// 根据hash在哈希环上的位置，顺时针找到的第一个节点就是对应的节点
	index := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	if index == len(m.keys) {
		index = 0
	}

	return m.hashMap[index]
}
