package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ConcurrentDict struct {
	table      []*shard
	count      int64
	shardCount int
}

type shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

// 得到大于等于 param 的最小2次幂作为容量（最小16）
// 比如 input: 31 output:32
// input: 60 output: 64
// input: 5 output: 16
func computeCapacity(param int) (size int) {
	if param <= 16 {
		// 最小 16
		return 16
	}

	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

func MakeConcurrentDict(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}

	return &ConcurrentDict{
		table:      table,
		count:      0,
		shardCount: shardCount,
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

// 检查 ConcurrentDict 是否没有初始化，没有初始化则返回 true
func (c *ConcurrentDict) notInit() bool {
	return c == nil || c.table == nil
}

func (c *ConcurrentDict) spread(hashcode uint32) uint32 {
	if c.notInit() {
		panic("dict is nil")
	}

	tableSize := uint32(len(c.table))

	return hashcode % tableSize
}

func (c *ConcurrentDict) getShard(index uint32) *shard {
	if c.notInit() {
		panic("dict is nil")
	}

	return c.table[index]
}

func (c *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	// 首先计算 key 的哈希
	if c.notInit() {
		panic("dict is nil")
	}
	hashcode := fnv32(key)
	// 根据哈希值计算 shard 的下标
	index := c.spread(hashcode)
	// 根据下标得到 shard
	s := c.getShard(index)
	// 在 shard 中读取数据，需要加锁
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	val, exists = s.m[key]
	return
}

func (c *ConcurrentDict) Len() int {
	if c.notInit() {
		panic("dict is nil")
	}

	return int(atomic.LoadInt64(&c.count))
}

// Put 返回新增kv的数量
func (c *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if c.notInit() {
		panic("dict is nil")
	}
	// 首先计算 key 对应的哈希值
	hashcode := fnv32(key)
	// 获取 shard
	index := c.spread(hashcode)
	s := c.getShard(index)
	// put 操作，加锁
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// 存在，更新
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}

	// 不存在，新增
	s.m[key] = val
	c.addCount()

	return 1
}

// PutIfAbsent 如果不存在就新增，不做更新操作，返回新增的数量
func (c *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if c.notInit() {
		panic("dict is nil")
	}
	// 首先计算 key 对应的哈希值
	hashcode := fnv32(key)
	// 获取 shard
	index := c.spread(hashcode)
	s := c.getShard(index)
	// put 操作，加锁
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// 存在，更新
	if _, ok := s.m[key]; ok {
		return 0
	}

	// 不存在，新增
	s.m[key] = val
	c.addCount()

	return 1
}

// PutIfExists 只做更新操作，返回更新的数量
func (c *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if c.notInit() {
		panic("dict is nil")
	}
	// 首先计算 key 对应的哈希值
	hashcode := fnv32(key)
	// 获取 shard
	index := c.spread(hashcode)
	s := c.getShard(index)
	// put 操作，加锁
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// 存在，更新
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}

	// 不存在，不做任何操作
	return 0
}

// Remove 删除，返回删除的数量
func (c *ConcurrentDict) Remove(key string) (result int) {
	if c.notInit() {
		panic("dict is nil")
	}
	// 首先计算 key 对应的哈希值
	hashcode := fnv32(key)
	// 获取 shard
	index := c.spread(hashcode)
	s := c.getShard(index)
	// 删除操作，加锁
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// 找到了，删除
	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		c.decreaseCount()
		return 1
	}

	// 没找到，不用删除
	return 0
}

func (c *ConcurrentDict) ForEach(consumer Consumer) {
	if c.notInit() {
		panic("dict is nil")
	}

	for _, s := range c.table {
		s.mutex.RLock()
		func() {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				if !consumer(key, value) {
					break
				}
			}
		}()
	}
}

func (c *ConcurrentDict) Keys() []string {
	keys := make([]string, 0, c.Len())
	c.ForEach(func(key string, val interface{}) bool {
		keys = append(keys, key)
		return true
	})

	return keys
}

// RandomKey 随机返回一个 key
func (s *shard) RandomKey() string {
	if s == nil {
		panic("shard is nil")
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for key := range s.m {
		return key
	}

	return ""
}

// RandomKeys 随机返回最多 limit 个 key，可能包含重复的key
func (c *ConcurrentDict) RandomKeys(limit int) []string {
	if c.notInit() {
		panic("dict is nil")
	}

	size := c.Len()
	if size <= limit {
		return c.Keys()
	}

	keys := make([]string, limit)
	shardCount := c.shardCount
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		index := nR.Intn(shardCount)
		s := c.getShard(uint32(index))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			keys[i] = key
			i++
		}
	}

	return keys
}

// RandomDistinctKeys 随机返回最多 limit 个 key，不包含重复的key
func (c *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	if c.notInit() {
		panic("dict is nil")
	}

	size := c.Len()
	if size <= limit {
		return c.Keys()
	}

	keys := make([]string, limit)
	keySet := make(map[string]struct{}, limit) // 记录已经选到的 key 集合，用于去重
	shardCount := c.shardCount
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		index := nR.Intn(shardCount)
		s := c.getShard(uint32(index))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if _, exist := keySet[key]; key != "" && !exist {
			keys[i] = key
			keySet[key] = struct{}{}
			i++
		}
	}

	return keys
}

func (c *ConcurrentDict) Clear() {
	*c = *MakeConcurrentDict(c.shardCount)
}

func (c *ConcurrentDict) addCount() int64 {
	return atomic.AddInt64(&c.count, 1)
}

func (c *ConcurrentDict) decreaseCount() int64 {
	return atomic.AddInt64(&c.count, -1)
}
