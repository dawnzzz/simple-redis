本项目完整地址 [simple-redis](https://github.com/dawnzzz/simple-redis)

# Hash 表

KV 内存数据库的核心是并发安全的哈希表，simple-redis 采用**分段锁**策略。**将 key 分散到固定数量的 shard 中，在一个 shard 内的读写操作不会阻塞其他 shard 内的操作**。

## 数据结构

定义 Dict 接口，该接口表示一个哈希表的操作：

```go
// Consumer is used to traversal dict, if it returns false the traversal will be break
type Consumer func(key string, val interface{}) bool

// Dict is interface of a key-value data structure
type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int
	Put(key string, val interface{}) (result int)
	PutIfAbsent(key string, val interface{}) (result int)
	PutIfExists(key string, val interface{}) (result int)
	Remove(key string) (result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
}
```

定义 ConcurrentDict 实现了 Dict 接口，ConcurrentDict 为并发安全的哈希表。**将 key 映射到不同的 shard 中，不同 shard 上的操作是不会相互影响的**，提高了**并发性**。

> 注意，这里 shard 的数量恒为 2 的整数幂。

```go
type ConcurrentDict struct {
	table      []*shard
	count      int64
	shardCount int
}

type shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}
```

## 定位 shard

将 key 映射为 shard 的哈希函数采用 FNV 算法。

> **FNV 能快速 hash 大量数据并保持较小的冲突率**，它的高度分散使它**适用于 hash 一些非常相近的字符串**。

```go
const prime32 = uint32(16777619)

func fnv32(key string) uint32 {
   hash := uint32(2166136261)
   for i := 0; i < len(key); i++ {
      hash *= prime32
      hash ^= uint32(key[i])
   }
   return hash
}
```

依据 key 的哈希值定位 shard（确定 shard 的 index），**当 n 为 2 的整数幂时** `h % n == (n - 1) & h`。

- spread 方法根据哈希值，返回 shard 的下标。
- getShard 方法根据下标值，返回对应的 shard。

```go
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
```

## Get 和 Put 方法

以 Get 和 Put 方法为例，说明 ConcurrentDict 的实现。

### Get

Get 方法用于查询数据，给定一个 key，返回对应的值。其流程为：

- 计算 key 的哈希值，根据**哈希值找到对应的 shard**。
- 对这个 shard 上**读锁**，在 map 中查询数据。

```go
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
```

### Put

Put 方法用于插入数据，给定 key 和 value 将键值对插入到某一个 shard 的 map 中。其流程为：

- 计算 key 的哈希值，根据**哈希值找到对应的 shard**。
- 对这个 shard 上**写锁**，在 map 中写入键值对。
- 新增数据返回 1，更新数据返回 0。

```go
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
```

# LockMap

**ConcurrentDict 可以保证单个 key 操作的并发安全性**，但是仍无法满足需求。

> 比如：
>
> - 在发出 MULTI 命令之后的所有的命令都送入命令队列中一起性形成一个事务进行执行，因此需要**锁定所有给定的键**直到完成所有键的查询和设置。

因此，需要实现 LockMap，用于锁定一个或者一组 key。

## 数据结构

多个键可以共用一个哈希槽（与 ConcurrentDict shard 并不是一一对应的，应该小于等于 ConcurrentDict 的 shard），不为单个 key 加锁而是**为它所在的哈希槽加锁**。

```go
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
```

在定位哈希槽时，同样**对 key 做 FNV 哈希函数**之后选择哈希槽。

## 为一个 key 加锁

为一个 key 加锁时非常简单，就是对 key 做哈希之后选择相应的 shard，之后在对应的 shard 上加锁即可。

## 锁定多个 key

在锁定多个 key 时，**所有协程都按照相同顺序加锁**，就可以避免循环等待防止死锁。

### 确定加锁顺序

一次性锁定多个 key 之前，首先确定加锁顺序，这样可以避免死锁。

- 对要加锁的**每一个 key 都做哈希函数**，确定其对应的 shard 的下标（这里用 map 实现 set，避免重复的下标）。
- 对**下标进行排序**，即可得到一个确定的加锁顺序。

```go
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
```

### 为多个 key 加读写锁

确定了加锁顺序之后，就可以为多个 key 加锁了。这里以加读写锁为例进行讲解：

```go
func (locks *Locks) RWLocks(writeKeys []string, readKeys []string)
```

流程如下：

- 首先不管是需要读的 key，还是需要写的 key，全部**计算出加锁顺序**。

```go
// 计算所有需要加锁的下标
keys := append(readKeys, writeKeys...)
indices := locks.toLockIndices(keys, false)
```

- 接着得到**需要加写锁的 shard 下标**。

```go
// 计算写锁对应的下标
writeIndices := make(map[uint32]struct{}, len(writeKeys))
for _, writeKey := range writeKeys {
	writeIndices[locks.spread(fnv32(writeKey))] = struct{}{}
}
```

- **按照顺序加锁**，检查这个 shard 是否需要加上写锁，如果需要就加写锁否则加上读锁。

```go
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
```

### 为多个 key 解读写锁

这里以解读写锁为例进行讲解：

```go
func (locks *Locks) RWUnLocks(writeKeys []string, readKeys []string)
```

流程如下：

- 首先不管是需要读的 key，还是需要写的 key，**全部计算出解锁顺序（解锁顺序与加锁顺序相反）**。
- 接着得到**需要解写锁的 shard 下标**。
- **按照顺序解锁**，检查这个 shard 是否需要解开写锁，如果需要就解写锁否则解读锁。