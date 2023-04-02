# Simple-Redis

本项目完整地址 [simple-redis](https://github.com/dawnzzz/simple-redis)

Simple-Redis 是一个 golang 编写的实现了 RESP（REdis Serialization Protocol）协议的简易 Redis，旨在通过自己的方式实现 Redis 缓存数据库的功能。

与 Redis 不同的是，**simple-redis 支持原子性事务**，并且还支持分布式原子性事务（配置文件中 open_atomic_tx 开启原子性事务）。

项目文件目录安排如下：

``` bash
├─config
├─database	# 数据库核心功能
│  ├─cluster	# 集群相关功能
│  │  └─tcc		# 分布式事务
│  ├─commands	# 数据库引擎实现
│  ├─engine
│  └─rdb	# 持久化
│      └─aof
├─datastruct	# 底层数据结构实现
│  ├─dict
│  ├─list
│  ├─lock
│  ├─set
│  └─sortedset
├─docs
├─interface		# 接口定义
│  ├─cluster
│  ├─database
│  ├─redis
│  └─tcp
├─lib
│  ├─consistenthash		# 一致性哈希
│  ├─pool	# 连接池
│  ├─sync	# 同步功能
│  │  ├─atomic
│  │  └─wait
│  ├─timewheel	# 时间轮算法
│  └─utils
├─logger
├─redis
│  ├─client
│  ├─connection
│  ├─parser		# RESP 协议解析器
│  ├─protocol
│  │  └─reply
│  └─server
└─tcp	# TCP服务器
```

## 使用方法

首先安装依赖：

``` bash
go mod tidy
```

### 服务器端

编译服务器端：

``` bash
go build main.go -o simple-redis
```

编译之后使用 `-f` 参数指定配置文件的位置，默认为 `config.yaml`：

``` bash
simple-redis -f config.yaml
```

集群配置文件如 `cluster_config1.yaml`、`cluster_config2.yaml`、`cluster_config3.yaml` 所示。

### 客户端

使用 Redis 的客户端进行连接（在使用 Redis 客户端进行连接时，若服务器设置了 keepalive 检测，若超过 keepalive 不发送消息则会断开连接）：

``` bash
redis-cli -h '127.0.0.1' -p 6179
```

## 可使用的命令

### db

- Select index：选择数据库，在 multi 时无法使用此命令
- BGRewriteAof：异步进行 AOF 持久化
- RewriteAof：同步进行 AOF 持久化操作
- Multi：开启一个事务命令队列
- Exec：执行队列中的命令
- Discard：放弃执行队列中的命令
- Watch key：监视 key，若 key 的版本号变化则放弃执行队列中的命令
- UnWatch key：取消监视 key 的版本号
- auth password：身份认证

### key

- Del key：删除某个 key
- Exist key：判断 key 是否存在
- ExpireAt key timestamp：指定过期时间
- Expire key seconds：指定过期秒数
- Persist key：取消 key 的过期时间
- KeyVersion key：获取 key 的版本号（在分布式事务中应用）

### string

- Set key value：设置 key 对应的 value
- Get key：获取 Key 对应的 value
- StrLen：获取 Key 对应 value 的长度
- SetNX key value：当 key 不存在时设置 value
- SetEX key value ttl：设置 key-value 的同时，设置其过期时间
- Append key appendValue：在 key 的 value 后面追加数据
- Incr key：为 key 的 value 加一
- Decr key：为 key 的 value 减一
- IncrBy key by：为 key 的 value 加 by
- DecrBy key by：为 key 的 value 减去 by

### hash

- HSet key field value：设置哈希表 key 中 field 的 value 值
- HSetNX key field value：当 field 不存在时才设置其值
- HGet key field：获取 field 对应的 value
- HExists key field：查询 field 是否在 key 中
- HGetAll key：获取 key 中所有的 field 和 value
- HIncrBy key field by：使得 key 中 field 的 value 加上 by
- HKeys key：获取 key 中所有的 field
- HVals key：获取 key 中所有的 value
- HLen key：获取 field 的个数

### set

- SAdd key member1 [member2 ...]：向集合 key 中添加元素
- SCard key：获取集合长度
- SDiff key1 [key2 ...]：获取第一个集合 key1 与其他集合的差异
- SInter key1 [key2 ...]：获取集合的交集
- SUnion key1 [key2 ...]：获取集合的并集
- SIsMember key member：查询 member 是否在集合 key 中
- SMembers key：获取集合 key 中所有的 member
- SPop key [count]：从集合 key 中随机弹出 count 个元素，count 默认为 1
- SRandMember key [count]：随机返回集合中的 count 个元素，count 默认为 1
- SRem key member1 [member2 ...]：删除集合中的 member

### list

- LPush key member1 [member2 ...]：从左侧向列表 key 推入元素 member
- RPush key member1 [member2 ...]：从右侧向列表 key 推入元素 member
- LPushX key member：当列表 key 存在时，向左侧推入 member
- RPushX key member：当列表 key 存在时，向右侧推入 member
- LPop key：从 key 的左侧弹出一个元素
- RPop key：从 key 的右侧弹出一个元素
- LIndex key index：获取列表 key 在 index 位置上的值
- LLen key：获取列表 key 的长度
- LRem key count value：删除 key 中值等于 value 的元素，count 为 0 代表全部删除，count 大于 0 代表从左到右删除的次数，count 小于 0 代表从右向左删除的次数
- LTrim key start end：删除列表 key 中坐标在 [start, end] 区间内的元素
- LRange key start end：获取列表 key 中坐标在 [start, end] 区间内的元素
- LSet  key index value：将列表 key 坐标 index 位置上的元素设置为 value

### zset

- ZAdd key score member1 [score2 member2 ...]：向有序集合中添加一个或者多个元素
- ZCard key：获取有序集合的长度
- ZScore key member：获取有序集合 key 中 member 对应的 分数
- ZCount key min max：获取分数在区间内元素的个数
- IncrBy key member by：令有序集合 key 中元素 member 的值加上 by
- ZRange key start stop [WithScores]：通过索引区间返回有序集合指定区间内的成员。
- ZRevRange key start stop [WithScores]：返回有序集中指定区间内的成员，通过索引，分数从高到低
- ZRangeByScore key min max [WithScores] [Limit offset count]：返回有序集合中指定分数区间的成员列表，有序集成员按分数值递增顺序排列
- ZRevRangeByScore key min max [WithScores] [Limit offset count]：返回有序集合中指定分数区间的成员列表，有序集成员按分数值递减顺序排列
- ZRank key member：返回有序集中指定成员按照从小到大顺序的排名
- ZRevRank key member：返回有序集中指定成员按照从大到小顺序的排名
- ZRem key member1 [member2 ...]：删除有序集合中一个或者多个成员
- ZRemRangeByRank key start stop：移除有序集合中给定的排名区间的所有成员
- ZRemRangeByScore key min max：移除有序集合中给定的分数区间的所有成员

## 详细文档目录

[1-TCP服务器](docs/1-TCP服务器.md)

[2-客户端](docs/2-客户端.md)

[3-协议解析器](docs/3-协议解析器.md)

[4-数据结构之Hash表和LockMap](docs/4-数据结构之Hash表和LockMap.md)

[5-数据结构之List和SortedSet](docs/5-数据结构之List和SortedSet.md)

[6-simple-redis服务器](docs/6-simple-redis服务器.md)

[7-底层数据库](docs/7-底层数据库.md)

[8-AOF持久化](docs/8-AOF持久化.md)

## TODO

- [x] string 实现
- [x] aof 持久化
- [x] 集群
- [x] 单机事务
- [x] 单机原子性事务
- [x] 单机订阅/发布
- [x] 集群订阅/发布
- [x] hash 实现
- [x] set 实现
- [x] zset 实现
- [x] list 实现
- [ ] rdb 持久化
- [x] 分布式事务
- [x] 分布式原子性事务