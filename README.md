# Simple-Redis

本项目启发于 [Godis](https://github.com/HDT3213/godis)

Simple-Redis 是一个 golang 编写的实现了 RESP（REdis Serialization Protocol）协议的简易 Redis，旨在通过自己的方式实现 Redis 缓存数据库的功能。

项目文件目录安排如下：

``` bash
├─config
├─database	# 数据库核心功能
│  ├─cluster	# 集群相关功能
│  ├─commands	# 各命令如get、set实现方式
│  ├─engine	# 数据库引擎实现
│  └─rdb	# 持久化
│      └─aof
├─datastruct	# 底层数据结构实现
│  ├─dict
│  └─lock
├─docs
├─interface		# 接口定义
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
├─logger	# 日志
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

可以使用本项目编写的客户端，编译命令为：

``` bash
go build client.go -o client
```

编译完成之后使用 `-h` 参数指定服务器的 IP 地址，`-p` 参数指定服务器开放的端口号：

``` bash
client -h '127.0.0.1' -p 6179
```

也可以使用 Redis 的客户端进行连接：

``` bash
redis-cli -h '127.0.0.1' -p 6179
```

## TODO

- [x] string 实现
- [x] aof 持久化
- [x] 集群
- [x] 单机事务
- [ ] 订阅/发布
- [ ] hash 实现
- [ ] set 实现
- [ ] zset 实现
- [ ] list 实现
- [ ] rdb 持久化
- [ ] 分布式集群事务