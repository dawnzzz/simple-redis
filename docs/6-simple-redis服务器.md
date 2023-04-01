本项目完整地址 [simple-redis](https://github.com/dawnzzz/simple-redis)

# Simple-Redis 服务器

在之前已经介绍了 TCP 服务器，本节介绍 Simple-Redis 服务器，这是一个**应用层服务器**。在 Handler 的 Handle 方法中，有这样一条命令 `result := h.db.Exec(client, r.Args)`，它将收到的命令交给 simple-redis 服务器去执行。

## 数据结构

simple-redis 服务器的被定义在 database/server.go 文件中，simple-redis 服务器的相关代码在 database 文件夹下。

simple-redis 服务器的数据结构如下，需要说明的是：

- dbSet：代表底层的数据库。
- AofPersister：AOF 持久化。
- AofFileSize：用于记录上一次 AOF 重写后的文件大小。
- rewriteWait、rewriting：用于同步 AOF 重写过程。
- closed：接收关闭信号，用于优雅的关闭（用于关闭自动 AOF 重写协程）。
- cluster：集群相关。
- publish：订阅发布相关操作。

```go
type Server struct {
   dbSet        []*atomic.Value // *DB
   AofPersister *aof.Persister  // AOF 持久化
   AofFileSize  int64
   rewriteWait  sync.WaitGroup
   rewriting    atomic.Bool
   closed       chan struct{}
   cluster      *cluster.Cluster
   publish      publish.Publish
}
```

> AOF 持久化、集群、发布订阅会在后面的章节中说明。

### 构造函数

Handler 会根据配置文件中是否配置了 peers 来开启单机 simple-redis 服务器或者集群 simple-redis 服务器，所以 Server 有**两个构造函数**。在创建集群服务器时，Server.cluster 会被初始化，并添加 Peers（单机模式下为 nil）。

```go
// NewStandaloneServer creates a standalone redis server
func NewStandaloneServer() *Server {
   server := initServer()

   return server
}

// NewClusterServer 创建一个集群服务器
func NewClusterServer(peers []string) *Server {
   server := initServer()

   // 加入集群
   cluster := cluster.NewCluster(config.Properties.Self)
   cluster.AddPeers(peers...)
   if cluster == nil {
      logger.Fatalf("please set 'self'(self ip:port) in conf file")
   }
   server.cluster = cluster

   return server
}
```

### initServer

其中 initServer 函数是两个构造器共有的步骤，包括：

- 初始化数据库。
- 如果开启了 AOF 持久化：
  - 还需要读取 AOF 持久化文件，开启 AOF 持久化。
  - 如果开启了自动 AOF 重写，则会启动一个协程进行自动 AOF 重写。

```go
// server 的通用初始化操作初始化
func initServer() *Server {
   // 初始化数据库
   server := &Server{
      closed: make(chan struct{}, 1),
   }
   if config.Properties.Databases <= 0 {
      config.Properties.Databases = 16 // default is 16
   }
   server.dbSet = make([]*atomic.Value, config.Properties.Databases)
   for i := range server.dbSet {
      singleDB := engine.MakeDB()
      singleDB.SetIndex(i)
      holder := &atomic.Value{}
      holder.Store(singleDB)
      server.dbSet[i] = holder
   }

   // 读取 AOF 持久化文件
   if config.Properties.AppendOnly {
      if config.Properties.AofFilename == "" { // default is dump.aof
         config.Properties.AofFilename = "dump.aof"
      }

      // 获取初始AOF文件大小
      server.AofFileSize = utils.GetFileSizeByName(config.Properties.AofFilename)

      // 开启 AOF 持久化
      AofPersister, err := aof.NewPersister(server, config.Properties.AofFilename, true, config.Properties.AofFsync, MakeAuxiliaryServer)
      if err != nil {
         logrus.Fatal(err)
      }
      server.bindPersister(AofPersister)

      // 自动 AOF 重写
      if config.Properties.AutoAofRewrite {
         if config.Properties.AutoAofRewritePercentage <= 0 {
            config.Properties.AutoAofRewritePercentage = 100
         }
         if config.Properties.AutoAofRewriteMinSize <= 0 {
            config.Properties.AutoAofRewriteMinSize = 64
         }

         // 开启 AOF 自动重写
         go server.autoAofRewrite()
      }
   }

   return server
}
```

> Server.autoAofRewrite 用于开启 AOF 自动重写，它会启动一个计时器，每 10 秒钟检查一次：
>
> - 如果当前正在重写，则跳过这个周期。
> - 当前如果没有重写，则首先会检查 AOF 文件的大小需不需要重写（重写条件在配置文件中配置，可以配置重写的最小 AOF 文件大小和体积超过上次重写后 AOF 文件大小的百分比时进行重写）。若需要重写，开启一个协程 Server.AofPersister.Rewrite 进行异步的重写工作，在重写结束后更新 Server.AofFileSize。
> - 当收到 Server.closed 发送的关闭消息后，协程退出。

## 执行命令方法 Server.Exec

因为有两种 simple-redis 服务器，所以**在执行命令时也有两种执行方式**，分别是单机模式下执行和集群模式下执行。

```go
func (s *Server) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
   if s.cluster != nil {
      return s.execCluster(client, cmdLine)
   }

   return s.execStandalone(client, cmdLine) // 单机模式
}
```

### execStandalone

Server.execStandalone 方法用于在单机模式下执行命令。

```go
// 单机模式执行命令的方式
func (s *Server) execStandalone(client redis.Connection, cmdLine [][]byte) redis.Reply
```

而**执行的命令又分为两种类型**，一种是如 AUTH、SELECT、PING 以及 AOF 持久化、订阅发布等**在 simple-redis 服务器层面执行的命令**；而第二种则是如 SET、GET 等**在具体某个数据库中执行的命令**。

对于第一种在 simple-redis 服务器层面执行的命令，在 execStandalone 函数中会**直接定义执行**：

```go
cmdName := strings.ToLower(string(cmdLine[0]))

if (cmdName) == "ping" {
   logger.Debugf("received heart beat from %v", client.Name())
   return reply.MakePongStatusReply()
}

if _, ok := client.(*connection.FakeConn); !ok { // fakeConn不做校验
   if cmdName == "auth" {
      return Auth(client, cmdLine[1:])
   }
   if !isAuthenticated(client) {
      return reply.MakeErrReply("NOAUTH Authentication required")
   }
}
switch cmdName {
case "select":
   return SelectDB(client, cmdLine[1:], len(s.dbSet))
case "bgrewriteaof":
   return BGRewriteAof(s, cmdLine[1:])
case "rewriteaof":
   return RewriteAof(s, cmdLine[1:])
case "multi":
   return StartMultiStandalone(client, cmdLine[1:])
case "exec":
   return ExecMultiStandalone(s, client, cmdLine[1:])
case "discard":
   return DiscardMultiStandalone(client, cmdLine[1:])
case "watch":
   return ExecWatchStandalone(s, client, cmdLine[1:])
case "unwatch":
   return ExecUnWatchStandalone(client, cmdLine[1:])
case "publish":
   return Publish(s, cmdLine[1:])
case "subscribe":
   return Subscribe(s, client, cmdLine[1:])
case "unsubscribe":
   return UnSubscribe(s, client, cmdLine[1:])
case "pubsub":
   return PubSub(s, cmdLine[1:])
}
```

第二种在具体某个数据库中执行的命令，则会**选择客户端当前所在的数据库**，将命令**送给相应的数据库中执行**。

```go
// normal commands
dbIndex := client.GetDBIndex()
selectedDB, errReply := s.selectDB(dbIndex)
if errReply != nil {
   return errReply
}
return selectedDB.Exec(client, cmdLine)
```

### execCluster

Server.execCluster 方法用于在集群模式下执行命令。

```go
// 集群模式执行命令的方式
func (s *Server) execCluster(client redis.Connection, cmdLine [][]byte) redis.Reply
```

同样的，在集群模式下**执行的命令又分为两种类型**，一种是如 AUTH、SELECT、PING 以及 AOF 持久化、订阅发布等**在 simple-redis 服务器层面执行的命令**；而第二种则是如 SET、GET 等**在具体某个数据库中执行的命令**。

对于第一种在 simple-redis 服务器层面执行的命令，在 execCluster 函数中会**直接定义执行**。对于第二种命令，则会**调用 Server.cluster.Exec 方法进行执行（在集群模式下，不一定在本地执行，有可能远程执行）**。

## 关闭 simple-redis 服务器

调用 Server.Close 方法会关闭 simple-redis 服务器，首先会向 Server.closed 发送一个消息，关闭自动重写协程；接着关闭 AOF 持久化、关闭集群、关闭发布订阅。

```go
func (s *Server) Close() {
   s.closed <- struct{}{}
   if config.Properties.AppendOnly {
      s.AofPersister.Close() // 关闭aof持久化
   }

   if s.cluster != nil {
      s.cluster.Close()
   }

   s.publish.Close()
}
```