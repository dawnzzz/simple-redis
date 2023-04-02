本项目完整地址 [simple-redis](https://github.com/dawnzzz/simple-redis)

本节说明 simple-redis 中的 AOF 持久化功能。AOF（append only file）是一种 Redis 持久化方式。，其优缺点总结如下：

- **优势：**
  - 持久化文件是用户可以理解的。
  - 备份机制更稳健，**丢失数据概率更低。**
  - AOF日志文件的命令通过**可读**的方式进行**记录**，这个特性非常适合做**灾难性的误删除的紧急恢复**。比如某人不小心使用了flushall清空了所有数据库，只有重写操作还没有发生，那么就可以立即拷贝AOF文件，将最后一条flushall命令给删了，然后再将该AOF文件放回去，就可以通过恢复机制，自动恢复所有数据。
- **劣势：**
  - 比起RDB占用**更多的磁盘空间。**
  - 恢复备份**速度慢**。
  - 每次读写都同步的话，有一定的性能压力。

# AOF 持久化

## 数据结构

Persister 是 AOF 持久化中的核心数据结构，它从 channel 中接收消息并且将消息写入到 AOF 文件中。其中重要的字段如下：

- **db：**指向 simple-redis 服务器。
- **tmpDBMaker：**临时数据库创建函数，在进行 AOF 重写时，需要建立一个临时数据库加载 AOF 持久化文件，通过遍历临时数据库中的 key 实现 AOF 持久化文件的重写压缩。
- **aofChan：**需要持久化的命令（payload 包含命令、数据库编号两个字段）发送到这个管道上进行持久化。
- **aofFilename：**AOF 持久化文件名。
- **aofFsync：**AOF 刷盘策略，共有三种策略分别是FsyncAlways、FsyncEverySec、FsyncNo。
- **currentDB：**当前数据库编号。

```go
const (
   FsyncAlways   = iota // 每一个命令都会进行刷盘操作
   FsyncEverySec        // 每秒进行一次刷盘操作
   FsyncNo              // 不主动进行刷盘操作，交给操作系统去决定
)

type CmdLine [][]byte

const (
   aofQueueSize = 1 << 16
)

type Persister struct {
   ctx         context.Context
   cancel      context.CancelFunc
   db          database.DBEngine
   tmpDBMaker  func() database.DBEngine
   aofChan     chan *payload
   aofFile     *os.File
   aofFilename string
   aofFsync    int // AOF 刷盘策略
   // aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
   aofFinished chan struct{}
   // pause aof for start/finish aof rewrite progress
   pausingAof sync.Mutex
   // 表示正在aof重写，同时只有一个aof重写
   aofRewriting sync.WaitGroup
   currentDB    int
}
```

payload 包含命令、数据库编号两个字段，它表示发送给 aofChan 的数据。

``` go
type payload struct {
	cmdLine CmdLine
	dbIndex int
}
```

## 持久化流程

### LoadAof 加载持久化文件

Persister.LoadAof 用于读取 AOF文件，这个方法在监听 aofChan 之前使用。

```go
// LoadAof 用于读取 AOF文件，这个方法在监听 aofChan 之前使用。
func (persister *Persister) LoadAof(maxBytes int64) {
```

其流程如下：

- **首先将 aofChan 设置为 nil**，因为 persister.db.Exec 在**执行 AOF 文件中的命令时，可能又会向 aofChan 中加入命令**。些命令是不需要加入到 aofChan 中的（加入 aofChan 中数据会出错，因为这算是又在 AOF 文件中记录了一次）。

```go
aofChan := persister.aofChan
persister.aofChan = nil
defer func(aofChan chan *payload) {
   persister.aofChan = aofChan
}(aofChan)
```

- 打开 AOF 文件，从 AOF 文件中读取 maxBytes 字节的数据。

```go
file, err := os.Open(persister.aofFilename)
if err != nil {
   if _, ok := err.(*os.PathError); ok {
      return
   }
   logger.Warn(err)
   return
}
defer file.Close()

// 打开 AOF 文件，从 AOF 文件中读取 maxBytes 字节的数据。
var reader io.Reader
if maxBytes > 0 {
   reader = io.LimitReader(file, int64(maxBytes))
} else {
   reader = file
}
```

- 读取 AOF 文件复用了协议解析器，fakeConn 仅仅用于持久化操作中（它表示一个**虚拟的客户端连接**，仅仅用于执行 AOF 文件中的命令）。

```go
// 读取 AOF 文件复用了协议解析器，fakeConn 仅仅用于持久化操作中（它表示一个**虚拟的客户端连接**，仅仅用于执行 AOF 文件中的命令）。
ch := parser.ParseStream(reader)
for p := range ch {
   if p.Err != nil {
      if p.Err == io.EOF {
         // aof file read finish
         break
      }
      logger.Error("parse error: " + p.Err.Error())
      continue
   }

   if p.Data == nil {
      logger.Error("empty payload")
      continue
   }

   // 执行
   r, ok := p.Data.(*reply.MultiBulkStringReply)
   fakeConn := connection.NewFakeConn()
   if !ok {
      logger.Error("require multi bulk protocol")
      continue
   }
   ret := persister.db.Exec(fakeConn, r.Args)
   if reply.IsErrorReply(ret) {
      logger.Error("exec err", string(ret.ToBytes()))
   }

   // 遇到 select 切换aof当前数据库
   if strings.ToLower(string(r.Args[0])) == "select" {
      // execSelect success, here must be no error
      dbIndex, err := strconv.Atoi(string(r.Args[1]))
      if err == nil {
         persister.currentDB = dbIndex
      }
   }
}
```

### 写入 AOF 文件

需要被持久化的命令都会被送往 Persister.AofChan，当启动 AOF 持久化时，会同时**启动一个协程监听 Persister.aofChan**，调用 Persister.writeAof 方法写入 AOF 文件。

```go
// 监听aofChan，写入 AOF 文件
func (persister *Persister) listenCmd() {
   for p := range persister.aofChan {
      persister.writeAof(p)
   }
   persister.aofFinished <- struct{}{}
}
```

#### writeAof 将一条命令写入 AOF 持久化文件

Persister.writeAof 方法用于将一条命令写入到 AOF 文件中。

```go
func (persister *Persister) writeAof(p *payload)
```

其流程如下：

- 首先，**选择正确的数据库**。每个客户端都可以选择自己的数据库，所以 payload 中要保存客户端选择的数据库。**选择的数据库与 AOF 文件中当前的数据库不一致时写入一条 Select 命令**。

```go
persister.pausingAof.Lock()
defer persister.pausingAof.Unlock()
// 首先，**选择正确的数据库**。
// 每个客户端都可以选择自己的数据库，所以 payload 中要保存客户端选择的数据库。
// **选择的数据库与 AOF 文件中当前的数据库不一致时写入一条 Select 命令**。
if p.dbIndex != persister.currentDB {
   selectCmd := utils.StringsToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
   data := reply.MakeMultiBulkStringReply(selectCmd).ToBytes()
   _, err := persister.aofFile.Write(data)
   if err != nil {
      logger.Warn(err)
      return // skip this command
   }
   persister.currentDB = p.dbIndex
}
```

- 接着**写入命令内容**。

```go
data := reply.MakeMultiBulkStringReply(p.cmdLine).ToBytes()
_, err := persister.aofFile.Write(data)
if err != nil {
   logger.Warn(err)
}
```

- 接着调用 listener.CallBack，如果刷盘策略为 FsyncAlways（每一条命令都刷盘），则调用 persister.aofFile.Sync 刷盘。

```go
// 如果刷盘策略为 FsyncAlways（每一条命令都刷盘），则调用 persister.aofFile.Sync 刷盘。
if persister.aofFsync == FsyncAlways {
   _ = persister.aofFile.Sync()
}
```

### fsyncEverySecond 每秒钟进行刷盘

当刷盘策略（fsync）为 FsyncEverySec 时，表示每秒钟都要进行一次刷盘策略。当开启这个选项时，会额外启动一个协程 Persister.fsyncEverySecond，来进行每秒钟的刷盘操作。

具体实现的思路就是**启动一个一秒钟通知一次 time.Ticker**，来执行 Persister.aofFile.Sync 同步刷盘操作。**当监听到 AOF 持久化结束（Persister.ctx.Done() 收到数据时）**退出线程。

## 关闭持久化

调用 Persister.Close 可以关闭 AOF 持久化，在关闭持久化操作时，需要进行以下步骤：

- 首先**等待 AOF 重写**完成。
- 接着关**闭 aofChan 管道**，等待 AOF 持久化完成。
- 调用 Persister中上下文 cancel 方法，用于**结束 Persister.fsyncEverySecond 方法**。

```go
// Close 关闭 AOF 持久化close
func (persister *Persister) Close() {
   // 等待 AOF 重写完成
   persister.aofRewriting.Wait()

   if persister.aofFile != nil {
      // 先**关闭 aofChan 通道**，接着**等待 AOF 持久化完成**（persister.listenCmd 方法结束），**关闭 AOF 文件句柄**。
      close(persister.aofChan)
      <-persister.aofFinished // wait for aof finished
      err := persister.aofFile.Close()
      if err != nil {
         logger.Warn(err)
      }
   }

   // 调用 persister 中上下文 cancel 方法，用于结束 persister.fsyncEverySecond 方法。
   persister.cancel()
}
```

# AOF 重写

AOF 重写可以减小持久化文件的大小，以删除无用的指令。

**重写必须在固定不变的数据集上进行**，不能直接使用内存中的数据。在 simple-redis 中，采用读取 AOF 文件生成**副本**的方式进行重写操作。

流程如下：

1. **重写开始：**暂停 AOF 写入 ->  准备重写 -> 恢复AOF写入。
2. **执行重写：**重写协程读取 AOF 文件中的**前一部分**（重写开始前的数据，不包括读写过程中写入的数据）并重写到临时文件中。
3. **重写结束：**暂停 AOF 写入 -> 将重写过程中产生的**新数据写入临时文件**中 -> 使用临时文件覆盖 AOF 文件（使用文件系统的 mv 命令保证安全） -> 恢复 AOF 写入。

```go
type RewriteCtx struct {
   tmpFile  *os.File // 重写时用到的临时文件
   fileSize int64    // 重写时文件大小
   dbIndex  int      // 重写时的当前数据库
}

func (persister *Persister) newRewritePersister() *Persister {
   tmpDB := persister.tmpDBMaker()
   return &Persister{
      db:          tmpDB,
      aofFilename: persister.aofFilename,
   }
}

func (persister *Persister) Rewrite(rewriteWait *sync.WaitGroup, rewriting *atomic.Bool) error {
   logger.Info("rewrite start...")
   persister.aofRewriting.Add(1)
   rewriting.Store(true)
   defer persister.aofRewriting.Done()
   defer func() {
      if rewriteWait != nil {
         rewriteWait.Done() // 通知 server 重写结束
      }
      rewriting.Store(false)
      logger.Info("rewrite finished...")
   }()

   rewriteCtx, err := persister.StartRewrite()
   if err != nil {
      return err
   }

   err = persister.DoRewrite(rewriteCtx)
   if err != nil {
      return err
   }

   err = persister.FinishRewrite(rewriteCtx)
   if err != nil {
      return err
   }

   return nil
}
```

## StartRewrite 开始重写

StartRewrite 开始重写流程为：暂停 AOF 写入 ->  准备重写 -> 恢复AOF写入。

- 首先暂停 AOF 写入。

```go
persister.pausingAof.Lock()
defer persister.pausingAof.Unlock()
```

- 接着调用 fsync 将缓冲区数据**落盘**，防止 AOF 文件不完整造成错误。

```go
err := persister.aofFile.Sync()
if err != nil {
   logger.Warn("fsync failed")
   return nil, err
}
```

- 获取当前 AOF 文件大小，创建临时文件。

```go
// 获取当前aof文件大小
fileStat, _ := os.Stat(persister.aofFilename)
fileSize := fileStat.Size()

// 创建临时文件
tmpFile, err := ioutil.TempFile("./", "*.aof")
if err != nil {
   logger.Warn("tmp file create failed")
   return nil, err
}

return &RewriteCtx{
   tmpFile:  tmpFile,
   fileSize: fileSize,
   dbIndex:  persister.currentDB,
}, nil
```

## DoRewrite 执行重写

DoRewrite 用于重写协程读取 AOF 文件中的**前一部分**（重写开始前的数据，不包括读写过程中写入的数据）并重写到临时文件中。流程如下：

- **读取 AOF 文件**在重写开始时获取到的文件大小长度的数据，这些数据是重写开始前的数据，**将重写的数据加载进入内存**。

```go
tmpFile := rewriteCtx.tmpFile

rewritePersister := persister.newRewritePersister()
rewritePersister.LoadAof(rewriteCtx.fileSize)
```

- 依次将每一个数据库中的数据，**重写进入临时的 AOF 文件**中。
  - 对于每一个数据库，首先在临时文件中写入 Select 命令**选择正确的数据库**。
  - 调用 foreach 函数，遍历数据库中的每一个 key，将每一个键值对写入到临时文件中。

```go
// 依次将每一个数据库中的数据，**重写进入临时的 AOF 文件**中。
for i := 0; i < config.Properties.Databases; i++ {
   // 对于每一个数据库，首先在临时文件中写入 Select 命令**选择正确的数据库**。
   data := reply.MakeMultiBulkStringReply(utils.StringsToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
   _, err := tmpFile.Write(data)
   if err != nil {
      return err
   }

   // 调用 foreach 函数，遍历数据库中的每一个 key，将每一个键值对写入到临时文件中。
   rewritePersister.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
      bytes := utils.EntityToBytes(key, entity)
      if bytes != nil {
         _, _ = tmpFile.Write(bytes)
      }
      if expiration != nil {
         // 有 TTL
         bytes := utils.ExpireToBytes(key, *expiration)
         if bytes != nil {
            _, _ = tmpFile.Write(bytes)
         }
      }

      return true
   })
}
```

## FinishRewrite 结束重写

FinishRewrite 的作用是暂停 AOF 写入 -> 将重写过程中产生的**新数据写入临时文件**中 -> 使用临时文件覆盖 AOF 文件（使用文件系统的 mv 命令保证安全） -> 恢复 AOF 写入。

其流程如下：

- 首先**暂停 AOF 文件的写入**。

```go
persister.pausingAof.Lock()
defer persister.pausingAof.Unlock()
```

- **打开 AOF 文件**，并 seek 到**重写开始的位置**。

```go
src, err := os.Open(persister.aofFilename)
if err != nil {
   logger.Error("open aofFilename failed: " + err.Error())
   return err
}

_, err = src.Seek(rewriteCtx.fileSize, 0)
if err != nil {
   logger.Error("seek failed: " + err.Error())
   return err
}
```

- 在临时文件中**写入一条 Select 命令**，使得临时文件切换到重写开始时选中的数据库。

```go
tmpFile := rewriteCtx.tmpFile
// 在临时文件中**写入一条 Select 命令**，使得临时文件切换到重写开始时选中的数据库。
data := reply.MakeMultiBulkStringReply(utils.StringsToCmdLine("SELECT", strconv.Itoa(rewriteCtx.dbIndex))).ToBytes()
_, err = tmpFile.Write(data)
if err != nil {
   logger.Error("tmp file rewrite failed: " + err.Error())
   return err
}
```

- 对齐数据库后，就可以把重写过程中产生的数据**复制到临时文件中了**。

```go
_, err = io.Copy(tmpFile, src)
if err != nil {
   logger.Error("copy aof file failed: " + err.Error())
   return err
}
```

- **使用 mv 命令**，令临时文件**代替** AOF 文件。

```go
_ = persister.aofFile.Close()
_ = src.Close()
_ = tmpFile.Close()
_ = os.Rename(tmpFile.Name(), persister.aofFilename)
```

- **重新打开 AOF 文件**，并重新**写入一次 Select 命令**保证 AOF 文件中的数据库与 persister.currentDB 一致。

```go
aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
if err != nil {
   panic(err)
}
persister.aofFile = aofFile

// write select command again to ensure aof file has the same db index with  persister.currentDB
data = reply.MakeMultiBulkStringReply(utils.StringsToCmdLine("SELECT", strconv.Itoa(rewriteCtx.dbIndex))).ToBytes()
_, err = persister.aofFile.Write(data)
if err != nil {
   panic(err)
}

return nil
```