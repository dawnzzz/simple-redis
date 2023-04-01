本项目完整地址 [simple-redis](https://github.com/dawnzzz/simple-redis)

`main.go` 文件用于**开启一个 simple-redis 服务器**，其流程如下：

-  首先加载配置文件
- 接着加载日志记录模块
- 最后开启 TCP 服务器

```go

var configFilename string
var defaultConfigFileName = "config.yaml"

func main() {
   flag.StringVar(&configFilename, "f", defaultConfigFileName, "the config file")
   flag.Parse()

   // 加载配置文件
   config.SetupConfig(configFilename)

   // 加载日志
   logger.SetupLogger()

   //
   if err := tcp.ListenAndServeWithSignal(server.MakeHandler()); err != nil {
      logger.Error(err)
   }
}
```

# TCP 服务器

`tcp/setver.go` 用于实现 TCP 服务器。

在 main 函数中，调用 ListenAndServeWithSignal 函数开启了一个 TCP 服务器。ListenAndServeWithSignal 的函数头如下：

```go
// ListenAndServeWithSignal 服务器开启监听,并且使用 signal 作为结束信号
func ListenAndServeWithSignal(handler tcp.Handler) error
```

**tcp.Handler 为 TCP 服务器上层应用接口，在本项目中代表一个 simple-redis 服务器**。

## ListenAndServeWithSignal

ListenAndServeWithSignal 用于**绑定端口，开启一个 TCP 服务器**，它的流程如下：

- 首先**开启两个通道**，一个用于接收**系统信号**（系统信号用于关闭），一个用于传递**关闭信号**。

```go
closeChan := make(chan struct{})   // 监听结束信号
signalChan := make(chan os.Signal) // 监听 signal
signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
```

- 开启一个**协程**用于接收系统信号，当**接收到关闭的系统信号时，向关闭管道中传入数据**，表明要关闭服务器。

```go
go func() {
   for {
      sig := <-signalChan
      switch sig {
      case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
         closeChan <- struct{}{}
         return
      default:
         continue
      }
   }
}()
```

- 开始监听 TCP 端口。

```go
address := fmt.Sprintf("%v:%v", config.Properties.Bind, config.Properties.Port)
listener, err := net.Listen("tcp", address)
if err != nil {
   return err
}
```

- 调用 ListenAndServe 开启真正的**应用层服务**。

```go
logger.Infoln("tcp server is listening at:", address)
ListenAndServe(listener, handler, closeChan)

return nil
```

### ListenAndServe

ListenAndServe 用于**接收 TCP 连接，为每一个 TCP 连接开启应用层服务（提供 simple-redis 服务）**，其函数头如下：

```go
// ListenAndServe TCP 服务器应用层服务
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan chan struct{})
```

ListenAndServe 的流程如下：

- **开启一个线程监听关闭信号**，当接收到关闭信号时，关闭TCP 端口监听（**listener.Accept() 会立即返回 io.EOF**）、关闭所有连接停止应用层服务。

- 为了防止意想不到的的错误发生，defer 中同样关闭TCP 端口监听、关闭 handler。
- 开启一个循环接收 TCP 连接，**对于每一个 TCP 连接都开启一个协程，调用 handler.Handle 为其进行应用层服务**。开启每一个协程时，都需要 waitDone.Add(1)，目的是**等待每一个 TCP 连接完成后再执行 defer**。

```go
ctx := context.Background()
var wg sync.WaitGroup
for {
   conn, err := listener.Accept()
   if err != nil {
      break
   }
   // 来了一个请求，开启协程处理请求
   logger.Info("accept a conn from:", conn.RemoteAddr().String())
   wg.Add(1)
   go func() {
      defer wg.Done()
      handler.Handle(ctx, conn)
   }()
}

// 等待所有请求处理完成
wg.Wait()
```

> 在生产环境下需要保证TCP服务器关闭前完成必要的清理工作，包括将完成正在进行的数据传输，关闭TCP连接等。这种关闭模式称为**优雅关闭**，可以避免资源泄露以及客户端未收到完整数据导致故障。
>
> TCP 服务器的优雅关闭模式通常为：**先关闭 listener 阻止新连接进入，然后遍历所有连接逐个进行关闭**。

# Handler

## Handler 接口

Handler 代表一个上传的应用服务，接口定义了两个函数：

- Handle：为某一个 TCP 连接进行应用层服务。
- Close：关闭上层应用服务。

interface/tcp/handler.go：

```go
type HandleFunc func(ctx context.Context, conn net.Conn)

type Handler interface {
   Handle(ctx context.Context, conn net.Conn)
   Close() error
}
```

## 接口的实现

在 redis/server/handler.go 文件下定义了 Handler 接口的实现。

### 结构体

Handler 的结构定义如下，包含以下几个字段：

- **activeConn：**记录活跃的连接，**用于检测客户端的连接是否存活**。其中 key 为 *connection.Connection，这是一个客户端连接的抽象表示，value 为收到一次客户端数据的时间戳。
- **db：**底层 simple- redis 数据库服务器。
- **closing 和 closingChan：**用于优雅的停止 Handler 服务。

```go
type Handler struct {
   activeConn  sync.Map // value记录activeConn的心跳
   db          database.DB
   closing     atomic.Boolean // refusing new client and new request
   closingChan chan struct{}  // 停止心跳检查计时器
}
```

使用 MakeHandler 函数可以返回一个 Handler，需要说明以下两点：

- Handler 会根据配置文件中是否配置了 Peers 来判断是否开启了集群模式。集群模式和单机模式会初始化不同的数据库服务器。
- 同时，也会根据配置文件中的 Keepalive 大于 0，来开启心跳（存活检查）。

### Handle 方法

**Handle 方法用于服务客户端的请求连接**，其核心在于将客户端连接 conn 绑定到协议解析器中，协议解析器不断的去解析客户端发送的数据。将解析之后的结果送入 simple-redis 服务器中执行，并且向客户端返回执行结果。

Handle 方法的流程如下：

- 首先根据 conn 来初始化一个客户端的抽象表示，并且在 h.activeConn 中记录，以备心跳检测的需要。

```go
client := connection.NewConn(conn)
h.activeConn.Store(client, time.Now())
```

- 接着**将 conn 绑定在协议解析器中**，不断读取协议解析器中解析的内容：
  - 若出现错误则关闭客户端的连接。
  - 正常的请求，先在 h.activeConn 中记录时间戳表示此时间点客户端发送过数据，接着送入 h.db 执行命令
  - 最后向客户端的连接中**写入请求结果**。

```go
for payload := range ch {
   if payload.Err != nil {
      // ....
      // 关闭客户端连接 h.closeClient(client)
      return
   }
   if payload.Data == nil {
      logger.Error("empty payload")
      continue
   }
    
   // 正常的请求，没有发生错误
   r, ok := payload.Data.(*reply.MultiBulkStringReply)
   if !ok {
      logger.Error("require multi bulk protocol")
      continue
   }
   
   // 记录时间戳，并送入simple-redis服务器中执行
   h.activeConn.Store(client, time.Now())

   result := h.db.Exec(client, r.Args)
   //返回结果
   if result != nil {
      _, _ = client.Write(result.ToBytes())
   } else {
      _, _ = client.Write(unknownErrReplyBytes)
   }
}
```

### 心跳检查

当配置文件中 keepalive 大于 0 时，表示**当客户端至少需要在 keepalive 秒内发送一条数据**（可以是心跳数据，也可以是命令）。当超过时间段都没有数据，服务器就认为客户端已经不再存活，则会关闭客户端的连接。

所以当 keepalive 大于 0 时，就需要**开启一个协程去检查客户端的存活情况**，对于超时未发送数据的客户端就及时的清理掉。

在 simple-redis 服务器中，**每 keepalive/2 秒钟**检查一次客户端的心跳。**利用 time.Ticker 来控制此流程**，所以开启一个 for select 循环，来不断的执行此过程。

- **当 ticker.C 有数据时**，说明已经**到了检查客户端存活情况的时间**了。于是遍历 h.activeConn 来检查上一次客户端发送消息的时间，如果在 keepalive 秒内没有收到消息，说明心跳超时，关闭客户端连接。
- **当收到 h.closingChan 发送的消息后**，说明**需要关闭 Handler 了**，所以停止计时器，并且返回结束协程。

```go
for {
   select {
   case <-ticker.C:
      h.activeConn.Range(func(key, value any) bool {
         if time.Now().After(value.(time.Time).Add(time.Second * time.Duration(keepalive))) {
            //  keepalive 秒内没有收到消息，关闭连接
            // 心跳超时，关闭连接
            h.closeClient(key.(*connection.Connection))
         }
         return true
      })
   case <-h.closingChan:
      ticker.Stop()
      return
   }
}
```

> Tips：
>
> **对于结束使用的 time.Ticker，要及时的调用 ticker.Stop 方法来结束计时器**。因为开启计时器时，会将交由系统协程去统一的管理计时器触发事件，Stop 会将这个计时器任务从系统协程中移除，从而节省系统资源。

### Close 方法

Close 方法用于关闭 Hanler，即结束上层 simple-redis 应用。

关闭的**流程**如下：

- 将 h.closing 设置为 true，接着向 h.closingChan 中发送一个消息表明要结束心跳检测。
- 遍历 h.activeConn，依次关闭客户端连接。
- 关闭 h.db 即数据库服务器。