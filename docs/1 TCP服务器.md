`main.go` 文件用于**开启一个 simple-redis 服务器**，其流程如下：

-  首先加载配置文件
- 接着加载日志记录模块
- 最后开启 TCP 服务器

```

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

**tcp.Handler 为 TCP 服务器上层应用接口**，定义了两个函数：

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

ListenAndServe 用于**接收 TCP 连接，为每一个 TCP 连接开启应用层服务**，其函数头如下：

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