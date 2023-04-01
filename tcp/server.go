package tcp

import (
	"context"
	"fmt"
	"github.com/dawnzzz/simple-redis/config"
	"github.com/dawnzzz/simple-redis/interface/tcp"
	"github.com/dawnzzz/simple-redis/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// ListenAndServeWithSignal 服务器开启监听,并且使用 signal 作为结束信号
func ListenAndServeWithSignal(handler tcp.Handler) error {
	closeChan := make(chan struct{})   // 监听结束信号
	signalChan := make(chan os.Signal) // 监听 signal
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
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

	address := fmt.Sprintf("%v:%v", config.Properties.Bind, config.Properties.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	logger.Infoln("tcp server is listening at:", address)
	ListenAndServe(listener, handler, closeChan)

	return nil
}

// ListenAndServe TCP 服务器应用层服务
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan chan struct{}) {
	// 开启一个协程检查退出信号
	go func() {
		<-closeChan
		logger.Info("server shutting down...")
		_ = listener.Close()
		_ = handler.Close()
	}()

	defer func() {
		if err := recover(); err != nil {
			// close during unexpected error
			_ = listener.Close()
			_ = handler.Close()
		}
	}()

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
}
