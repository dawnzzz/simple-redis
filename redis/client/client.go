package client

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/dawnzzz/simple-redis/interface/redis"
	"github.com/dawnzzz/simple-redis/lib/sync/wait"
	"github.com/dawnzzz/simple-redis/logger"
	"github.com/dawnzzz/simple-redis/redis/parser"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	running
	closed
)

type Client struct {
	conn        net.Conn      // 与服务器的tcp连接
	pendingReqs chan *request // 等待发送的请求
	waitingReqs chan *request // 等待服务器响应的请求
	ticker      *time.Ticker  // 发送心跳的计时器
	addr        string

	isCmdLine  bool // 标记是否是命令行客户端
	curDBIndex int  // 当前数据库

	status  int32 // 服务器状态（创建/运行/关闭）
	working *sync.WaitGroup

	keepalive time.Duration // 服务器存活检查时间
}

type request struct {
	id        uint64      // 请求id
	args      [][]byte    // 上行参数
	reply     redis.Reply // 收到的返回值
	heartbeat bool        // 标记是否是心跳请求
	waiting   *wait.Wait  // 调用协程发送请求后通过 waitgroup 等待请求异步处理完成
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

func MakeClient(addr string, keepalive int) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		addr:        addr,
		working:     &sync.WaitGroup{},

		keepalive: time.Second * time.Duration(keepalive),
	}, nil
}

func MakeCmdLineClient(addr string, keepalive int) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		addr:        addr,
		working:     &sync.WaitGroup{},

		isCmdLine: true,
		keepalive: time.Second * time.Duration(keepalive),
	}, nil
}

// Start starts asynchronous goroutines
func (client *Client) Start() {
	go client.handleWrite()
	go client.handleRead()

	if client.keepalive > 0 {
		// 开启心跳
		client.ticker = time.NewTicker(time.Second * client.keepalive / 2) // 每 keepalive/2 秒发送一次心跳
		go client.heartbeat()
	}

	atomic.StoreInt32(&client.status, running)
}

func (client *Client) StartCmdLine() {
	client.Start()

	reader := bufio.NewReader(os.Stdin)
	for {
		if atomic.LoadInt32(&client.status) == closed {
			fmt.Printf("server is cloed. exiting...\nbye bye\n")
			break
		}

		if client.curDBIndex == 0 {
			fmt.Printf("%s>", client.addr)
		} else {
			fmt.Printf("%s[%d]>", client.addr, client.curDBIndex)
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("%v\n", err)
			break
		}
		line = strings.TrimRight(line, "\r\n")
		argsStr := strings.Split(line, " ")

		if strings.ToLower(line) == "exit" {
			fmt.Printf("bye bye\n")
			break
		}

		argsBytes := make([][]byte, 0, len(argsStr))
		for _, argStr := range argsStr {

			if len(argStr) > 0 {
				argsBytes = append(argsBytes, []byte(argStr))
			}
		}

		if len(argsBytes) == 0 {
			continue
		}

		r := client.Send(argsBytes)

		result := r.DataString()
		fmt.Println(result)
	}
}

// Close stops asynchronous goroutines and close connection
func (client *Client) Close() {
	atomic.StoreInt32(&client.status, closed)
	if client.keepalive > 0 {
		client.ticker.Stop()
	}
	// stop new request
	close(client.pendingReqs)

	// wait working process stop
	client.working.Wait()
	// client.heartbeatWorking.Wait()

	_ = client.conn.Close()
	close(client.waitingReqs)
}

func (client *Client) StatusClosed() bool {
	return atomic.LoadInt32(&client.status) == closed
}

// Send sends a request to redis server
func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return reply.MakeErrReply("client closed")
	}

	request := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}

	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()

	client.pendingReqs <- request

	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.MakeErrReply("server time out")
	}
	if request.err != nil {
		return reply.MakeErrReply("request failed")
	}

	if _, ok := request.reply.(*reply.StandardErrReply); !ok && strings.ToLower(string(args[0])) == "select" {
		curDBIndex, _ := strconv.Atoi(string(args[1]))
		client.curDBIndex = curDBIndex
	}

	return request.reply
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

func (client *Client) doRequest(req *request) {
	re := reply.MakeMultiBulkStringReply(req.args)
	bytes := re.ToBytes()

	// 最多失败重试3次
	var err error
	for i := 0; i < 3; i++ {
		_, err = client.conn.Write(bytes)
		if err == nil || (!strings.Contains(err.Error(), "timeout") && // only retry timeout
			!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}

	if err == nil {
		client.waitingReqs <- req
	} else {
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) handleRead() {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	request := <-client.waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) reconnect() {
	if !client.isCmdLine {
		logger.Info("reconnect with: " + client.addr)
	}

	_ = client.conn.Close() // ignore possible errors from repeated closes

	var conn net.Conn
	for i := 0; i < 3; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			if !client.isCmdLine {
				logger.Error("reconnect error: " + err.Error())
			}
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	if conn == nil { // reach max retry, abort
		client.Close()
		return
	}
	client.conn = conn

	close(client.waitingReqs)
	for req := range client.waitingReqs {
		req.err = errors.New("connection closed")
		req.waiting.Done()
	}

	client.waitingReqs = make(chan *request, chanSize)

	// restart handle read
	go client.handleRead()
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) doHeartbeat() {
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- request
	request.waiting.WaitWithTimeout(maxWait)
}
