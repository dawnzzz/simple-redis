package connection

import (
	"Dawndis/interface/redis"
	"Dawndis/lib/sync/wait"
	"Dawndis/logger"
	"net"
	"sync"
	"time"
)

type CmdLine [][]byte

// Connection represents a connection with a client
type Connection struct {
	conn net.Conn

	// wait until finish sending data, used for graceful shutdown
	sendingData wait.Wait

	// lock while server sending response
	mu sync.Mutex

	// password may be changed by CONFIG command during runtime, so store the password
	password string

	// selected db
	selectedDB int

	isMulti        bool          // 表明是否在 multi 开启事务中
	queue          [][][]byte    // 事务中排队的命令
	syntaxErrQueue []redis.Reply // 事务中的语法错误
}

var connPool = sync.Pool{
	New: func() interface{} {
		return &Connection{}
	},
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// NewConn creates Connection instance
func NewConn(conn net.Conn) *Connection {
	c, ok := connPool.Get().(*Connection)
	if !ok {
		logger.Error("connection pool make wrong type")
		return &Connection{
			conn: conn,
		}
	}
	c.conn = conn
	return c
}

// Write sends response to client over tcp connection
func (c *Connection) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}

	c.sendingData.Add(1)
	defer func() {
		c.sendingData.Done()
	}()

	return c.conn.Write(bytes)
}

// Close disconnect with the client
func (c *Connection) Close() error {
	c.sendingData.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	c.sendingData = wait.Wait{}
	c.password = ""
	c.selectedDB = 0
	connPool.Put(c)
	return nil
}

func (c *Connection) SetPassword(password string) {
	c.password = password
}

func (c *Connection) GetPassword() string {
	return c.password
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(i int) {
	c.selectedDB = i
}

func (c *Connection) Name() string {
	if c.conn != nil {
		return c.RemoteAddr().String()
	}

	return ""
}
