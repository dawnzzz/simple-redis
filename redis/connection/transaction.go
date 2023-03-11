package connection

import "Dawndis/interface/redis"

// GetMultiStatus 获取是否在multi中
func (c *Connection) GetMultiStatus() bool {
	return c.isMulti
}

// SetMultiStatus 设置是否在multi中
func (c *Connection) SetMultiStatus(state bool) {
	if !state {
		c.queue = nil
		c.syntaxErrQueue = nil
	}
	c.isMulti = state
}

// GetEnqueuedCmdLine 获取multi中的命令
func (c *Connection) GetEnqueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmdLine 向multi队列中加入命令
func (c *Connection) EnqueueCmdLine(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// ClearCmdLines 清空multi队列
func (c *Connection) ClearCmdLines() {
	c.queue = nil
}

func (c *Connection) GetSyntaxErrQueue() []redis.Reply {
	return c.syntaxErrQueue
}

func (c *Connection) EnqueueSyntaxErrQueue(r redis.Reply) {
	c.syntaxErrQueue = append(c.syntaxErrQueue, r)
}

func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

func (c *Connection) CancelWatching() {
	c.watching = nil
}
