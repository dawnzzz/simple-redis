package cluster

import (
	"Dawndis/config"
	"Dawndis/interface/redis"
	"Dawndis/lib/utils"
	"Dawndis/redis/client"
	"Dawndis/redis/protocol/reply"
	"strconv"
)

type getter struct {
	addr string
}

func newGetter(addr string) *getter {
	return &getter{
		addr: addr,
	}
}

func (g *getter) RemoteExec(dbIndex int, args [][]byte) redis.Reply {
	c, err := client.MakeClient(g.addr, config.Properties.Keepalive) // 与远程节点建立连接
	if err != nil {
		return reply.MakeErrReply("ERR remote node " + g.addr + " is not online")
	}
	defer c.Close()

	c.Start()
	// 如果需要密码，先进行验证
	r := c.Send(utils.StringsToCmdLine("AUTH", config.Properties.Password))
	if reply.IsErrorReply(r) {
		return reply.MakeErrReply("ERR cluster password is required, please set same password in cluster")
	}
	// 切换数据库
	r = c.Send(utils.StringsToCmdLine("SELECT", strconv.Itoa(dbIndex)))
	if reply.IsErrorReply(r) {
		return reply.MakeErrReply("ERR exec command failed")
	}

	// 再执行命令
	return c.Send(args)
}
