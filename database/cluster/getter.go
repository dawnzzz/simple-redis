package cluster

import (
	"Dawndis/config"
	"Dawndis/interface/redis"
	"Dawndis/lib/pool"
	"Dawndis/lib/utils"
	"Dawndis/redis/client"
	"Dawndis/redis/protocol/reply"
	"strconv"
)

type getter struct {
	addr    string
	poolMap map[int]*pool.Pool // 每一个数据库对应一个连接池
}

func newGetter(addr string) *getter {
	// Client连接结束函数
	finalizer := func(x interface{}) {
		if c, ok := x.(*client.Client); ok {
			c.Close()
		}
	}

	checkAlive := func(x interface{}) bool {
		if c, ok := x.(*client.Client); ok {
			return c.StatusClosed()
		}

		return false
	}

	// 为每一个连接创建连接池
	poolMap := make(map[int]*pool.Pool, config.Properties.Databases)
	for i := 0; i < config.Properties.Databases; i++ {
		var dbIndex = i
		// Client连接构造函数
		factory := func() (interface{}, error) {
			c, err := client.MakeClient(addr, config.Properties.Keepalive)
			if err != nil {
				return nil, err
			}

			c.Start()
			// 如果需要密码，先进行验证
			if config.Properties.Password != "" {
				r := c.Send(utils.StringsToCmdLine("AUTH", config.Properties.Password))
				if reply.IsErrorReply(r) {
					c.Close()
					return nil, reply.MakeErrReply("ERR cluster password is required, please set same password in cluster")
				}
			}
			// 切换数据库
			r := c.Send(utils.StringsToCmdLine("SELECT", strconv.Itoa(dbIndex)))
			if reply.IsErrorReply(r) {
				c.Close()
				return nil, reply.MakeErrReply("ERR exec command failed")
			}
			return c, nil
		}

		poolMap[dbIndex] = pool.New(factory, finalizer, checkAlive, pool.Config{
			MaxIdleNum:   1,
			MaxActiveNum: 16,
			MaxRetryNum:  1,
		})
	}

	return &getter{
		addr:    addr,
		poolMap: poolMap,
	}
}

func (g *getter) RemoteExec(dbIndex int, args [][]byte) redis.Reply {
	raw, err := g.poolMap[dbIndex].Get()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer g.poolMap[dbIndex].Put(raw)

	c, _ := raw.(*client.Client)

	// 再执行命令
	return c.Send(args)
}

func (g *getter) Close() {
	for _, p := range g.poolMap {
		p.Close()
	}
}
