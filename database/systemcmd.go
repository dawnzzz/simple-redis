package database

import (
	"github.com/dawnzzz/simple-redis/config"
	"github.com/dawnzzz/simple-redis/interface/redis"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"strconv"
)

// Auth validate client's password
func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	if config.Properties.Password == "" {
		return reply.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	passwd := string(args[0])
	c.SetPassword(passwd)
	if config.Properties.Password != passwd {
		return reply.MakeErrReply("ERR invalid password")
	}
	return &reply.OkReply{}
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.Password == "" {
		return true
	}
	return c.GetPassword() == config.Properties.Password
}

func SelectDB(c redis.Connection, args [][]byte, dbNum int) redis.Reply {
	if c.GetMultiStatus() {
		// 无法在multi中使用select
		errReply := reply.MakeErrReply("cannot select database within multi")
		c.EnqueueSyntaxErrQueue(errReply)
		return errReply
	}

	if len(c.GetWatching()) > 0 {
		// 无法在watching时使用select
		return reply.MakeErrReply("cannot select database when watching")
	}

	if len(args) != 1 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'select' command")
	}

	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR select db index is not an integer")
	}

	if dbIndex < 0 || dbIndex >= dbNum {
		return reply.MakeErrReply("ERR index is invalid")
	}

	c.SelectDB(dbIndex)

	return &reply.OkReply{}
}

func BGRewriteAof(s *Server, args [][]byte) redis.Reply {
	if s.rewriting.Load() {
		// 如果当前正在重写，直接返回
		return reply.MakeStatusReply("Background append only file rewriting doing")
	}

	// 否则进行异步重写
	s.rewriteWait.Add(1)
	go s.AofPersister.Rewrite(&s.rewriteWait, &s.rewriting)
	return reply.MakeStatusReply("Background append only file rewriting started")
}

func RewriteAof(s *Server, args [][]byte) redis.Reply {
	if s.rewriting.Load() {
		// 如果当前正在重写，等待重写结束返回
		s.rewriteWait.Wait()
		return reply.MakeOkReply()
	}

	// 否则进行重写
	s.rewriteWait.Add(1)
	err := s.AofPersister.Rewrite(&s.rewriteWait, &s.rewriting)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	return reply.MakeOkReply()
}
