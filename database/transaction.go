package database

import (
	"Dawndis/interface/redis"
	"Dawndis/redis/protocol/reply"
)

// StartMulti 客户端开启 Multi 事务
func StartMulti(client redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 0 { // 参数数量不正确
		return reply.MakeArgNumErrReply("multi")
	}

	// 查看是否已经处于multi
	if client.GetMultiStatus() {
		// 已经处于multi阶段了，直接返回
		return reply.MakeErrReply("ERR MULTI calls can not be nested")
	}

	// 设置multi状态
	client.SetMultiStatus(true)

	return reply.MakeOkReply()
}

// ExecMultiStandalone 单机模式执行multi队列中的命令
func ExecMultiStandalone(s *Server, client redis.Connection, args [][]byte) redis.Reply {
	defer client.SetMultiStatus(false) // 结束multi
	if len(args) != 0 {                // 参数数量不正确
		return reply.MakeArgNumErrReply("multi")
	}

	// 检查是否有语法错误，若有语法错误则一律不执行
	if len(client.GetSyntaxErrQueue()) > 0 {
		return reply.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
	}

	// 选择数据库
	dbIndex := client.GetDBIndex()
	localDB, errReply := s.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}

	// 执行队列中的命令
	return localDB.ExecMulti(client)
}

// ExecDiscard 放弃执行multi队列中的命令
func ExecDiscard(client redis.Connection, args [][]byte) redis.Reply {
	defer client.SetMultiStatus(false) // 放弃执行

	if len(args) != 0 { // 参数数量不正确
		return reply.MakeArgNumErrReply("multi")
	}

	if !client.GetMultiStatus() { // 没有multi
		return reply.MakeErrReply("ERR DISCARD without MULTI")
	}

	return reply.MakeOkReply()
}

// ExecWatch 执行WATCH命令
func ExecWatch(s *Server, client redis.Connection, args [][]byte) redis.Reply {

	if len(args) <= 0 { // 参数数量不正确
		return reply.MakeArgNumErrReply("multi")
	}

	// 选择数据库
	dbIndex := client.GetDBIndex()
	localDB, errReply := s.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}

	// 记录当前version
	watching := client.GetWatching()
	for _, rawKey := range args {
		key := string(rawKey)
		watching[key] = localDB.GetVersion(key)
	}

	return reply.MakeOkReply()
}

// ExecUnWatch 取消对所有key的watch
func ExecUnWatch(client redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 0 { // 参数数量不正确
		return reply.MakeArgNumErrReply("multi")
	}

	// 取消watch
	client.CancelWatching()

	return reply.MakeOkReply()
}
