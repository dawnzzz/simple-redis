package cluster

import (
	"Dawndis/database/cluster/tcc"
	"Dawndis/database/engine"
	"Dawndis/interface/redis"
	"Dawndis/lib/utils"
	"Dawndis/redis/protocol/reply"
	"strings"
)

// StartMultiCluster 集群模式下执行multi命令
func (cluster *Cluster) StartMultiCluster(client redis.Connection, args [][]byte) redis.Reply {
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

	// 初始化一个分布式事务协调者
	id := cluster.idGenerator.Generate().String() // 生成一个事务id
	coordinator := tcc.NewCoordinator(id, client.GetDBIndex(), cluster.self, cluster.peers, cluster.getters)
	cluster.coordinatorMap.Put(id, coordinator) // 记录
	// 设置事务id
	client.SetTxID(id)

	return reply.MakeOkReply()
}

// ExecMultiCluster 集群模式下执行exec命令
func (cluster *Cluster) ExecMultiCluster(client redis.Connection, args [][]byte) redis.Reply {
	if !client.GetMultiStatus() {
		return reply.MakeErrReply("ERR EXEC without MULTI")
	}

	if len(args) != 0 { // 参数数量不正确
		return reply.MakeArgNumErrReply("exec")
	}

	defer client.SetMultiStatus(false) // 结束multi
	defer client.CancelWatching()      // 取消watch

	// 检查是否有语法错误，若有语法错误则一律不执行
	if len(client.GetSyntaxErrQueue()) > 0 {
		return reply.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
	}

	// 获取事务协调者
	raw, _ := cluster.coordinatorMap.Get(client.GetTxID())
	coordinator := raw.(*tcc.Coordinator)
	// 执行分布式任务
	cmdLines := client.GetEnqueuedCmdLine() // 获取队列中的命令
	// 获取watching key
	watching := client.GetWatching()

	return coordinator.ExecTx(cmdLines, watching)
}

// DiscardMultiCluster 集群模式下执行discard命令
func (cluster *Cluster) DiscardMultiCluster(client redis.Connection, args [][]byte) redis.Reply {

	if len(args) != 0 { // 参数数量不正确
		return reply.MakeArgNumErrReply("discard")
	}

	if !client.GetMultiStatus() { // 没有multi
		return reply.MakeErrReply("ERR DISCARD without MULTI")
	}

	defer client.SetMultiStatus(false) // 放弃执行
	defer client.CancelWatching()      // 取消watch

	return reply.MakeOkReply()
}

func (cluster *Cluster) Try(db *engine.DB, args [][]byte) redis.Reply {
	if len(args) < 2 { // 参数数量不正确
		return reply.MakeArgNumErrReply("try")
	}

	// 根据事务id获取相关事务
	id := string(args[0])
	cmdName := strings.ToLower(string(args[1]))
	switch cmdName {
	case "start":
		tx := tcc.NewTransaction(id, db) // 不存在则新建一个本地事务
		cluster.transactionMap.Put(id, tx)
		return reply.MakeOkReply()
	case "end":
		raw, ok := cluster.transactionMap.Get(id)
		if !ok {
			return reply.MakeErrReply("ERR TRY END WITHOUT TRY START")
		}
		tx, _ := raw.(*tcc.Transaction)
		return tx.Try()
	case "watched":
		if len(args) != 4 { // 参数数量不正确
			return reply.MakeArgNumErrReply("try")
		}

		raw, ok := cluster.transactionMap.Get(id)
		if !ok {
			return reply.MakeErrReply("ERR TRY WATCHED WITHOUT TRY START")
		}
		tx, _ := raw.(*tcc.Transaction)
		return tx.SaveVersion(args[1:])
	default:
		raw, ok := cluster.transactionMap.Get(id)
		if !ok {
			return reply.MakeErrReply("ERR TRY COMMAND WITHOUT TRY START")
		}
		tx, _ := raw.(*tcc.Transaction)
		// 在本地事务中添加cmd line
		cmdLine := args[1:]
		return tx.AddCmdLine(cmdLine)
	}
}

func (cluster *Cluster) Commit(args [][]byte) redis.Reply {
	if len(args) != 1 { // 参数数量不正确
		return reply.MakeArgNumErrReply("commit")
	}

	// 根据事务id获取相关事务
	id := string(args[0])
	var tx *tcc.Transaction
	raw, ok := cluster.transactionMap.Get(id)
	if !ok {
		return reply.MakeErrReply("ERR COMMIT WITHOUT TRY")
	}
	tx, _ = raw.(*tcc.Transaction)

	// 进行提交
	return tx.Commit()
}

func (cluster *Cluster) Cancel(args [][]byte) redis.Reply {
	if len(args) != 1 { // 参数数量不正确
		return reply.MakeArgNumErrReply("cancel")
	}

	// 根据事务id获取相关事务
	id := string(args[0])
	var tx *tcc.Transaction
	raw, ok := cluster.transactionMap.Get(id)
	if !ok {
		return reply.MakeErrReply("ERR CANCEL WITHOUT TRY")
	}
	tx, _ = raw.(*tcc.Transaction)

	// 进行回滚
	return tx.Cancel()
}

func (cluster *Cluster) End(args [][]byte) redis.Reply {
	if len(args) != 1 { // 参数数量不正确
		return reply.MakeArgNumErrReply("end")
	}

	// 根据事务id获取相关事务
	id := string(args[0])
	var tx *tcc.Transaction
	raw, ok := cluster.transactionMap.Get(id)
	if !ok {
		return reply.MakeErrReply("ERR END WITHOUT TRY")
	}
	tx, _ = raw.(*tcc.Transaction)

	// 结束事务
	defer cluster.transactionMap.Remove(id)
	return tx.End()
}

func (cluster *Cluster) Watch(dbIndex int, db *engine.DB, client redis.Connection, args [][]byte) redis.Reply {
	if client.GetMultiStatus() {
		return reply.MakeErrReply("ERR WATCH inside MULTI is not allowed")
	}

	if len(args) <= 0 { // 参数数量不正确
		return reply.MakeArgNumErrReply("watch")
	}

	// 记录当前version
	watching := client.GetWatching()
	for _, rawKey := range args {
		key := string(rawKey)
		cmdLine := utils.StringsToCmdLine("KeyVersion", key) // 获取当前的key version
		raw := cluster.Exec(client, dbIndex, db, cmdLine)
		r := raw.(*reply.IntReply)
		watching[key] = uint32(r.Code)
	}

	return reply.MakeOkReply()
}

func (cluster *Cluster) UnWatch(client redis.Connection, args [][]byte) redis.Reply {
	if client.GetMultiStatus() {
		return reply.MakeErrReply("ERR UNWATCH inside MULTI is not allowed")
	}

	if len(args) != 0 { // 参数数量不正确
		return reply.MakeArgNumErrReply("unwatch")
	}

	// 删除watching
	client.CancelWatching()

	return reply.MakeOkReply()
}
