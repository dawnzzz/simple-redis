package tcc

import (
	"Dawndis/interface/cluster"
	"Dawndis/interface/redis"
	"Dawndis/lib/utils"
	"Dawndis/redis/protocol/reply"
	"fmt"
)

// Coordinator TCC事务协调者
type Coordinator struct {
	id      string                        // 事务id
	dbIndex int                           // 数据库id
	self    string                        // 自己的地址
	peers   cluster.PeerPicker            // 一致性哈希，用于选择节点
	getters map[string]cluster.PeerGetter // 用于和远程节点通信

	cmdLinesNum int
	indexMap    map[string][]int // key为peer，value为cmdline在原来multi队列中的下标，用于重组reply
}

func NewCoordinator(id string, dbIndex int, self string, peers cluster.PeerPicker, getters map[string]cluster.PeerGetter) *Coordinator {
	return &Coordinator{
		id:      id,
		dbIndex: dbIndex,
		self:    self,
		peers:   peers,
		getters: getters,
	}
}

// ExecTx 执行TCC分布式事务
func (coordinator *Coordinator) ExecTx(cmdLines [][][]byte) redis.Reply {
	// 首先对命令进行分组，以同一个节点上执行的所有命令为一组
	groupByMap := coordinator.groupByCmdLines(cmdLines)

	// 对每一组中发送try命令
	needCancel := false // 记录是否需要回滚
	for peer, cmd := range groupByMap {
		r := coordinator.sendTry(peer, cmd)

		if reply.IsErrorReply(r) {
			needCancel = true
		}
	}

	// 向各个节点提交/回滚分布式事务
	replies := make(map[string]redis.Reply)
	for peer := range groupByMap {
		if needCancel {
			// 回滚事务
			coordinator.sendCancel(peer)
		} else {
			// 提交事务
			replies[peer] = coordinator.sendCommit(peer)
		}
	}

	if needCancel {
		// 回滚，返回错误
		return reply.MakeErrReply("EXECABORT Transaction discarded because of previous errors(tcc tx).")
	}

	// 正常提交，重组reply
	return coordinator.recombineReplies(replies)
}

func (coordinator *Coordinator) sendTry(peer string, cmdLines [][][]byte) redis.Reply {
	// 获取getter
	getter := coordinator.getters[peer]

	var r redis.Reply
	// 发送try开始命令
	r = getter.RemoteExec(coordinator.dbIndex, utils.StringsToCmdLine("try", coordinator.id, "start"))
	fmt.Printf("%#v:%#v\n", peer, r.DataString())
	if reply.IsErrorReply(r) {
		// 如果发生错误，则中断try，直接返回
		return r
	}

	// 依次发送需要执行的命令，每一条命令=try tx_id cmdline，如 try 123456 set k1 v1
	for _, cmdLine := range cmdLines {
		tryCmd := make([][]byte, 0, 2+len(cmdLine))
		tryCmd = append(tryCmd, utils.StringsToCmdLine("try", coordinator.id)...)
		tryCmd = append(tryCmd, cmdLine...)
		r = getter.RemoteExec(coordinator.dbIndex, tryCmd)
		fmt.Printf("%#v:%#v\n", peer, r.DataString())
		if reply.IsErrorReply(r) {
			// 如果发生错误，则中断try，直接返回
			return r
		}
	}

	// 发送try结束命令
	r = getter.RemoteExec(coordinator.dbIndex, utils.StringsToCmdLine("try", coordinator.id, "end"))
	fmt.Printf("%#v:%#v\n", peer, r.DataString())
	if reply.IsErrorReply(r) {
		// 如果发生错误，则中断try，直接返回
		return r
	}

	return reply.MakeOkReply()
}

// 对命令进行分组，以同一个节点上执行的所有命令为一组
func (coordinator *Coordinator) groupByCmdLines(cmdLines [][][]byte) map[string][][][]byte {
	groupByMap := make(map[string][][][]byte)
	indexMap := make(map[string][]int) // key为peer，value为cmdline在原来multi队列中的下标
	for i, cmdLine := range cmdLines {
		key := string(cmdLine[1])
		peer, ok := coordinator.peers.PickNode(key)
		if !ok {
			peer = coordinator.self
		}
		groupByMap[peer] = append(groupByMap[peer], cmdLine)
		indexMap[peer] = append(indexMap[peer], i)
	}

	coordinator.indexMap = indexMap
	coordinator.cmdLinesNum = len(cmdLines)
	return groupByMap
}

func (coordinator *Coordinator) sendCommit(peer string) redis.Reply {
	// 获取getter
	getter := coordinator.getters[peer]

	return getter.RemoteExec(coordinator.dbIndex, utils.StringsToCmdLine("commit", coordinator.id))
}

func (coordinator *Coordinator) sendCancel(peer string) redis.Reply {
	// 获取getter
	getter := coordinator.getters[peer]

	return getter.RemoteExec(coordinator.dbIndex, utils.StringsToCmdLine("cancel", coordinator.id))
}

// 重组commit之后收到的命令结果
func (coordinator *Coordinator) recombineReplies(replies map[string]redis.Reply) redis.Reply {
	combinedArgs := make([][]byte, coordinator.cmdLinesNum)

	// 排序结果
	for peer := range replies {
		indies := coordinator.indexMap[peer]
		args := replies[peer].(*reply.MultiBulkStringReply).Args
		for i, arg := range args {
			index := indies[i]
			combinedArgs[index] = arg
		}
	}

	// 合并
	return reply.MakeMultiBulkStringReply(combinedArgs)
}
