package cluster

import (
	"github.com/bwmarrin/snowflake"
	"github.com/dawnzzz/simple-redis/database/engine"
	"github.com/dawnzzz/simple-redis/datastruct/dict"
	"github.com/dawnzzz/simple-redis/interface/cluster"
	"github.com/dawnzzz/simple-redis/interface/redis"
	"github.com/dawnzzz/simple-redis/lib/consistenthash"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"hash/crc32"
)

const (
	replicasNum = 16
)

// Cluster 用于和集群中的主机进行交互
type Cluster struct {
	self    string                        // 本机地址，如 127.0.0.1:6107
	peers   cluster.PeerPicker            // 一致性哈希，用于选择节点
	getters map[string]cluster.PeerGetter // 用于和远程节点通信

	idGenerator    *snowflake.Node  // snowflake id生成器，用于生成分布式事务的id
	transactionMap *dict.SimpleDict // 记录所有的分布式事务（本地）
	coordinatorMap *dict.SimpleDict // 记录事务协调者
}

func NewCluster(self string) *Cluster {
	if self == "" {
		return nil
	}

	node, _ := snowflake.NewNode(int64(crc32.ChecksumIEEE([]byte(self))) % 1024)

	return &Cluster{
		self:    self,
		peers:   consistenthash.New(replicasNum, nil),
		getters: make(map[string]cluster.PeerGetter),

		idGenerator:    node,
		transactionMap: dict.MakeSimpleDict(),
		coordinatorMap: dict.MakeSimpleDict(),
	}
}

// AddPeers 添加节点
func (cluster *Cluster) AddPeers(peers ...string) {
	for _, peer := range peers {

		cluster.getters[peer] = newGetter(peer)
	}

	cluster.peers.AddNodes(cluster.self)
	cluster.peers.AddNodes(peers...)
}

func (cluster *Cluster) Exec(client redis.Connection, dbIndex int, db *engine.DB, cmdLine [][]byte) redis.Reply {
	if mustLocal(cmdLine) {
		// 在本地执行
		return db.Exec(client, cmdLine)
	}

	if client.GetMultiStatus() {
		// 在multi状态下，也在本地检查是否有语法错误
		return db.Exec(client, cmdLine)
	}

	key := string(cmdLine[1])
	peer, ok := cluster.peers.PickNode(key)
	if !ok || peer == cluster.self {
		// 在本地执行
		return db.Exec(client, cmdLine)
	}

	// 远程执行
	return cluster.getters[peer].RemoteExec(dbIndex, cmdLine)
}

func (cluster *Cluster) ExecInPeer(peer string, dbIndex int, cmdLine [][]byte) redis.Reply {
	getter, ok := cluster.getters[peer]
	if !ok {
		return reply.MakeErrReply("ERR Cluster Peers error")
	}

	return getter.RemoteExec(dbIndex, cmdLine)
}

func (cluster *Cluster) Close() {
	for _, g := range cluster.getters {
		g.Close()
	}
}

// 判断这条命令是否一定在本地执行
// TODO: 后面进行修改，这里只是进行简单的判断
func mustLocal(cmdLine [][]byte) bool {
	if len(cmdLine) <= 1 {
		return true
	}

	return false
}
