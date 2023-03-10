package cluster

import (
	"Dawndis/database/engine"
	"Dawndis/interface/redis"
	"Dawndis/lib/consistenthash"
)

const (
	replicasNum = 16
)

// Cluster 用于和集群中的主机进行交互
type Cluster struct {
	self    string                // 本机地址，如 127.0.0.1:6107
	peers   PeerPicker            // 一致性哈希，用于选择节点
	getters map[string]PeerGetter // 用于和远程节点通信
}

func NewCluster(self string) *Cluster {
	if self == "" {
		return nil
	}

	return &Cluster{
		self:    self,
		peers:   consistenthash.New(replicasNum, nil),
		getters: make(map[string]PeerGetter),
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
	key := string(cmdLine[1])
	peer, ok := cluster.peers.PickNode(key)
	if !ok || peer == cluster.self {
		// 在本地执行
		return db.Exec(client, cmdLine)
	}

	// 远程执行
	return cluster.getters[peer].RemoteExec(dbIndex, cmdLine)
}
