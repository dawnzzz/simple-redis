package cluster

import "Dawndis/interface/redis"

type PeerPicker interface {
	PickNode(key string) (string, bool)
	AddNodes(keys ...string)
	GetAllNodes() []string
}

type PeerGetter interface {
	RemoteExec(dbIndex int, args [][]byte) redis.Reply
	Close()
}
