package cluster

import "Dawndis/interface/redis"

type PeerPicker interface {
	PickNode(key string) (string, bool)
	AddNodes(keys ...string)
}

type PeerGetter interface {
	RemoteExec(dbIndex int, args [][]byte) redis.Reply
	Close()
}
