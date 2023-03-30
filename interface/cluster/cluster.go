package cluster

import "github.com/dawnzzz/simple-redis/interface/redis"

type PeerPicker interface {
	PickNode(key string) (string, bool)
	AddNodes(keys ...string)
	GetAllNodes() []string
}

type PeerGetter interface {
	RemoteExec(dbIndex int, args [][]byte) redis.Reply
	Close()
}
