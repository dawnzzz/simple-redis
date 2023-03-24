package database

import (
	"Dawndis/interface/redis"
	"Dawndis/redis/protocol/reply"
	"strings"
)

var (
	subscribeBytes   = []byte("subscribe")
	unsubscribeBytes = []byte("unsubscribe")
)

func Publish(s *Server, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeArgNumErrReply("publish")
	}

	name := string(args[0])
	message := args[1]
	result := s.publish.Publish(name, message)

	return reply.MakeIntReply(int64(result))
}

func UnSubscribe(s *Server, client redis.Connection, args [][]byte) redis.Reply {
	var names []string

	if len(args) == 0 {
		names = make([]string, client.GetSubscribeNum())

		for i, name := range client.GetSubscribes() {
			names[i] = name
		}
	} else {
		names = make([]string, len(args))
		for i, arg := range args {
			names[i] = string(arg)
		}
	}

	s.publish.UnSubscribe(client, names...)

	return reply.MakeNoReply()
}

func Subscribe(s *Server, client redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return reply.MakeArgNumErrReply("subscribe")
	}

	names := make([]string, len(args))
	for i, arg := range args {
		names[i] = string(arg)
	}

	s.publish.Subscribe(client, names...)

	return reply.MakeNoReply()
}

func PubSub(s *Server, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return reply.MakeArgNumErrReply("pubsub")
	}
	subCommand := strings.ToLower(string(args[0])) // 子命令

	switch subCommand {
	case "channels":
		// 列出当前的活跃频道。
		// 活跃频道指的是那些至少有一个订阅者的频道
		if len(args) != 1 {
			return reply.MakeArgNumErrReply("pubsub channels")
		}
		activeChannels := s.publish.ActiveChannels()
		return reply.MakeMultiBulkStringReply(activeChannels)
	case "numsub":
		// 返回频道的订阅者数量
		var replyArgs [][]byte

		if len(args) == 1 {
			// 查询所有频道
			replyArgs = s.publish.SubscribersNum()
		} else {
			// 查询给定频道
			names := make([]string, len(args[1:]))
			for i, arg := range args[1:] {
				names[i] = string(arg)
			}

			replyArgs = s.publish.SubscribersNum(names...)
		}

		return reply.MakeMultiBulkStringReply(replyArgs)
	default:
		return reply.MakeErrReply("ERR Unknown PUBSUB subcommand or wrong number of arguments for '" + subCommand + "'")
	}
}
