package database

import (
	"Dawndis/interface/redis"
	"Dawndis/redis/protocol/reply"
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
