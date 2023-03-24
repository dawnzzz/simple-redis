package database

import (
	"Dawndis/config"
	"Dawndis/interface/redis"
	"Dawndis/lib/utils"
	"Dawndis/redis/protocol/reply"
	"strconv"
	"strings"
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

		// 将 []string 转为 [][]byte
		replyArgs := make([][]byte, 0, len(activeChannels))
		for _, channel := range activeChannels {
			replyArgs = append(replyArgs, []byte(channel))
		}

		return reply.MakeMultiBulkStringReply(replyArgs)
	case "numsub":
		// 返回频道的订阅者数量
		var channels []string
		var nums []int

		if len(args) == 1 {
			// 查询所有频道
			channels, nums = s.publish.SubscribersNum()
		} else {
			// 查询给定频道
			names := make([]string, len(args[1:]))
			for i, arg := range args[1:] {
				names[i] = string(arg)
			}

			channels, nums = s.publish.SubscribersNum(names...)
		}

		// 转换为replyArgs
		replyArgs := make([][]byte, 0, 2*len(channels))
		for i := 0; i < len(channels); i++ {
			replyArgs = append(replyArgs, []byte(channels[i]))
			strNum := strconv.Itoa(nums[i])
			replyArgs = append(replyArgs, []byte(strNum))
		}

		return reply.MakeMultiBulkStringReply(replyArgs)
	default:
		return reply.MakeErrReply("ERR Unknown PUBSUB subcommand or wrong number of arguments for '" + subCommand + "'")
	}
}

func PublishCluster(s *Server, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeArgNumErrReply("publish")
	}

	name := string(args[0])
	message := args[1]

	// 在本地执行
	result := int64(s.publish.Publish(name, message))

	// 向其他节点发送singlepublish消息
	for _, peer := range config.Properties.Peers {
		intReply, ok := s.cluster.ExecInPeer(peer, 0, utils.StringsToCmdLine("singlepublish", name, string(message))).(*reply.IntReply)
		if !ok {
			continue
		}

		result += intReply.Code
	}

	return reply.MakeIntReply(result)
}

func PubSubCluster(s *Server, args [][]byte) redis.Reply {
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

		// 在本地执行
		activeChannels := s.publish.ActiveChannels()

		channelSet := make(map[string]struct{}) // 去掉重复频道
		for _, channel := range activeChannels {
			channelSet[channel] = struct{}{}
		}

		// 远程执行
		for _, peer := range config.Properties.Peers {
			r, ok := s.cluster.ExecInPeer(peer, 0, utils.StringsToCmdLine("singlepubsub", subCommand)).(*reply.MultiBulkStringReply)
			if !ok {
				continue
			}

			for _, arg := range r.Args {
				if _, ok := channelSet[string(arg)]; ok {
					continue
				}

				activeChannels = append(activeChannels, string(arg))
				channelSet[string(arg)] = struct{}{}
			}
		}

		// 将 []string 转为 [][]byte
		replyArgs := make([][]byte, 0, len(activeChannels))
		for _, channel := range activeChannels {
			replyArgs = append(replyArgs, []byte(channel))
		}

		return reply.MakeMultiBulkStringReply(replyArgs)
	case "numsub":
		// 返回频道的订阅者数量
		var channels []string
		var nums []int

		// 在本地执行
		if len(args) == 1 {
			// 查询所有频道
			channels, nums = s.publish.SubscribersNum()
		} else {
			// 查询给定频道
			names := make([]string, len(args[1:]))
			for i, arg := range args[1:] {
				names[i] = string(arg)
			}

			channels, nums = s.publish.SubscribersNum(names...)
		}

		channelSet := make(map[string]int) // 记录频道的订阅数
		for i := 0; i < len(channels); i += 2 {
			channelSet[channels[i]] = nums[i]
		}

		// 远程执行
		cmd := make([]string, 0, len(args[1:])+2)
		cmd = append(cmd, "singlepubsub")
		cmd = append(cmd, subCommand)
		for _, arg := range args[1:] {
			cmd = append(cmd, string(arg))
		}

		for _, peer := range config.Properties.Peers {
			var r *reply.MultiBulkStringReply
			var ok bool
			if len(args) == 1 {
				r, ok = s.cluster.ExecInPeer(peer, 0, utils.StringsToCmdLine("singlepubsub", subCommand)).(*reply.MultiBulkStringReply)
			} else {
				r, ok = s.cluster.ExecInPeer(peer, 0, utils.StringsToCmdLine(cmd...)).(*reply.MultiBulkStringReply)
			}
			if !ok {
				continue
			}

			for i := 0; i < len(r.Args); i += 2 {
				channel := string(r.Args[i])
				strNum := string(r.Args[i+1])
				num, err := strconv.Atoi(strNum)
				if err != nil {
					continue
				}

				channelSet[channel] += num
			}
		}

		// 转换为replyArgs
		replyArgs := make([][]byte, 0, len(channelSet))
		for channel, num := range channelSet {

			replyArgs = append(replyArgs, []byte(channel))
			strNum := strconv.Itoa(num)
			replyArgs = append(replyArgs, []byte(strNum))
		}

		return reply.MakeMultiBulkStringReply(replyArgs)
	default:
		return reply.MakeErrReply("ERR Unknown PUBSUB subcommand or wrong number of arguments for '" + subCommand + "'")
	}
}
