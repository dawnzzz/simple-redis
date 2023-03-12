package utils

import (
	"Dawndis/interface/database"
	"Dawndis/redis/protocol/reply"
	"strconv"
	"time"
)

var (
	setCmd       = []byte("SET")
	pExpireAtCmd = []byte("PEXPIREAT")
)

// EntityToBytes serialize data entity to redis multi bulk bytes
func EntityToBytes(key string, entity *database.DataEntity) []byte {
	if entity == nil {
		return nil
	}

	return EntityToReply(key, entity).ToBytes()
}

func EntityToCmdLine(key string, entity *database.DataEntity) [][]byte {
	if entity == nil {
		return nil
	}

	return EntityToReply(key, entity).Args
}

func EntityToReply(key string, entity *database.DataEntity) *reply.MultiBulkStringReply {
	if entity == nil {
		return nil
	}

	var cmd *reply.MultiBulkStringReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = stringToCmd(key, val)
		//case List.List:
		//	cmd = listToCmd(key, val)
		//case *set.Set:
		//	cmd = setToCmd(key, val)
		//case dict.Dict:
		//	cmd = hashToCmd(key, val)
		//case *SortedSet.SortedSet:
		//	cmd = zSetToCmd(key, val)
	}

	if cmd == nil {
		return nil
	}

	return cmd
}

// ExpireToBytes 将expireAt命令转为[]byte(*reply.MultiBulkStringReply.ToBytes())
func ExpireToBytes(key string, expireAt time.Time) []byte {
	return ExpireToReply(key, expireAt).ToBytes()
}

// ExpireToCmdLine 将expireAt命令转为[][]byte
func ExpireToCmdLine(key string, expireAt time.Time) [][]byte {
	return ExpireToReply(key, expireAt).Args
}

// ExpireToReply 将expireAt命令转为*reply.MultiBulkStringReply
func ExpireToReply(key string, expireAt time.Time) *reply.MultiBulkStringReply {
	args := make([][]byte, 3)
	args[0] = pExpireAtCmd
	args[1] = []byte(key)
	args[2] = []byte(strconv.FormatInt(expireAt.UnixNano()/1e6, 10))

	return reply.MakeMultiBulkStringReply(args)
}

func stringToCmd(key string, bytes []byte) *reply.MultiBulkStringReply {
	args := make([][]byte, 3)
	args[0] = setCmd
	args[1] = []byte(key)
	args[2] = bytes

	return reply.MakeMultiBulkStringReply(args)
}
