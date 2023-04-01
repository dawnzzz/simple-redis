package commands

import (
	"github.com/dawnzzz/simple-redis/database/engine"
	"github.com/dawnzzz/simple-redis/interface/redis"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"strconv"
	"time"
)

func execDel(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	// 首先查询是否存在
	_, exist := db.GetEntity(key)

	if !exist {
		// 不存在，直接返回
		return reply.MakeIntReply(0), &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	// key存在，删除
	db.Remove(key)

	return reply.MakeIntReply(1), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execExpireAt(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	raw, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	expireAt := time.Unix(raw, 0)

	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	db.Expire(key, expireAt)
	return reply.MakeIntReply(1), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: &expireAt,
	}
}

func execExpire(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	expireTime, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	expireAt := time.Now().Add(time.Second * time.Duration(expireTime))

	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	db.Expire(key, expireAt)
	return reply.MakeIntReply(1), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: &expireAt,
	}
}

func execKeyVersion(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	version := db.GetVersion(key)

	return reply.MakeIntReply(int64(version)), &engine.AofExpireCtx{
		NeedAof:  false,
		ExpireAt: nil,
	}
}

func execExist(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	_, exist := db.GetEntity(key)
	if !exist {
		return reply.MakeIntReply(0), nil
	}

	return reply.MakeIntReply(1), nil
}

func execPersist(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	_, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	_, exists = db.TTLMap().Get(key)
	if !exists {
		return reply.MakeIntReply(0), nil
	}

	db.Persist(key)

	return reply.MakeIntReply(1), &engine.AofExpireCtx{NeedAof: true}
}

func init() {
	engine.RegisterCommand("Del", execDel, writeFirstKey, 2, engine.FlagWrite)
	engine.RegisterCommand("ExpireAt", execExpireAt, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("Expire", execExpire, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("KeyVersion", execKeyVersion, writeFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("Exist", execExist, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("Persist", execPersist, writeFirstKey, 2, engine.FlagWrite)
}
