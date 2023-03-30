package commands

import (
	"github.com/dawnzzz/simple-redis/database/engine"
	"github.com/dawnzzz/simple-redis/interface/database"
	"github.com/dawnzzz/simple-redis/interface/redis"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"strconv"
	"time"
)

const (
	upsertPolicy = iota // default
	insertPolicy        // set nx
	updatePolicy        // set ex
)

const unlimitedTTL int64 = 0

func execSet(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	value := args[1]

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)

	return &reply.OkReply{}, &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

// execSetNX sets string if not exists
func execSetNX(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	value := args[1]

	entity := &database.DataEntity{
		Data: value,
	}

	result := db.PutIfAbsent(key, entity)

	return reply.MakeIntReply(int64(result)), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

// execSetEX sets string and its ttl(s)
func execSetEX(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	value := args[2]

	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}, &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Second * time.Duration(ttl))
	db.Expire(key, expireTime)

	return &reply.OkReply{}, &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: &expireTime,
	}
}

// execPSetEX set a key's time to live in  milliseconds
func execPSetEX(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	value := args[2]

	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return &reply.SyntaxErrReply{}, nil
	}

	entity := &database.DataEntity{
		Data: value,
	}

	db.PutEntity(key, entity)
	expireTime := time.Now().Add(time.Millisecond * time.Duration(ttl))
	db.Expire(key, expireTime)

	return &reply.OkReply{}, &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: &expireTime,
	}
}

func execAppend(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	appendValue := args[1]

	value, errReply := GetAsString(db, key)
	if errReply != nil {
		return errReply, &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	entity := &database.DataEntity{
		Data: append(value, appendValue...),
	}

	db.PutEntity(key, entity)

	return &reply.OkReply{}, &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execGet(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	value, errReply := GetAsString(db, key)
	if errReply != nil {
		return errReply, &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	if value == nil {
		return &reply.NullBulkStringReply{}, &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	return reply.MakeBulkStringReply(value), &engine.AofExpireCtx{
		NeedAof:  false,
		ExpireAt: nil,
	}
}

func execIncr(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	return doIncrBy(db, key, 1)
}

func execDecr(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	return doIncrBy(db, key, -1)
}

func execIncrBy(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	by, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR step is not an integer or out of range"), &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	return doIncrBy(db, key, by)
}

func execDecrBy(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	by, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR step is not an integer or out of range"), &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	return doIncrBy(db, key, -by)
}

func execStrLen(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	value, errReply := GetAsString(db, key)
	if errReply != nil {
		return errReply, &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	if value == nil {
		return &reply.NullBulkStringReply{}, &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	return reply.MakeIntReply(int64(len(value))), &engine.AofExpireCtx{
		NeedAof:  false,
		ExpireAt: nil,
	}
}

func GetAsString(db *engine.DB, key string) ([]byte, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

func doIncrBy(db *engine.DB, key string, by int64) (redis.Reply, *engine.AofExpireCtx) {
	value, errReply := GetAsString(db, key)
	if errReply != nil {
		return errReply, &engine.AofExpireCtx{
			NeedAof:  false,
			ExpireAt: nil,
		}
	}

	// 数据存在，+by
	if value != nil {
		valueInt, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range"), &engine.AofExpireCtx{
				NeedAof:  false,
				ExpireAt: nil,
			}
		}
		if (valueInt > 0 && by > 0 && valueInt+by <= 0) || (valueInt < 0 && by < 0 && valueInt+by >= 0) {
			// 判断溢出
			return reply.MakeErrReply("ERR value is not an integer or out of range"), &engine.AofExpireCtx{
				NeedAof:  false,
				ExpireAt: nil,
			}
		}
		db.PutEntity(key, &database.DataEntity{
			Data: []byte(strconv.FormatInt(valueInt+by, 10)),
		})
		return reply.MakeIntReply(valueInt + by), &engine.AofExpireCtx{
			NeedAof:  true,
			ExpireAt: nil,
		}
	}

	// 数据不存在，设置为by
	db.PutEntity(key, &database.DataEntity{
		Data: []byte(strconv.FormatInt(by, 10)),
	})
	return reply.MakeIntReply(by), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func init() {
	engine.RegisterCommand("Set", execSet, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("SetNX", execSetNX, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("SetEX", execSetEX, writeFirstKey, 4, engine.FlagWrite)
	engine.RegisterCommand("Append", execAppend, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("Incr", execIncr, writeFirstKey, 2, engine.FlagWrite)
	engine.RegisterCommand("Decr", execDecr, writeFirstKey, 2, engine.FlagWrite)
	engine.RegisterCommand("IncrBy", execIncrBy, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("DecrBy", execDecrBy, writeFirstKey, 3, engine.FlagWrite)
	engine.RegisterCommand("Get", execGet, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("StrLen", execStrLen, readFirstKey, 2, engine.FlagReadOnly)
}
