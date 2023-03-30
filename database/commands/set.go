package commands

import (
	"github.com/dawnzzz/simple-redis/database/engine"
	Set "github.com/dawnzzz/simple-redis/datastruct/set"
	"github.com/dawnzzz/simple-redis/interface/database"
	"github.com/dawnzzz/simple-redis/interface/redis"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"strconv"
)

func execSAdd(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	// 查找是否有key
	key := string(args[0])
	set, _, errReply := getOrInitSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	// 添加members
	count := 0
	for _, arg := range args[1:] {
		count += set.Add(string(arg))
	}

	return reply.MakeIntReply(int64(count)), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execSCard(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	set, errReply := getAsSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if set == nil {
		return reply.MakeIntReply(0), nil
	}

	return reply.MakeIntReply(int64(set.Len())), nil
}

func execSDiff(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	// 解析所有的key
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}

	// 计算第一个集合与其他集合之间的差异
	// 第一个集合中独有的元素

	// 第一个集合为空，直接返回
	diffSet, errReply := getAsSet(db, keys[0])
	if errReply != nil {
		return errReply, nil
	}
	if diffSet == nil {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}

	for _, key := range keys[1:] {
		set, errReply := getAsSet(db, key)
		if errReply != nil {
			return errReply, nil
		}

		if set == nil {
			continue
		}

		diffSet = diffSet.Diff(set)
	}

	// 获取结果
	if diffSet == nil {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}

	result := make([][]byte, 0, diffSet.Len())
	diffSet.ForEach(func(member string) bool {
		result = append(result, []byte(member))

		return true
	})

	return reply.MakeMultiBulkStringReply(result), nil
}

func execSInter(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	// 解析所有的key
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}

	// 计算这些集合的并集
	var interSet Set.Set

	for _, key := range keys {
		set, errReply := getAsSet(db, key)
		if errReply != nil {
			return errReply, nil
		}
		if set == nil {
			continue
		}

		if interSet == nil {
			interSet = set
			continue
		}

		interSet = set.Intersect(interSet)
	}

	// 获取结果
	if interSet == nil {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}

	result := make([][]byte, 0, interSet.Len())
	interSet.ForEach(func(member string) bool {
		result = append(result, []byte(member))

		return true
	})

	return reply.MakeMultiBulkStringReply(result), nil
}

func execSIsMember(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	member := string(args[1])

	set, errReply := getAsSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if set == nil {
		return reply.MakeIntReply(0), nil
	}

	if set.Has(member) {
		return reply.MakeIntReply(1), nil
	}

	return reply.MakeIntReply(0), nil
}

func execSMembers(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	set, errReply := getAsSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if set == nil {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}

	results := make([][]byte, 0, set.Len())
	set.ForEach(func(member string) bool {
		results = append(results, []byte(member))

		return true
	})

	return reply.MakeMultiBulkStringReply(results), nil
}

func execSPop(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) > 2 {
		return reply.MakeSyntaxErrReply(), nil
	}

	key := string(args[0])
	count := 1
	if len(args) == 2 {
		var err error
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
		}
	}

	set, errReply := getAsSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if set == nil {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}

	members := set.RandomDistinctMembers(count)
	result := make([][]byte, 0, len(members))
	for _, member := range members {
		set.Remove(member)
		result = append(result, []byte(member))
	}

	return reply.MakeMultiBulkStringReply(result), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execSRandMember(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) > 2 {
		return reply.MakeSyntaxErrReply(), nil
	}

	key := string(args[0])
	count := 1
	if len(args) == 2 {
		var err error
		count, err = strconv.Atoi(string(args[1]))
		if err != nil {
			return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
		}
	}

	set, errReply := getAsSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if set == nil {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}

	members := set.RandomDistinctMembers(count)
	result := make([][]byte, 0, len(members))
	for _, member := range members {
		result = append(result, []byte(member))
	}

	return reply.MakeMultiBulkStringReply(result), nil
}

func execSRem(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	// 查找是否有key
	key := string(args[0])
	set, errReply := getAsSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if set == nil {
		return reply.MakeIntReply(0), nil
	}

	// 删除members
	count := 0
	for _, arg := range args[1:] {
		count += set.Remove(string(arg))
	}

	return reply.MakeIntReply(int64(count)), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execSUnion(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	// 解析所有的key
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}

	// 计算这些集合的差集
	unionSet := Set.MakeSimpleSet()
	for _, key := range keys {
		set, errReply := getAsSet(db, key)
		if errReply != nil {
			return errReply, nil
		}

		if set == nil {
			continue
		}

		unionSet = set.Union(unionSet)
	}

	// 获取结果
	result := make([][]byte, 0, unionSet.Len())
	unionSet.ForEach(func(member string) bool {
		result = append(result, []byte(member))

		return true
	})

	return reply.MakeMultiBulkStringReply(result), nil
}

func getAsSet(db *engine.DB, key string) (set Set.Set, errorReply reply.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(Set.Set)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return set, nil
}

func getOrInitSet(db *engine.DB, key string) (set Set.Set, inited bool, errReply reply.ErrorReply) {
	set, errReply = getAsSet(db, key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if set == nil {
		set = Set.MakeSimpleSet()
		db.PutEntity(key, &database.DataEntity{
			Data: set,
		})
		inited = true
	}
	return set, inited, nil
}

func init() {
	engine.RegisterCommand("SAdd", execSAdd, writeFirstKey, -3, engine.FlagWrite)
	engine.RegisterCommand("SCard", execSCard, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("SDiff", execSDiff, prepareSetCalculate, -2, engine.FlagReadOnly)
	engine.RegisterCommand("SInter", execSInter, prepareSetCalculate, -2, engine.FlagReadOnly)
	engine.RegisterCommand("SIsMember", execSIsMember, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("SMembers", execSMembers, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("SPop", execSPop, writeFirstKey, -2, engine.FlagWrite)
	engine.RegisterCommand("SRandMember", execSRandMember, readFirstKey, -2, engine.FlagReadOnly)
	engine.RegisterCommand("SRem", execSRem, writeFirstKey, -3, engine.FlagWrite)
	engine.RegisterCommand("SUnion", execSUnion, prepareSetCalculate, -2, engine.FlagReadOnly)
}
