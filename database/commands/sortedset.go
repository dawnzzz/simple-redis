package commands

import (
	"github.com/dawnzzz/simple-redis/database/engine"
	"github.com/dawnzzz/simple-redis/datastruct/sortedset"
	"github.com/dawnzzz/simple-redis/interface/database"
	"github.com/dawnzzz/simple-redis/interface/redis"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"strconv"
	"strings"
)

func execZAdd(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args)%2 != 1 {
		return reply.MakeSyntaxErrReply(), nil
	}

	key := string(args[0])
	size := (len(args) - 1) / 2
	elements := make([]*sortedset.Element, size)

	for i := 0; i < size; i++ {
		scoreValue := args[2*i+1]
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(scoreValue), 64)
		if err != nil {
			return reply.MakeErrReply("ERR value is not a valid float"), nil
		}
		elements[i] = &sortedset.Element{
			Member: member,
			Score:  score,
		}
	}

	sortedSet, _, errReply := getOrInitSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	i := 0
	for _, e := range elements {
		if sortedSet.Add(e.Member, e.Score) {
			i++
		}
	}

	return reply.MakeIntReply(int64(i)), &engine.AofExpireCtx{
		NeedAof:  true,
		ExpireAt: nil,
	}
}

func execZCard(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return reply.MakeIntReply(0), nil
	}

	length := sortedSet.Len()
	return reply.MakeIntReply(length), nil
}

func execZScore(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return reply.MakeNullBulkStringReply(), nil
	}

	element, ok := sortedSet.Get(member)
	if !ok {
		return reply.MakeNullBulkStringReply(), nil
	}

	score := strconv.FormatFloat(element.Score, 'f', -1, 64)
	return reply.MakeBulkStringReply([]byte(score)), nil
}

func execZCount(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])

	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return reply.MakeErrReply(err.Error()), nil
	}

	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return reply.MakeErrReply(err.Error()), nil
	}

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return reply.MakeIntReply(0), nil
	}

	return reply.MakeIntReply(sortedSet.Count(min, max)), nil
}

func execZIncrBy(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	rawDelta := string(args[1])
	field := string(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not a valid float"), nil
	}

	// get or init entity
	sortedSet, _, errReply := getOrInitSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	element, exists := sortedSet.Get(field)
	if !exists {
		sortedSet.Add(field, delta)

		return reply.MakeBulkStringReply(args[1]), &engine.AofExpireCtx{NeedAof: true}
	}

	score := element.Score + delta
	sortedSet.Add(field, score)
	bytes := []byte(strconv.FormatFloat(score, 'f', -1, 64))

	return reply.MakeBulkStringReply(bytes), &engine.AofExpireCtx{NeedAof: true}
}

func execZRange(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) != 3 && len(args) != 4 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'zrange' command"), nil
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return reply.MakeErrReply("syntax error"), nil
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}

	return range0(db, key, start, stop, withScores, false)
}

func execZRevRange(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) != 3 && len(args) != 4 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'zrange' command"), nil
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return reply.MakeErrReply("syntax error"), nil
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}

	return range0(db, key, start, stop, withScores, true)
}

func execZRangeByScore(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) < 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command"), nil
	}
	key := string(args[0])

	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return reply.MakeErrReply(err.Error()), nil
	}

	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return reply.MakeErrReply(err.Error()), nil
	}

	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return reply.MakeErrReply("ERR syntax error"), nil
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
				}
				i += 3
			} else {
				return reply.MakeErrReply("ERR syntax error"), nil
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, false)
}

func execZRevRangeByScore(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) < 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command"), nil
	}
	key := string(args[0])

	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return reply.MakeErrReply(err.Error()), nil
	}

	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return reply.MakeErrReply(err.Error()), nil
	}

	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return reply.MakeErrReply("ERR syntax error"), nil
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
				}
				i += 3
			} else {
				return reply.MakeErrReply("ERR syntax error"), nil
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, true)
}

func execZRank(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return reply.MakeNullBulkStringReply(), nil
	}

	rank := sortedSet.GetRank(member, false)
	if rank < 0 {
		return reply.MakeNullBulkStringReply(), nil
	}

	return reply.MakeIntReply(rank), nil
}

func execZRevRank(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}

	if sortedSet == nil {
		return reply.MakeNullBulkStringReply(), nil
	}

	rank := sortedSet.GetRank(member, true)
	if rank < 0 {
		return reply.MakeNullBulkStringReply(), nil
	}

	return reply.MakeIntReply(rank), nil
}

func execZRem(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	// parse args
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}

	// get entity
	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return reply.MakeIntReply(0), nil
	}

	var deleted int64 = 0
	for _, field := range fields {
		if sortedSet.Remove(field) {
			deleted++
		}
	}

	return reply.MakeIntReply(deleted), &engine.AofExpireCtx{NeedAof: true}
}

func execRemRangeByRank(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("ERR value is not an integer or out of range"), nil
	}

	// get data
	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return reply.MakeIntReply(0), nil
	}

	// compute index
	size := sortedSet.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return reply.MakeIntReply(0), nil
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	// assert: start in [0, size - 1], stop in [start, size]
	removed := sortedSet.RemoveByRank(start, stop)

	return reply.MakeIntReply(removed), &engine.AofExpireCtx{NeedAof: true}
}

func execRemRangeByScore(db *engine.DB, args [][]byte) (redis.Reply, *engine.AofExpireCtx) {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'zremrangebyscore' command"), nil
	}
	key := string(args[0])

	min, err := sortedset.ParseScoreBorder(string(args[1]))
	if err != nil {
		return reply.MakeErrReply(err.Error()), nil
	}

	max, err := sortedset.ParseScoreBorder(string(args[2]))
	if err != nil {
		return reply.MakeErrReply(err.Error()), nil
	}

	// get data
	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}

	removed := sortedSet.RemoveByScore(min, max)

	return reply.MakeIntReply(removed), &engine.AofExpireCtx{NeedAof: true}
}

func getAsSortedSet(db *engine.DB, key string) (sortedSet *sortedset.SortedSet, errorReply reply.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	sortedSet, ok := entity.Data.(*sortedset.SortedSet)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return sortedSet, nil
}

func getOrInitSortedSet(db *engine.DB, key string) (sortedSet *sortedset.SortedSet, inited bool, errReply reply.ErrorReply) {
	sortedSet, errReply = getAsSortedSet(db, key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if sortedSet == nil {
		sortedSet = sortedset.MakeSortedSet()
		db.PutEntity(key, &database.DataEntity{
			Data: sortedSet,
		})
		inited = true
	}
	return sortedSet, inited, nil
}

func range0(db *engine.DB, key string, start int64, stop int64, withScores bool, desc bool) (redis.Reply, *engine.AofExpireCtx) {
	// get data
	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}

	// compute index
	size := sortedSet.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	// assert: start in [0, size - 1], stop in [start, size]
	slice := sortedSet.Range(start, stop, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr)
			i++
		}
		return reply.MakeMultiBulkStringReply(result), nil
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return reply.MakeMultiBulkStringReply(result), nil
}

func rangeByScore0(db *engine.DB, key string, min *sortedset.ScoreBorder, max *sortedset.ScoreBorder, offset int64, limit int64, withScores bool, desc bool) (redis.Reply, *engine.AofExpireCtx) {
	// get data
	sortedSet, errReply := getAsSortedSet(db, key)
	if errReply != nil {
		return errReply, nil
	}
	if sortedSet == nil {
		return reply.MakeEmptyMultiBulkStringReply(), nil
	}

	slice := sortedSet.RangeByScore(min, max, offset, limit, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = []byte(element.Member)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = []byte(scoreStr)
			i++
		}
		return reply.MakeMultiBulkStringReply(result), nil
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return reply.MakeMultiBulkStringReply(result), nil
}

func init() {
	engine.RegisterCommand("ZAdd", execZAdd, writeFirstKey, -4, engine.FlagWrite)
	engine.RegisterCommand("ZCard", execZCard, readFirstKey, 2, engine.FlagReadOnly)
	engine.RegisterCommand("ZScore", execZScore, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("ZCount", execZCount, readFirstKey, 4, engine.FlagReadOnly)
	engine.RegisterCommand("ZIncrBy", execZIncrBy, writeFirstKey, 4, engine.FlagWrite)
	engine.RegisterCommand("ZRange", execZRange, readFirstKey, -4, engine.FlagReadOnly)
	engine.RegisterCommand("ZRevRange", execZRevRange, readFirstKey, -4, engine.FlagReadOnly)
	engine.RegisterCommand("ZRangeByScore", execZRangeByScore, readFirstKey, -4, engine.FlagReadOnly)
	engine.RegisterCommand("ZRevRangeByScore", execZRevRangeByScore, readFirstKey, -4, engine.FlagReadOnly)
	engine.RegisterCommand("ZRank", execZRank, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("ZRevRank", execZRevRank, readFirstKey, 3, engine.FlagReadOnly)
	engine.RegisterCommand("ZRem", execZRem, writeFirstKey, -3, engine.FlagWrite)
	engine.RegisterCommand("ZRemRangeByRank", execRemRangeByRank, writeFirstKey, 4, engine.FlagWrite)
	engine.RegisterCommand("ZRemRangeByScore", execRemRangeByScore, writeFirstKey, 4, engine.FlagWrite)
}
