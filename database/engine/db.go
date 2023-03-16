package engine

import (
	"Dawndis/datastruct/dict"
	"Dawndis/datastruct/lock"
	"Dawndis/interface/database"
	"Dawndis/interface/redis"
	"Dawndis/lib/utils"
	"Dawndis/redis/protocol/reply"
	"strings"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockSize     = 1024
)

type DB struct {
	index      int // 数据库号
	data       dict.Dict
	ttlMap     dict.Dict
	versionMap dict.Dict
	locker     *lock.Locks
	addAof     func(line CmdLine)
}

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

func MakeDB() *DB {
	return &DB{
		data:       dict.MakeConcurrentDict(dataDictSize),
		ttlMap:     dict.MakeConcurrentDict(ttlDictSize),
		versionMap: dict.MakeConcurrentDict(dataDictSize),
		locker:     lock.Make(lockSize),
		addAof:     func(line CmdLine) {},
	}
}

func MakeBasicDB() *DB {
	return &DB{
		data:   dict.MakeSimpleDict(),
		ttlMap: dict.MakeSimpleDict(),
		locker: lock.Make(1),
		addAof: func(line CmdLine) {},
	}
}

// Flush Warning! clean all db data
func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
	db.locker = lock.Make(lockSize)
}

// Exec executes command within one database
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	if c.GetMultiStatus() { // 如果客户端已经进入了multi
		// 检查是否有语法错误
		if errReply := db.CheckSyntaxErr(cmdLine); errReply != nil {
			c.EnqueueSyntaxErrQueue(errReply) // 语法有错误
			return errReply
		}
		// 获取命令，检查是否支持multi
		if errReply := db.CheckSupportMulti(cmdLine); errReply != nil {
			c.EnqueueSyntaxErrQueue(errReply) // 语法有错误
			return errReply
		}

		// 语法没有错误，则进入队列等待执行
		c.EnqueueCmdLine(cmdLine)

		return reply.MakeStatusReply("QUEUED")
	}

	// 正常执行的命令
	return db.execNormalCommand(cmdLine)
}

func (db *DB) execNormalCommand(cmdLine [][]byte) redis.Reply {
	if errReply := db.CheckSyntaxErr(cmdLine); errReply != nil {
		// 检查是否有语法错误
		return errReply
	}
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 获取命令
	cmd, _ := cmdTable[cmdName]

	// 执行前的加锁
	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	// 执行
	fun := cmd.executor
	r, aofExpireCtx := fun(db, cmdLine[1:])
	db.afterExec(r, aofExpireCtx, cmdLine)
	// 非读命令执行增加版本
	db.addVersion(write...)

	return r
}

func (db *DB) ExecWithLock(cmdLine CmdLine) redis.Reply {
	if errReply := db.CheckSyntaxErr(cmdLine); errReply != nil {
		// 检查是否有语法错误
		return errReply
	}

	// 执行
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, _ := cmdTable[cmdName]
	fun := cmd.executor
	r, aofExpireCtx := fun(db, cmdLine[1:])
	db.afterExec(r, aofExpireCtx, cmdLine)

	return r
}

// afterExec 命令执行之后的相关处理，如持久化相关等
func (db *DB) afterExec(r redis.Reply, aofExpireCtx *AofExpireCtx, cmdLine [][]byte) {
	key := string(cmdLine[1])
	// 持久化相关
	if aofExpireCtx != nil && aofExpireCtx.NeedAof {
		// 需要进行AOF持久化
		db.addAof(cmdLine)
		if aofExpireCtx.ExpireAt != nil {
			// 有过期时间
			db.addAof(utils.ExpireToCmdLine(key, *aofExpireCtx.ExpireAt))
		}
	}
}

// 验证参数数量是否正确
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

func (db *DB) GetIndex() int {
	return db.index
}

func (db *DB) SetIndex(index int) {
	if index > 0 {
		db.index = index
	}
}

func (db *DB) SetAddAof(addAof func(line CmdLine)) {
	db.addAof = addAof
}

func (db *DB) AddAof(line CmdLine) {
	db.addAof(line)
}

func (db *DB) GetDBSize() (int, int) {
	return db.data.Len(), db.ttlMap.Len()
}

// ForEach traverses all the keys in the database
func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, val interface{}) bool {
		entity, _ := val.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)

		if ok {
			// 有过期时间
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}
		return cb(key, entity, expiration)
	})
}

func (db *DB) CheckSyntaxErr(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 获取命令
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}

	return nil
}
