package engine

import (
	"github.com/dawnzzz/simple-redis/config"
	"github.com/dawnzzz/simple-redis/interface/redis"
	"github.com/dawnzzz/simple-redis/lib/utils"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"strings"
	"time"
)

// ExecMulti multi命令执行阶段
func (db *DB) ExecMulti(c redis.Connection) redis.Reply {
	return db.ExecMultiCommand(c.GetEnqueuedCmdLine(), c.GetWatching())
}

func (db *DB) ExecMultiCommand(cmdLines [][][]byte, watching map[string]uint32) redis.Reply {
	// 此时不需要检查是否有语法错误，因为在排队过程中已经检查过了

	// // 获取所有需要加锁的key
	writeKeys := make([]string, len(cmdLines))
	readKeys := make([]string, len(cmdLines)+len(watching))
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		// 获取命令
		cmd, _ := cmdTable[cmdName]

		// 获取需要加锁的key
		prepare := cmd.prepare
		write, read := prepare(cmdLine[1:])
		writeKeys = append(writeKeys, write...)
		readKeys = append(readKeys, read...)
	}

	// 获取需要watch的key
	watchingKeys := make([]string, 0, len(watching))
	for key := range watching {
		watchingKeys = append(watchingKeys, key)
	}
	readKeys = append(readKeys, watchingKeys...)

	// 执行前的加锁
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)

	// 执行前检查version是否变化
	versionChanged := db.checkVersionChanged(watching)
	if versionChanged {
		// version变化了，什么都不执行
		return reply.MakeNullBulkStringReply()
	}

	// 执行
	var results [][]byte     // 存储命令执行的结果
	var undoLogs [][]CmdLine // undo日志
	aborted := false
	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(string(cmdLine[0]))
		// 获取命令
		cmd, _ := cmdTable[cmdName]

		if config.Properties.OpenAtomicTx {
			// 开启原子性事务，记录undo日志
			key := string(cmdLine[1])
			undoLogs = append(undoLogs, db.GetUndoLog(key))
		}

		// 执行命令
		fun := cmd.executor
		r, aofExpireCtx := fun(db, cmdLine[1:])

		if config.Properties.OpenAtomicTx && reply.IsErrorReply(r) {
			// 如果开启原子性事务，并且其中一条命令执行失败了，全部全部回滚
			undoLogs = undoLogs[:len(undoLogs)-1] // 不执行最后一条失败的undo log
			aborted = true
			break
		}

		results = append(results, []byte(r.DataString()))
		db.afterExec(r, aofExpireCtx, cmdLine)
	}

	if len(results) == 0 {
		return reply.MakeEmptyMultiBulkStringReply()
	}

	if config.Properties.OpenAtomicTx && aborted { // 开启原子性事务并且执行失败了，则进行回滚
		size := len(undoLogs)
		for i := size - 1; i >= 0; i-- {
			undoLog := undoLogs[i]
			if len(undoLog) == 0 {
				continue
			}
			for _, cmdLine := range undoLog {
				db.ExecWithLock(cmdLine)
			}
		}
		return reply.MakeErrReply("EXECABORT Transaction rollback because of errors during executing. (atomic tx is open)")
	}

	// 未开启原子性事务，或者执行成功
	// 写命令增加版本
	db.AddVersion(writeKeys...)

	return reply.MakeMultiBulkStringReply(results)
}

func (db *DB) CheckSupportMulti(cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, _ := cmdTable[cmdName]
	if cmd.prepare == nil {
		return reply.MakeErrReply("ERR command '" + cmdName + "' cannot be used in MULTI")
	}

	return nil
}

func (db *DB) GetUndoLog(key string) []CmdLine {
	undoLog := make([]CmdLine, 0, 3)
	entity, exist := db.GetEntity(key)
	if !exist {
		// 不存在，直接删除key
		undoLog = append(undoLog, utils.StringsToCmdLine("DEL", key))
	} else {
		// 存在，首先删除新的值
		undoLog = append(undoLog, utils.StringsToCmdLine("DEL", key))
		// 接着恢复为原来的值
		undoLog = append(undoLog, utils.EntityToCmdLine(key, entity))
		// 设置 TTL
		if raw, ok := db.ttlMap.Get(key); ok { // 获取过期时间
			// 如果有过期时间
			expireTime, _ := raw.(time.Time)
			// 设置过期时间
			undoLog = append(undoLog, utils.ExpireToCmdLine(key, expireTime))
		}
	}

	return undoLog
}
