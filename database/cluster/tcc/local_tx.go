package tcc

import (
	"Dawndis/database/engine"
	"Dawndis/interface/redis"
	"Dawndis/lib/timewheel"
	"Dawndis/logger"
	"Dawndis/redis/protocol/reply"
	"strconv"
	"time"
)

const (
	phaseCreate = iota
	phaseTry
	phaseCommit
	phaseCancel
)

var (
	maxLockTime = time.Second * 5
)

// Transaction TCC本地事务
type Transaction struct {
	id    string // 事务id
	phase int    // 阶段，分别是try、commit、cancel三个阶段

	writeKeys   []string
	readKeys    []string
	watchedKeys map[string]uint32 // 记录watch时的版本

	cmdLines [][][]byte // 本次事务中包含的命令
	undoLogs [][][]byte // 本次事务的undo log

	db *engine.DB
}

// NewTransaction 开启一个TCC本地事务
func NewTransaction(id string, db *engine.DB) *Transaction {
	tx := &Transaction{
		id:    id,
		db:    db,
		phase: phaseCreate,

		watchedKeys: make(map[string]uint32),
	}

	return tx
}

// AddCmdLine TCC事务的try阶段，添加命令
func (tx *Transaction) AddCmdLine(cmdLine [][]byte) redis.Reply {
	if tx.phase != phaseCreate && tx.phase != phaseTry {
		return reply.MakeErrReply("ERR TRY ERROR")
	}
	tx.phase = phaseTry // 设置try阶段

	// 添加命令
	tx.cmdLines = append(tx.cmdLines, cmdLine)

	// 计算需要读写的key
	writeKeys, readKeys := engine.GetWriteReadKeys(cmdLine)
	tx.writeKeys = append(tx.writeKeys, writeKeys...)
	tx.readKeys = append(tx.readKeys, readKeys...)

	return reply.MakeOkReply()
}

// Try TCC事务的结束try阶段，锁定key
func (tx *Transaction) Try() redis.Reply {
	if tx.phase != phaseTry {
		return reply.MakeErrReply("ERR TRY ERROR")
	}

	// 锁定需要读写的key
	tx.db.RWLocks(tx.writeKeys, tx.readKeys)

	// 在时间轮中添加任务, 自动回滚超时未提交的事务
	taskKey := tx.id

	timewheel.Delay(maxLockTime, taskKey, func() {
		if tx.phase == phaseTry {
			logger.Info("abort transaction: " + tx.id)
			_ = tx.Cancel()
		}
	})

	// 读取版本是否变化
	for key, oldVersion := range tx.watchedKeys {
		newVersion := tx.db.GetVersion(key)
		if oldVersion != newVersion {
			// 获取key的版本，如果版本变化了，则返回错误
			return reply.MakeErrReply("ERR VERSION CHANGED")
		}
	}

	return reply.MakeOkReply()
}

func (tx *Transaction) SaveVersion(cmdLine [][]byte) redis.Reply {
	if tx.phase != phaseCreate && tx.phase != phaseTry {
		return reply.MakeErrReply("ERR TRY ERROR")
	}
	tx.phase = phaseTry // 设置try阶段

	key := string(cmdLine[1])
	version, err := strconv.Atoi(string(cmdLine[2]))
	if err != nil {
		return reply.MakeErrReply("ERR TRY WATCHED VERSION MUST BE NUMBER")
	}

	tx.watchedKeys[key] = uint32(version)
	tx.readKeys = append(tx.readKeys, key) // 需要加读锁

	return reply.MakeOkReply()
}

// Commit TCC事务的commit阶段
func (tx *Transaction) Commit() redis.Reply {
	defer func() {
		// 解锁
		tx.db.RWUnLocks(tx.writeKeys, tx.readKeys)
		// 设置阶段
		tx.phase = phaseCommit
	}()

	// 依次执行命令
	var results [][]byte // 存储命令执行的结果
	for _, cmdLine := range tx.cmdLines {
		r := tx.db.ExecWithLock(cmdLine)
		cmdName := string(cmdLine[0])
		key := string(cmdLine[1])
		if !engine.IsReadOnlyCommand(cmdName) && !reply.IsErrorReply(r) {
			// 写命令、执行成功，则增加版本
			tx.db.AddVersion(key)
		}
		results = append(results, []byte(r.DataString()))
	}

	return reply.MakeMultiBulkStringReply(results)
}

// Cancel TCC事务的cancel阶段
func (tx *Transaction) Cancel() redis.Reply {
	defer func() {
		// 解锁
		tx.db.RWUnLocks(tx.writeKeys, tx.readKeys)
		// 设置阶段
		tx.phase = phaseCancel
	}()

	return reply.MakeOkReply()
}
