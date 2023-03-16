package tcc

import (
	"Dawndis/database/engine"
	"Dawndis/interface/redis"
	"Dawndis/lib/timewheel"
	"Dawndis/logger"
	"Dawndis/redis/protocol/reply"
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

	writeKeys []string
	readKeys  []string

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
	}

	// 在时间轮中添加任务, 自动回滚超时未提交的事务
	taskKey := tx.id
	timewheel.Delay(maxLockTime, taskKey, func() {
		if tx.phase == phaseTry {
			logger.Info("abort transaction: " + tx.id)
			_ = tx.Cancel()
		}
	})

	return tx
}

// AddCmdLine TCC事务的try阶段，添加命令
func (tx *Transaction) AddCmdLine(cmdLine [][]byte) redis.Reply {
	if tx.phase != phaseCreate && tx.phase != phaseTry {
		return reply.MakeErrReply("ERR TRY ERROR")
	}
	tx.phase = phaseTry // 设置try阶段

	key := string(cmdLine[1])
	// 添加命令
	tx.cmdLines = append(tx.cmdLines, cmdLine)
	// 添加undo log
	tx.undoLogs = append(tx.undoLogs, tx.db.GetUndoLog(key)...)
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

	// 依次执行回滚
	for _, undoLog := range tx.undoLogs {
		tx.db.ExecWithLock(undoLog)
	}

	return reply.MakeOkReply()
}
