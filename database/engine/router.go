package engine

import (
	"Dawndis/interface/redis"
	"strings"
	"time"
)

// AofExpireCtx 记录在执行命令时，是否需要AOF持久化，是否有过期时间
type AofExpireCtx struct {
	NeedAof  bool
	ExpireAt *time.Time
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) (redis.Reply, *AofExpireCtx)

// PreFunc returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

var cmdTable = make(map[string]*command)

type command struct {
	executor ExecFunc
	prepare  PreFunc // return related keys command
	arity    int     // allow number of args, arity < 0 means len(args) >= -arity
	flags    int     // flagWrite or flagReadOnly
}

const (
	FlagWrite    = 0
	FlagReadOnly = 1
)

// RegisterCommand registers a new command
// arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
// for example: the arity of `get` is 2, `mget` is -2
func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, arity int, flags int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		prepare:  prepare,
		arity:    arity,
		flags:    flags,
	}
}

func IsReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	if cmd, ok := cmdTable[name]; ok && (cmd.flags&FlagReadOnly > 0) {
		return true
	}

	return false
}
func GetWriteReadKeys(cmdLine [][]byte) ([]string, []string) {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmdName = strings.ToLower(cmdName)

	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil, nil
	}

	prepare := cmd.prepare
	write, read := prepare(cmdLine[1:])

	return write, read
}
