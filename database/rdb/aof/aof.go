package aof

import (
	"context"
	"errors"
	"github.com/dawnzzz/simple-redis/interface/database"
	"github.com/dawnzzz/simple-redis/lib/utils"
	"github.com/dawnzzz/simple-redis/logger"
	"github.com/dawnzzz/simple-redis/redis/connection"
	"github.com/dawnzzz/simple-redis/redis/parser"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	FsyncAlways   = iota // 每一个命令都会进行刷盘操作
	FsyncEverySec        // 每秒进行一次刷盘操作
	FsyncNo              // 不主动进行刷盘操作，交给操作系统去决定
)

type CmdLine [][]byte

const (
	aofQueueSize = 1 << 16
)

type Persister struct {
	ctx         context.Context
	cancel      context.CancelFunc
	db          database.DBEngine
	tmpDBMaker  func() database.DBEngine
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	aofFsync    int // AOF 刷盘策略
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	// 表示正在aof重写，同时只有一个aof重写
	aofRewriting sync.WaitGroup
	currentDB    int
}

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

func NewPersister(db database.DBEngine, filename string, load bool, fsync int, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	if fsync < FsyncAlways || fsync > FsyncNo {
		return nil, errors.New("load aof failed, aof fsync must be: 0: always, 1: every sec, 2: no")
	}
	persister := &Persister{}
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.aofFilename = filename
	persister.aofFsync = fsync
	persister.currentDB = 0

	if load {
		persister.LoadAof(0) // 加载全部数据
	}

	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{})

	go func() {
		// 监听aofChan，写入 AOF 文件
		persister.listenCmd()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	if persister.aofFsync == FsyncEverySec { // 每秒钟进行刷盘同步
		persister.fsyncEverySecond()
	}

	return persister, nil
}

func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.pausingAof.Lock()
				if err := persister.aofFile.Sync(); err != nil {
					logger.Errorf("fsync failed: %v", err)
				}
				persister.pausingAof.Unlock()
			case <-persister.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

// Close 关闭 AOF 持久化close
func (persister *Persister) Close() {
	// 等待 AOF 重写完成
	persister.aofRewriting.Wait()

	if persister.aofFile != nil {
		// 先**关闭 aofChan 通道**，接着**等待 AOF 持久化完成**（persister.listenCmd 方法结束），**关闭 AOF 文件句柄**。
		close(persister.aofChan)
		<-persister.aofFinished // wait for aof finished
		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}

	// 调用 persister 中上下文 cancel 方法，用于结束 persister.fsyncEverySecond 方法。
	persister.cancel()
}

// LoadAof 用于读取 AOF文件，这个方法在监听 aofChan 之前使用。
func (persister *Persister) LoadAof(maxBytes int64) {
	// 首先将 aofChan 设置为 nil，因为 persister.db.Exec 在执行 AOF 文件中的命令时，可能又会向 aofChan 中加入命令。
	// 这些命令是不需要加入到 aofChan 中的（加入 aofChan 中数据会出错，因为这算是又在 AOF 文件中记录了一次），从 AOF 文件中读取并执行即可。
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	// 打开 AOF 文件，从 AOF 文件中读取 maxBytes 字节的数据。
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}

	// 读取 AOF 文件复用了协议解析器，fakeConn 仅仅用于持久化操作中（它表示一个**虚拟的客户端连接**，仅仅用于执行 AOF 文件中的命令）。
	ch := parser.ParseStream(reader)
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				// aof file read finish
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}

		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}

		// 执行
		r, ok := p.Data.(*reply.MultiBulkStringReply)
		fakeConn := connection.NewFakeConn()
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		ret := persister.db.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) {
			logger.Error("exec err", string(ret.ToBytes()))
		}

		// 遇到 select 切换aof当前数据库
		if strings.ToLower(string(r.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(r.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}
	}
}

// 监听aofChan，写入 AOF 文件
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

// 用于将一条命令写入到 AOF 文件中。
func (persister *Persister) writeAof(p *payload) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	// 首先，**选择正确的数据库**。
	// 每个客户端都可以选择自己的数据库，所以 payload 中要保存客户端选择的数据库。
	// **选择的数据库与 AOF 文件中当前的数据库不一致时写入一条 Select 命令**。
	if p.dbIndex != persister.currentDB {
		selectCmd := utils.StringsToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		data := reply.MakeMultiBulkStringReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return // skip this command
		}
		persister.currentDB = p.dbIndex
	}

	// 接着**写入命令内容**。
	data := reply.MakeMultiBulkStringReply(p.cmdLine).ToBytes()
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}

	// 如果刷盘策略为 FsyncAlways（每一条命令都刷盘），则调用 persister.aofFile.Sync 刷盘。
	if persister.aofFsync == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

// SaveCmdLine 用于向 aofChan 通道中发送一条命令。
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// 如果 aofChan 为空，则说明在 LoadAof 过程中，直接返回即可。
	if persister.aofChan == nil {
		return
	}

	// 如果刷盘策略为 FsyncAlways，则直接调用 persister.writeAof 方法将命令写入 AOF 文件中。
	if persister.aofFsync == FsyncAlways {
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}

	// 否则，就将命令和执行这条命令的数据库发送到 aofChan 中。
	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
}
