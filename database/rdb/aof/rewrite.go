package aof

import (
	"github.com/dawnzzz/simple-redis/config"
	"github.com/dawnzzz/simple-redis/interface/database"
	"github.com/dawnzzz/simple-redis/lib/utils"
	"github.com/dawnzzz/simple-redis/logger"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type RewriteCtx struct {
	tmpFile  *os.File // 重写时用到的临时文件
	fileSize int64    // 重写时文件大小
	dbIndex  int      // 重写时的当前数据库
}

func (persister *Persister) newRewritePersister() *Persister {
	tmpDB := persister.tmpDBMaker()
	return &Persister{
		db:          tmpDB,
		aofFilename: persister.aofFilename,
	}
}

func (persister *Persister) Rewrite(rewriteWait *sync.WaitGroup, rewriting *atomic.Bool) error {
	logger.Info("rewrite start...")
	persister.aofRewriting.Add(1)
	rewriting.Store(true)
	defer persister.aofRewriting.Done()
	defer func() {
		if rewriteWait != nil {
			rewriteWait.Done() // 通知 server 重写结束
		}
		rewriting.Store(false)
		logger.Info("rewrite finished...")
	}()

	rewriteCtx, err := persister.StartRewrite()
	if err != nil {
		return err
	}

	err = persister.DoRewrite(rewriteCtx)
	if err != nil {
		return err
	}

	err = persister.FinishRewrite(rewriteCtx)
	if err != nil {
		return err
	}

	return nil
}

// StartRewrite 暂停 AOF 写入 ->  准备重写 -> 恢复AOF写入。
func (persister *Persister) StartRewrite() (*RewriteCtx, error) {
	// 首先暂停aof写入
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// 调用 fsync 将缓冲区数据**落盘**，防止 AOF 文件不完整造成错误。
	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// 获取当前aof文件大小
	fileStat, _ := os.Stat(persister.aofFilename)
	fileSize := fileStat.Size()

	// 创建临时文件
	tmpFile, err := ioutil.TempFile("./", "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}

	return &RewriteCtx{
		tmpFile:  tmpFile,
		fileSize: fileSize,
		dbIndex:  persister.currentDB,
	}, nil
}

// DoRewrite 重写协程读取 AOF 文件中的**前一部分**（重写开始前的数据，不包括读写过程中写入的数据）并重写到临时文件中。
func (persister *Persister) DoRewrite(rewriteCtx *RewriteCtx) error {
	// 读取 AOF 文件在重写开始时获取到的文件大小长度的数据，这些数据是重写开始前的数据，**将重写的数据加载进入内存**。
	tmpFile := rewriteCtx.tmpFile

	rewritePersister := persister.newRewritePersister()
	rewritePersister.LoadAof(rewriteCtx.fileSize)

	// 依次将每一个数据库中的数据，**重写进入临时的 AOF 文件**中。
	for i := 0; i < config.Properties.Databases; i++ {
		// 对于每一个数据库，首先在临时文件中写入 Select 命令**选择正确的数据库**。
		data := reply.MakeMultiBulkStringReply(utils.StringsToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tmpFile.Write(data)
		if err != nil {
			return err
		}

		// 调用 foreach 函数，遍历数据库中的每一个 key，将每一个键值对写入到临时文件中。
		rewritePersister.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			bytes := utils.EntityToBytes(key, entity)
			if bytes != nil {
				_, _ = tmpFile.Write(bytes)
			}
			if expiration != nil {
				// 有 TTL
				bytes := utils.ExpireToBytes(key, *expiration)
				if bytes != nil {
					_, _ = tmpFile.Write(bytes)
				}
			}

			return true
		})
	}

	return nil
}

// FinishRewrite 暂停 AOF 写入 -> 将重写过程中产生的**新数据写入临时文件**中 -> 使用临时文件覆盖 AOF 文件（使用文件系统的 mv 命令保证安全） -> 恢复 AOF 写入。
func (persister *Persister) FinishRewrite(rewriteCtx *RewriteCtx) error {
	// 暂停 AOF 写入
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// **打开 AOF 文件**，并 seek 到**重写开始的位置**。
	src, err := os.Open(persister.aofFilename)
	if err != nil {
		logger.Error("open aofFilename failed: " + err.Error())
		return err
	}

	_, err = src.Seek(rewriteCtx.fileSize, 0)
	if err != nil {
		logger.Error("seek failed: " + err.Error())
		return err
	}

	tmpFile := rewriteCtx.tmpFile
	// 在临时文件中**写入一条 Select 命令**，使得临时文件切换到重写开始时选中的数据库。
	data := reply.MakeMultiBulkStringReply(utils.StringsToCmdLine("SELECT", strconv.Itoa(rewriteCtx.dbIndex))).ToBytes()
	_, err = tmpFile.Write(data)
	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return err
	}

	// 对齐数据库后，就可以把重写过程中产生的数据**复制到临时文件中了**。
	_, err = io.Copy(tmpFile, src)
	if err != nil {
		logger.Error("copy aof file failed: " + err.Error())
		return err
	}

	// **使用 mv 命令**，令临时文件**代替** AOF 文件。
	_ = persister.aofFile.Close()
	_ = src.Close()
	_ = tmpFile.Close()
	_ = os.Rename(tmpFile.Name(), persister.aofFilename)

	// **重新打开 AOF 文件**，并重新**写入一次 Select 命令**保证 AOF 文件中的数据库与 persister.currentDB 一致。
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	persister.aofFile = aofFile

	// write select command again to ensure aof file has the same db index with  persister.currentDB
	data = reply.MakeMultiBulkStringReply(utils.StringsToCmdLine("SELECT", strconv.Itoa(rewriteCtx.dbIndex))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err)
	}

	return nil
}
