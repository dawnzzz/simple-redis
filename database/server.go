package database

import (
	"Dawndis/config"
	"Dawndis/database/cluster"
	_ "Dawndis/database/commands"
	"Dawndis/database/engine"
	"Dawndis/database/rdb/aof"
	"Dawndis/interface/database"
	"Dawndis/interface/redis"
	"Dawndis/lib/utils"
	"Dawndis/logger"
	"Dawndis/redis/connection"
	"Dawndis/redis/protocol/reply"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server is a redis-server
type Server struct {
	dbSet        []*atomic.Value // *DB
	AofPersister *aof.Persister  // AOF 持久化
	AofFileSize  int64
	rewriteWait  sync.WaitGroup
	rewriting    atomic.Bool
	closed       chan struct{}
	cluster      *cluster.Cluster
}

// NewStandaloneServer creates a standalone redis server
func NewStandaloneServer() *Server {
	server := initServer()

	return server
}

// NewClusterServer 创建一个集群服务器
func NewClusterServer(peers []string) *Server {
	server := initServer()

	// 加入集群
	cluster := cluster.NewCluster(config.Properties.Self)
	cluster.AddPeers(peers...)
	if cluster == nil {
		logger.Fatalf("please set 'self'(self ip:port) in conf file")
	}
	server.cluster = cluster

	return server
}

func (s *Server) Exec(client redis.Connection, cmdLine [][]byte) redis.Reply {
	if s.cluster != nil {
		return s.execCluster(client, cmdLine)
	}

	return s.execStandalone(client, cmdLine) // 单机模式
}

// 单机模式执行命令的方式
func (s *Server) execStandalone(client redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))

	if (cmdName) == "ping" {
		logger.Debugf("received heart beat from %v", client.Name())
		return reply.MakePongStatusReply()
	}

	if _, ok := client.(*connection.FakeConn); !ok { // fakeConn不做校验
		if cmdName == "auth" {
			return Auth(client, cmdLine[1:])
		}
		if !isAuthenticated(client) {
			return reply.MakeErrReply("NOAUTH Authentication required")
		}
	}
	switch cmdName {
	case "select":
		return SelectDB(client, cmdLine[1:], len(s.dbSet))
	case "bgrewriteaof":
		return BGRewriteAof(s, cmdLine[1:])
	case "rewriteaof":
		return RewriteAof(s, cmdLine[1:])
	case "multi":
		return StartMulti(client, cmdLine[1:])
	case "exec":
		return ExecMultiStandalone(s, client, cmdLine[1:])
	case "discard":
		return ExecDiscard(client, cmdLine[1:])
	}

	// normal commands
	dbIndex := client.GetDBIndex()
	selectedDB, errReply := s.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	return selectedDB.Exec(client, cmdLine)
}

// 集群模式执行命令的方式
func (s *Server) execCluster(client redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdName := strings.ToLower(string(cmdLine[0]))

	if (cmdName) == "ping" {
		logger.Debugf("received heart beat from %v", client.Name())
		return reply.MakePongStatusReply()
	}

	if _, ok := client.(*connection.FakeConn); !ok { // fakeConn不做校验
		if cmdName == "auth" {
			return Auth(client, cmdLine[1:])
		}
		if !isAuthenticated(client) {
			return reply.MakeErrReply("NOAUTH Authentication required")
		}
	}

	switch cmdName {
	case "select":
		return SelectDB(client, cmdLine[1:], len(s.dbSet))
	case "bgrewriteaof":
		return BGRewriteAof(s, cmdLine[1:])
	case "rewriteaof":
		return RewriteAof(s, cmdLine[1:])
	}

	// normal commands
	dbIndex := client.GetDBIndex()
	localDB, errReply := s.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}

	return s.cluster.Exec(client, dbIndex, localDB, cmdLine)
}

// server 的通用初始化操作初始化
func initServer() *Server {
	// 初始化数据库
	server := &Server{
		closed: make(chan struct{}, 1),
	}
	if config.Properties.Databases <= 0 {
		config.Properties.Databases = 16 // default is 16
	}
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range server.dbSet {
		singleDB := engine.MakeDB()
		singleDB.SetIndex(i)
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}

	// 读取 AOF 持久化文件
	if config.Properties.AppendOnly {
		if config.Properties.AofFilename == "" { // default is dump.aof
			config.Properties.AofFilename = "dump.aof"
		}

		// 获取初始AOF文件大小
		server.AofFileSize = utils.GetFileSizeByName(config.Properties.AofFilename)

		// 开启 AOF 持久化
		AofPersister, err := aof.NewPersister(server, config.Properties.AofFilename, true, config.Properties.AofFsync, MakeAuxiliaryServer)
		if err != nil {
			logrus.Fatal(err)
		}
		server.bindPersister(AofPersister)

		// 自动 AOF 重写
		if config.Properties.AutoAofRewrite {
			if config.Properties.AutoAofRewritePercentage <= 0 {
				config.Properties.AutoAofRewritePercentage = 100
			}
			if config.Properties.AutoAofRewriteMinSize <= 0 {
				config.Properties.AutoAofRewriteMinSize = 64
			}

			// 开启 AOF 自动重写
			go server.autoAofRewrite()
		}
	}

	return server
}

func (s *Server) selectDB(dbIndex int) (*engine.DB, *reply.StandardErrReply) {
	if dbIndex >= len(s.dbSet) || dbIndex < 0 {
		return nil, reply.MakeErrReply("ERR DB index is out of range")
	}
	return s.dbSet[dbIndex].Load().(*engine.DB), nil
}
func (s *Server) mustSelectDB(dbIndex int) *engine.DB {
	selectedDB, err := s.selectDB(dbIndex)
	if err != nil {
		panic(err)
	}
	return selectedDB
}

func (s *Server) AfterClientClose(c redis.Connection) {
	// TODO
}

func (s *Server) Close() {
	s.closed <- struct{}{}
	if config.Properties.AppendOnly {
		s.AofPersister.Close() // 关闭aof持久化
	}

	if s.cluster != nil {
		s.cluster.Close()
	}
}

func (s *Server) GetDBSize(dbIndex int) (int, int) {
	db := s.mustSelectDB(dbIndex)
	return db.GetDBSize()
}

func (s *Server) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db := s.mustSelectDB(dbIndex)
	db.ForEach(cb)
}

func (s *Server) autoAofRewrite() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			if s.rewriting.Load() {
				// 当前正在重写，跳过这个周期
				continue
			}
			// 开始重写
			s.rewriteWait.Add(1)
			// 检查 aof 文件大小
			aofFileSize := utils.GetFileSizeByName(config.Properties.AofFilename)
			// 检查是否需要重写
			if aofFileSize > s.AofFileSize*config.Properties.AutoAofRewritePercentage/100 && aofFileSize > config.Properties.AutoAofRewriteMinSize*1024*1024 {
				// 开启重写
				go s.AofPersister.Rewrite(&s.rewriteWait, &s.rewriting)
				// 等待结束重写
				s.rewriteWait.Wait()
				// 更新 aof 文件大小
				s.AofFileSize = aofFileSize
			} else {
				s.rewriteWait.Done()
			}

		case <-s.closed:
			ticker.Stop()
			return
		}
	}
}
