package database

import (
	"Dawndis/config"
	"Dawndis/database/engine"
	"Dawndis/database/rdb/aof"
	"Dawndis/interface/database"
	"sync/atomic"
)

// MakeAuxiliaryServer create a Server only with basic capabilities for aof rewrite and other usages
func MakeAuxiliaryServer() database.DBEngine {
	mdb := &Server{}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		db := engine.MakeBasicDB()
		holder := &atomic.Value{}
		holder.Store(db)
		mdb.dbSet[i] = holder
	}
	return mdb
}

func (s *Server) bindPersister(aofPersister *aof.Persister) {
	s.AofPersister = aofPersister
	for _, db := range s.dbSet {
		singleDB := db.Load().(*engine.DB)
		singleDB.SetAddAof(func(line engine.CmdLine) {
			if config.Properties.AppendOnly {
				// TODO 处理TTL命令
				aofPersister.SaveCmdLine(singleDB.GetIndex(), line)
			}
		})
	}
}
