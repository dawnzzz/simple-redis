package server

import (
	"context"
	"github.com/dawnzzz/simple-redis/config"
	database2 "github.com/dawnzzz/simple-redis/database"
	"github.com/dawnzzz/simple-redis/interface/database"
	"github.com/dawnzzz/simple-redis/lib/sync/atomic"
	"github.com/dawnzzz/simple-redis/logger"
	"github.com/dawnzzz/simple-redis/redis/connection"
	"github.com/dawnzzz/simple-redis/redis/parser"
	"github.com/dawnzzz/simple-redis/redis/protocol/reply"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn  sync.Map // value记录activeConn的心跳
	db          database.DB
	closing     atomic.Boolean // refusing new client and new request
	closingChan chan struct{}  // 停止心跳检查计时器
}

// MakeHandler creates a Handler instance
func MakeHandler() *Handler {
	var db database.DB
	if config.Properties.Peers != nil && len(config.Properties.Peers) != 0 {
		// 集群模式
		db = database2.NewClusterServer(config.Properties.Peers)
		logger.Infof("cluster mode, peer is %v", config.Properties.Peers)
	} else {
		// 单机模式
		db = database2.NewStandaloneServer()
	}

	h := &Handler{
		db:          db,
		closingChan: make(chan struct{}, 1),
	}

	if config.Properties.Keepalive > 0 {
		go h.checkActiveHeartbeat(config.Properties.Keepalive) // 开启心跳检查
	}

	return h
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, time.Now())

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err
			errReply := reply.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkStringReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}

		h.activeConn.Store(client, time.Now())

		result := h.db.Exec(client, r.Args)
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}
	}
}

func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	h.closingChan <- struct{}{}
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool { // close all active conn
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}

func (h *Handler) checkActiveHeartbeat(keepalive int) {
	ticker := time.NewTicker(time.Second * time.Duration(keepalive/2)) // 每keepalive/2秒钟检查一次客户端的心跳
	for {
		select {
		case <-ticker.C:
			h.activeConn.Range(func(key, value any) bool {
				if time.Now().After(value.(time.Time).Add(time.Second * time.Duration(keepalive))) {
					//  keepalive 秒内没有收到消息，关闭连接
					// 心跳超时，关闭连接
					h.closeClient(key.(*connection.Connection))
				}
				return true
			})
		case <-h.closingChan:
			ticker.Stop()
			return
		}
	}

}
