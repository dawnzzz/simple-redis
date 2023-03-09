package server

import (
	database2 "Dawndis/database"
	"Dawndis/interface/database"
	"Dawndis/lib/sync/atomic"
	"Dawndis/logger"
	"Dawndis/redis/connection"
	"Dawndis/redis/parser"
	"Dawndis/redis/protocol/reply"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
	maxHeartbeatInterval = time.Second * 60
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
	db = database2.NewStandaloneServer()

	h := &Handler{
		db:          db,
		closingChan: make(chan struct{}, 1),
	}
	go h.checkActiveHeartbeat() // 开启心跳检查

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

func (h *Handler) checkActiveHeartbeat() {
	ticker := time.NewTicker(time.Second * 5) // 每五秒钟检查一次客户端的心跳
	for {
		select {
		case <-ticker.C:
			h.activeConn.Range(func(key, value any) bool {
				if time.Now().After(value.(time.Time).Add(maxHeartbeatInterval)) {
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
