package engine

import (
	"Dawndis/lib/timewheel"
	"Dawndis/logger"
	"time"
)

/* ---- TTL Functions ---- */

func (db *DB) genExpireTaskKey(key string) string {
	return "expire:" + key
}

// Expire set expire time of key
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	expireTaskKey := db.genExpireTaskKey(key)
	timewheel.At(expireTime, expireTaskKey, func() {
		// 先对key加锁，防止ttl更改
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key) // 获取过期时间
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired { // 过期则移除
			db.Remove(key)
		}
	})
}

// ExpireAfter set key expiring after xx time
func (db *DB) ExpireAfter(key string, delay time.Duration) {
	expireTime := time.Now().Add(delay)
	db.ttlMap.Put(key, expireTime)
	expireTaskKey := db.genExpireTaskKey(key)
	timewheel.Delay(delay, expireTaskKey, func() {
		// 先对key加锁，防止ttl更改
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key) // 获取过期时间
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired { // 过期则移除
			db.Remove(key)
		}
	})
}

// Persist cancel ttl of key
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	expireTaskKey := db.genExpireTaskKey(key)
	timewheel.Cancel(expireTaskKey)
}

// IsExpired key是否过期
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}

	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired { // 过期就移除key
		db.Remove(key)
	}

	return expired
}
