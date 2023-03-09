package redis

// Reply 表示 redis 序列化协议中的一条消息
type Reply interface {
	ToBytes() []byte
	DataString() string
}
