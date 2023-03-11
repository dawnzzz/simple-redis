package redis

// Connection represents a connection with a client
type Connection interface {
	Write([]byte) (int, error)

	Close() error

	SetPassword(string)
	GetPassword() string

	GetDBIndex() int
	SelectDB(int)

	Name() string

	GetMultiStatus() bool
	SetMultiStatus(bool)
	GetEnqueuedCmdLine() [][][]byte
	EnqueueCmdLine([][]byte)
	ClearCmdLines()
	GetSyntaxErrQueue() []Reply
	EnqueueSyntaxErrQueue(Reply)
	GetWatching() map[string]uint32
	CancelWatching()
}
