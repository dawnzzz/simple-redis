package reply

const (
	CRLF = "\r\n"
)

var (
	// 空字符串
	nullBulkBytes = []byte("$-1\r\n")
	// 空列表
	emptyMultiBulkBytes = []byte("*0\r\n")
	// ok 状态
	okStatusDataString = "OK"
	okStatusBytes      = []byte("+OK\r\n")
)
