package reply

// StatusReply 记录 RESP 中的状态信息（以+开头的信息）
type StatusReply struct {
	Status string
}

// MakeStatusReply 创建一个 StatusReply
func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

func (r *StatusReply) DataString() string {
	return r.Status
}

/* OK REPLY */

// OkReply is +OK
type OkReply struct{}

var (
	okBytes    = okStatusBytes
	theOkReply = OkReply{}
)

func MakeOkReply() *OkReply {
	return &theOkReply
}

// ToBytes marshal redis.Reply
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

func (r *OkReply) DataString() string {
	return okStatusDataString
}

/* PONG REPLY */

type PongReply struct {
}

var (
	pongBytes      = []byte("+PONG\r\n")
	pongDataString = "PONG"
)

func MakePongStatusReply() *PongReply {
	return &PongReply{}
}

func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

func (r *PongReply) DataString() string {
	return pongDataString
}
