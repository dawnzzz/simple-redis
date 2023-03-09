package reply

import "strconv"

// IntReply stores an int64 number
type IntReply struct {
	Code int64
}

// MakeIntReply creates int protocol
func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ToBytes marshal redis.Reply
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

func (r *IntReply) DataString() string {
	return "(integer) " + strconv.FormatInt(r.Code, 10)
}
