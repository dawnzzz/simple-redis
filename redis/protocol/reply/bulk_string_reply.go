package reply

import "strconv"

type BulkStringReply struct {
	Arg []byte
}

func MakeBulkStringReply(Arg []byte) *BulkStringReply {
	return &BulkStringReply{
		Arg: Arg,
	}
}

func (r *BulkStringReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	head := "$" + strconv.Itoa(len(r.Arg)) + CRLF
	body := string(r.Arg) + CRLF
	return []byte(head + body)
}

func (r *BulkStringReply) DataString() string {
	return string(r.Arg)
}

type NullBulkStringReply struct {
}

func MakeNullBulkStringReply() *NullBulkStringReply {
	return &NullBulkStringReply{}
}

func (r *NullBulkStringReply) ToBytes() []byte {
	return nullBulkBytes
}

func (r *NullBulkStringReply) DataString() string {
	return "(nil)"
}
