package reply

import (
	"bytes"
	"strconv"
)

type MultiBulkStringReply struct {
	Args [][]byte
}

func MakeMultiBulkStringReply(args [][]byte) *MultiBulkStringReply {
	return &MultiBulkStringReply{
		Args: args,
	}
}

func (r *MultiBulkStringReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		bulk := MakeBulkStringReply(arg)
		buf.WriteString(string(bulk.ToBytes()))
	}

	return buf.Bytes()
}

func (r *MultiBulkStringReply) DataString() string {
	args := make([]byte, 0, 10)
	for i, arg := range r.Args {
		args = append(args, arg...)
		if i != len(r.Args)-1 {
			args = append(args, '\n')
		}
	}

	return string(args)
}

type EmptyMultiBulkStringReply struct {
}

func MakeEmptyMultiBulkStringReply() *EmptyMultiBulkStringReply {
	return &EmptyMultiBulkStringReply{}
}

func (r *EmptyMultiBulkStringReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func (r *EmptyMultiBulkStringReply) DataString() string {
	return ""
}
