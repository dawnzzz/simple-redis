package reply

import (
	"bytes"
	"strconv"
	"strings"
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
	if len(r.Args) == 0 {
		return "(empty list or set)"
	}

	var builder strings.Builder
	for i, arg := range r.Args {
		builder.WriteString(strconv.Itoa(i+1) + ") ")
		builder.Write(arg)
		if i != len(r.Args)-1 {
			builder.WriteByte('\n')
		}
	}

	return builder.String()
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
	return "(empty list or set)"
}
