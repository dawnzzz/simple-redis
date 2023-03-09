package parser

import (
	"Dawndis/interface/redis"
	"Dawndis/logger"
	"Dawndis/redis/protocol/reply"
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
)

type Payload struct {
	Data redis.Reply
	Err  error
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parser(reader, ch)
	return ch
}

func parser(rawReader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()

	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			// 检查格式，必须以 \r\n 结尾
			continue
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'}) // 去除结尾的 \r\n
		switch line[0] {
		case '+':
			content := string(line[1:])
			ch <- &Payload{
				Data: reply.MakeStatusReply(content),
			}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{
				Data: reply.MakeIntReply(value),
			}
		case '-':
			content := string(line[1:])
			ch <- &Payload{
				Data: reply.MakeErrReply(content),
			}
		case '$':
			err = parseBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		case '*':
			err = parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		}
	}
}

func parseBulkString(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 首先从 header 中解析出字符串长度
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		protocolError(ch, "illegal bulk string header: "+string(header))
		return nil
	} else if strLen == -1 {
		// 长度为 -1 代表空字符串
		ch <- &Payload{
			Data: reply.MakeNullBulkStringReply(),
		}
		return nil
	}
	// 根据长度读取 body
	body := make([]byte, strLen+2) // 2 为 CRLF 的长度
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}

	ch <- &Payload{
		Data: reply.MakeBulkStringReply(body[:len(body)-2]),
	}

	return nil
}

func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// 解析出数组长度
	nStrs, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || nStrs < 0 {
		protocolError(ch, "illegal array header "+string(header[1:]))
		return nil
	} else if nStrs == 0 {
		ch <- &Payload{
			Data: reply.MakeEmptyMultiBulkStringReply(),
		}
		return nil
	}

	lines := make([][]byte, 0, nStrs)
	for i := int64(0); i < nStrs; i++ {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			return err
		}

		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "illegal bulk string header "+string(line))
			break
		}
		strLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || strLen < -1 {
			protocolError(ch, "illegal bulk string length "+string(line))
			break
		} else if strLen == -1 {
			lines = append(lines, []byte{})
		} else {
			body := make([]byte, strLen+2)
			_, err := io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			lines = append(lines, body[:len(body)-2])
		}
	}

	ch <- &Payload{
		Data: reply.MakeMultiBulkStringReply(lines),
	}

	return nil
}

func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Err: err}
}
