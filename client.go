package main

import (
	"flag"
	"fmt"
	"github.com/dawnzzz/simple-redis/redis/client"
	"github.com/sirupsen/logrus"
)

var (
	host      string
	port      int
	keepalive int
)

func main() {
	flag.StringVar(&host, "h", "localhost", "the host of dawndis server")
	flag.IntVar(&port, "p", 6179, "the port of dawndis server")
	flag.IntVar(&keepalive, "k", 0, "the interval of server check client alive")
	flag.Parse()

	addr := fmt.Sprintf("%v:%v", host, port)

	c, err := client.MakeCmdLineClient(addr, keepalive)
	if err != nil {
		logrus.Fatal("make client err, ", err)
	}

	c.StartCmdLine()
}
