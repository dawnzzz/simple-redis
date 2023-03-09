package main

import (
	"Dawndis/redis/client"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
)

var (
	host string
	port int
)

func main() {
	flag.StringVar(&host, "h", "localhost", "the host of dawndis server")
	flag.IntVar(&port, "p", 6179, "the port of dawndis server")

	addr := fmt.Sprintf("%v:%v", host, port)

	c, err := client.MakeCmdLineClient(addr)
	if err != nil {
		logrus.Fatal("make client err, ", err)
	}

	c.StartCmdLine()
}
