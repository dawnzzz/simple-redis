package main

import (
	"Dawndis/config"
	"Dawndis/logger"
	"Dawndis/redis/server"
	"Dawndis/tcp"
	"flag"
)

// 配置文件
var configFilename string
var defaultConfigFileName = "config.yaml"

func main() {
	flag.StringVar(&configFilename, "f", defaultConfigFileName, "the config file")
	flag.Parse()

	// 加载配置文件
	config.SetupConfig(configFilename)

	// 加载日志
	logger.SetupLogger()

	//
	if err := tcp.ListenAndServeWithSignal(server.MakeHandler()); err != nil {
		logger.Error(err)
	}
}
