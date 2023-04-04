package main

import (
	"flag"
	"fmt"
	"github.com/dawnzzz/simple-redis/config"
	"github.com/dawnzzz/simple-redis/logger"
	"github.com/dawnzzz/simple-redis/redis/server"
	"github.com/dawnzzz/simple-redis/tcp"
)

// 配置文件
var configFilename string
var defaultConfigFileName = "config.yaml"

const banner = `
 ________   ___   _____ ______    ________   ___        _______    ________   _______    ________   ___   ________      
|\   ____\ |\  \ |\   _ \  _   \ |\   __  \ |\  \      |\  ___ \  |\   __  \ |\  ___ \  |\   ___ \ |\  \ |\   ____\     
\ \  \___|_\ \  \\ \  \\\__\ \  \\ \  \|\  \\ \  \     \ \   __/| \ \  \|\  \\ \   __/| \ \  \_|\ \\ \  \\ \  \___|_    
 \ \_____  \\ \  \\ \  \\|__| \  \\ \   ____\\ \  \     \ \  \_|/__\ \   _  _\\ \  \_|/__\ \  \ \\ \\ \  \\ \_____  \   
  \|____|\  \\ \  \\ \  \    \ \  \\ \  \___| \ \  \____ \ \  \_|\ \\ \  \\  \|\ \  \_|\ \\ \  \_\\ \\ \  \\|____|\  \  
    ____\_\  \\ \__\\ \__\    \ \__\\ \__\     \ \_______\\ \_______\\ \__\\ _\ \ \_______\\ \_______\\ \__\ ____\_\  \ 
   |\_________\\|__| \|__|     \|__| \|__|      \|_______| \|_______| \|__|\|__| \|_______| \|_______| \|__||\_________\
   \|_________|                                                                                             \|_________|

powered by https://github.com/dawnzzz/simple-redis

`

func main() {
	flag.StringVar(&configFilename, "f", defaultConfigFileName, "the config file")
	flag.Parse()

	fmt.Print(banner)

	// 加载配置文件
	config.SetupConfig(configFilename)

	// 加载日志
	logger.SetupLogger()

	//
	if err := tcp.ListenAndServeWithSignal(server.MakeHandler()); err != nil {
		logger.Error(err)
	}
}
