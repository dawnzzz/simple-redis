package config

import (
	"Dawndis/logger"
	"github.com/spf13/viper"
	"os"
)

// ServerProperties 服务器配置
type ServerProperties struct {
	Debug     bool   `mapstructure:"debug"`     // 是否是debug
	Bind      string `mapstructure:"bind"`      // 服务器绑定地址
	Port      int    `mapstructure:"port"`      // 监听端口
	Password  string `mapstructure:"password"`  // 密码
	Databases int    `mapstructure:"databases"` // 数据库数量

	/* AOF持久化配置 */
	AppendOnly               bool   `mapstructure:"append_only"`                 // 是否开启 AOF 持久化
	AofFilename              string `mapstructure:"aof_filename"`                // AOF 持久化文件名
	AofFsync                 int    `mapstructure:"aof_fsync"`                   // AOF 刷盘策略
	AutoAofRewrite           bool   `mapstructure:"auto_aof_rewrite"`            // 是否开启 AOF 自动重写
	AutoAofRewritePercentage int64  `mapstructure:"auto_aof_rewrite_percentage"` // 触发重写所需要的 aof 文件体积百分比，增量大于这个值时才进行重写
	AutoAofRewriteMinSize    int64  `mapstructure:"auto_aov_rewrite_min_size"`   // 表示触发AOF重写的最小文件体积，单位mb
}

var Properties *ServerProperties

func init() {
	// 默认配置
	Properties = &ServerProperties{
		Debug:     os.Getenv("ENV") == "DEBUG",
		Bind:      "127.0.0.1",
		Port:      6179,
		Password:  "",
		Databases: 16,

		AppendOnly:               true,
		AofFilename:              "dump.aof",
		AofFsync:                 0,
		AutoAofRewrite:           false,
		AutoAofRewritePercentage: 100,
		AutoAofRewriteMinSize:    64,
	}
}

// SetupConfig 读配置文件，加载配置文件
func SetupConfig(configFilename string) {
	if !fileExists(configFilename) {
		// 文件不存在，直接用默认配置
		return
	}

	viper.SetConfigFile(configFilename)
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		logger.Fatalf("setup config err, %v", err)
	}

	if err := viper.Unmarshal(Properties); err != nil {
		logger.Fatalf("setup config unmarshal err, %v", err)
	}

	if Properties.Debug == true { // debug 没有密码
		Properties.Password = ""
	}
}

func setDefault() {
	// todo 设置默认值
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
