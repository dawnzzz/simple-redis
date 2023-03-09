package logger

import (
	"github.com/sirupsen/logrus"
	"os"
)

var log = logrus.New()

func SetupLogger() {
	if os.Getenv("ENV") == "DEBUG" {
		log.SetLevel(logrus.DebugLevel)
	}
}

func Debug(args ...interface{}) {
	log.Debug(args)
}

func Debugf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

func Info(args ...interface{}) {
	log.Info(args...)
}

func Infoln(args ...interface{}) {
	log.Infoln(args...)
}

func Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

func Warn(args ...interface{}) {
	log.Warn(args)
}

func Error(args ...interface{}) {
	log.Error(args)
}

func Errorf(format string, args ...interface{}) {
	log.Errorf(format, args)
}

func Fatalf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}
