package log

import (
	"os"

	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger

func Inst() *logrus.Logger {
	return logger
}

func init() {
	logger = logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})
	logger.SetOutput(os.Stderr)
}
