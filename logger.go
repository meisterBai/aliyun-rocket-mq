package alirmq

import (
	"os"

	"github.com/sirupsen/logrus"
)

var Logger = logrus.New()

func init() {
	Logger.SetNoLock()
	Logger.Out = os.Stdout
}
