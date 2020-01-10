package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/xiaojiaoyu100/aliyun-rocket-mq"
)

type Subscription struct {
	Topic string
	Tag   string
	Fn    func(m *alirmq.M) error
}

var subscriptions = []Subscription{
	{
		Topic: "StudentTopic",
		Tag:   "CommonUserStudent||Attend",
		Fn: func(m *alirmq.M) error {
			fmt.Printf("CommonUserStudent||Attend = %v\n", string(m.Body))
			return nil
		},
	},
	{
		Topic: "StudentTopic",
		Tag:   "Dropout",
		Fn: func(m *alirmq.M) error {
			fmt.Printf("Dropout = %v\n", string(m.Body))
			return nil
		},
	},
	{
		Topic: "StudentTopic",
		Tag:   "Paid",
		Fn: func(m *alirmq.M) error {
			fmt.Printf("Paid = %v\n", string(m.Body))
			return nil
		},
	},
	{
		Topic: "TestPay",
		Tag:   "",
		Fn: func(m *alirmq.M) error {
			fmt.Printf("TestPay, start = %v\n", string(m.Body))
			return nil
		},
	},
}

func main() {
	customFormatter := new(logrus.TextFormatter)
	customFormatter.FullTimestamp = true                        // 显示完整时间
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000" // 时间格式

	logrus.SetFormatter(customFormatter)
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.DebugLevel)

	cred := &alirmq.RocketMQCredentials{
		NameServer: "",
		AccessKey:  "",
		SecretKey:  "",
		NameSpace:  "",
		GroupName:  "",
		Region:     "",
	}

	consumer, err := alirmq.NewConsumer(
		cred,
		alirmq.WithMaxTopicCount(12),
		alirmq.WithReconsumerTime(3),
	)
	if err != nil {
		fmt.Printf("new consumer error = %v", err)
		return
	}

	for _, s := range subscriptions {
		if err := consumer.Subscribe(s.Topic, s.Tag, s.Fn); err != nil {
			fmt.Printf("subscribe topic=%v, error=%v\n", s.Topic, err)
		}
	}

	err = consumer.Start()
	if err != nil {
		fmt.Printf("start error = %v\n", err)
	}

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutdown server ...")

	if err := consumer.Stop(); err != nil {
		log.Fatalf("server shutdown error = %v\n", err)
	}
}
