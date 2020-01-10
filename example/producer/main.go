package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/xiaojiaoyu100/aliyun-rocket-mq"
)

func main() {
	cred := &alirmq.RocketMQCredentials{
		NameServer: "",
		AccessKey:  "",
		SecretKey:  "",
		NameSpace:  "",
		Region:     "",
	}

	producer, err := alirmq.NewProducer(
		cred,
		alirmq.WithSendMessageTimeout(180),
		alirmq.WithProducerInstanceName("MQ_INST_1314867367055240_BbJBcaGg"),
	)
	if err != nil {
		fmt.Printf("new producer error = %v\n", err)
		return
	}

	if err := producer.Start(); err != nil {
		fmt.Printf("start producer error = %v\n", err)
		return
	}

	for i := 0; i < 20; i++ {
		body := []byte(fmt.Sprintf("tag - %v", i))
		r := producer.Send(
			"ApiTopicC",
			body,
			alirmq.WithTag("tag"),
			alirmq.WithKey(fmt.Sprintf("key-%v", i)))
		fmt.Printf("MessageId = %v, error = %v\n", r.MessageId(), r.Err)
	}

	// 使用异步的方式发送消息
	//callback := func(ctx context.Context, result *alirmq.SendResult) {
	//	fmt.Printf("MessageId = %v, error = %v\n", result.MessageId(), result.Err)
	//}
	//for i := 10; i < 20; i++ {
	//	body := []byte(fmt.Sprintf("tag - %v", i))
	//	err := producer.SendAsync(
	//		"TagTestTopic",
	//		body,
	//		callback,
	//		alirmq.WithTag("tag"),
	//		alirmq.WithKey(fmt.Sprintf("key-%v", i)))
	//	fmt.Printf("sendAsync error=%v\n", err)
	//}
	//
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutdown server ...")

	if err := producer.Stop(); err != nil {
		fmt.Printf("error = %v", err)
	}
}
