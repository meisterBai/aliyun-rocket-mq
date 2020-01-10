package alirmq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/xiaojiaoyu100/rocketmq-client-go"
	"github.com/xiaojiaoyu100/rocketmq-client-go/primitive"
	"github.com/xiaojiaoyu100/rocketmq-client-go/producer"
)

type rProducer struct {
	// 生产者配置
	options  *ProducerOption
	producer rocketmq.Producer
	activeAt time.Time
}

func NewProducer(cred *RocketMQCredentials, setter ...ProducerSetter) (*rProducer, error) {
	opt := &ProducerOption{
		cred: cred,
	}
	for _, apply := range setter {
		apply(opt)
	}

	nameServers := strings.Split(opt.cred.NameServer, ",")
	if opt.InstanceName == "" {
		opt.InstanceName = strconv.Itoa(os.Getpid())
	}

	innerProducer, err := rocketmq.NewProducer(
		producer.WithNameServer(nameServers),
		producer.WithNamespace(opt.cred.NameSpace),
		producer.WithGroupName(opt.GroupName),
		producer.WithInstanceName(opt.InstanceName),
		producer.WithCredentials(primitive.Credentials{
			AccessKey: opt.cred.AccessKey,
			SecretKey: opt.cred.SecretKey,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("alirmq new producer error = %v\n", err)
	}

	p := &rProducer{
		options:  opt,
		producer: innerProducer,
		activeAt: time.Now(),
	}
	return p, nil
}

func (p *rProducer) Start() error {
	return p.producer.Start()
}

func (p *rProducer) Stop() error {
	return p.producer.Shutdown()
}

type SendResult struct {
	Result *primitive.SendResult
	Err    error
}

func (r *SendResult) MessageId() string {
	return r.Result.MsgID
}

func (r *SendResult) Success() bool {
	if r.Err == nil && r.Result.Status == primitive.SendOK {
		return true
	}
	return false
}

func createMessage(topic string, body []byte, setter ...MessageSetter) *primitive.Message {
	m := newDefaultMessage(topic, body)

	for _, apply := range setter {
		apply(m)
	}

	msg := primitive.NewMessage(m.Topic, m.Body)
	msg.WithTag(m.Tag)
	msg.WithKeys([]string{m.Key})
	for k, v := range m.Properties {
		msg.WithProperty(k, v)
	}
	msg.WithProperty(_RetryOriginTopic, m.Topic)
	return msg
}

func (p *rProducer) Send(topic string, body []byte, setter ...MessageSetter) SendResult {
	msg := createMessage(topic, body, setter...)

	sendResult, err := p.producer.SendSync(context.Background(), msg)
	return SendResult{
		Result: sendResult,
		Err:    err,
	}
}

func (p *rProducer) SendAsync(topic string, body []byte, callback func(ctx context.Context, result *primitive.SendResult, err error), setter ...MessageSetter) error {
	msg := createMessage(topic, body, setter...)
	if callback == nil {
		callback = defaultPublishAsyncCallback
	}

	err := p.producer.SendAsync(context.Background(), callback, msg)
	return err
}

func defaultPublishAsyncCallback(ctx context.Context, result *primitive.SendResult, err error) {
	if err != nil {
		Logger.Errorf("send result = %v, error = %v", result, err)
	} else {
		Logger.Infof("send result = %v", result)
	}
}
