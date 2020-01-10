package alirmq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xiaojiaoyu100/rocketmq-client-go"
	"github.com/xiaojiaoyu100/rocketmq-client-go/primitive"
	"github.com/xiaojiaoyu100/rocketmq-client-go/producer"

	"github.com/xiaojiaoyu100/aliyun-rocket-mq/log"
)

type Producer struct {
	// 生产者配置
	options   *ProducerOption
	producer  rocketmq.Producer
	activeAt  time.Time
	topicMap  sync.Map
	onsClient *OnsClient
}

func NewProducer(cred *RocketMQCredentials, setter ...ProducerSetter) (*Producer, error) {
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
		producer.WithRetry(opt.Retry),
		producer.WithCredentials(primitive.Credentials{
			AccessKey: opt.cred.AccessKey,
			SecretKey: opt.cred.SecretKey,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("alirmq new producer error = %v\n", err)
	}

	var onsClient *OnsClient
	if opt.cred.Region != "" {
		onsClient, err = NewOnsClient(opt.cred.Region, opt.cred.AccessKey, opt.cred.SecretKey, opt.cred.NameSpace)
		if err != nil {
			return nil, fmt.Errorf("alirmq new onsClient error = %v\n", err)
		}
	}

	p := &Producer{
		options:  opt,
		producer: innerProducer,
		activeAt: time.Now(),
		onsClient: onsClient,
	}
	return p, nil
}

func (p *Producer) syncTopic() error {
	// 没有开启自动创建topic功能
	if p.onsClient == nil {
		return nil
	}

	topics, err := p.onsClient.ListTopic()
	if err != nil {
		return err
	}

	p.topicMap.Range(func(key interface{}, value interface{}) bool {
		p.topicMap.Delete(key)
		return true
	})

	for _, topic := range topics {
		p.topicMap.Store(topic.Topic, struct {}{})
	}
	return nil
}

func (p *Producer) createTopicNotExist(topic string) error {
	if _, ok := p.topicMap.Load(topic); ok {
		return nil
	}

	err := p.onsClient.CreateTopic(topic, topic, NormalMessageType)
	if err != nil {
		_ = p.syncTopic()
		return err
	}

	p.topicMap.Store(topic, struct {}{})
	time.Sleep(time.Second)
	return nil
}

func (p *Producer) autoCreateTopic() bool {
	return  p.onsClient != nil
}

func (p *Producer) Start() error {
	err := p.syncTopic()
	if err != nil {
		return fmt.Errorf("region setted, but sync topic error = %w", err)
	}
	return p.producer.Start()
}

func (p *Producer) Stop() error {
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

func CreateMessage(topic string, body []byte, setter ...MessageSetter) *primitive.Message {
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

func (p *Producer) Send(topic string, body []byte, setter ...MessageSetter) SendResult {
	msg := CreateMessage(topic, body, setter...)

	if p.autoCreateTopic() {
		if err := p.createTopicNotExist(topic); err != nil {
			log.Inst().Errorf("create topic error=%v\n", topic)
		}
	}
	sendResult, err := p.producer.SendSync(context.Background(), msg)

	// 如果是topic 不存在则删除本地缓存
	if isTopicNotExistError(err) {
		p.topicMap.Delete(topic)
	}
	return SendResult{
		Result: sendResult,
		Err:    err,
	}
}

func (p *Producer) SendAsync(topic string, body []byte, callback func(ctx context.Context, result *SendResult), setter ...MessageSetter) error {
	msg := CreateMessage(topic, body, setter...)

	var callbackFn func(ctx context.Context, result *primitive.SendResult, err error)
	if callback == nil {
		callbackFn = defaultSendAsyncCallback
	} else {
		callbackFn = func(ctx context.Context, result *primitive.SendResult, err error) {
			r := &SendResult{
				Result: result,
				Err:    err,
			}
			callback(ctx, r)
		}
	}

	if p.autoCreateTopic() {
		_ = p.createTopicNotExist(topic)
	}
	err := p.producer.SendAsync(context.Background(), callbackFn, msg)
	return err
}

func (p *Producer) RawProducer() *rocketmq.Producer {
	return &p.producer
}

func defaultSendAsyncCallback(ctx context.Context, result *primitive.SendResult, err error) {
	if err != nil {
		log.Inst().Errorf("send result = %v, error = %v", result, err)
	} else {
		log.Inst().Infof("send result = %v", result)
	}
}

func isTopicNotExistError(err error) bool {
	text := fmt.Sprintf("%s", err)
	return strings.Contains(text, "topic") && strings.Contains(text, "not exist")
}