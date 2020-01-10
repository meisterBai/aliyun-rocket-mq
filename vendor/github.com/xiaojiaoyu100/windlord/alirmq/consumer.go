package alirmq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/xiaojiaoyu100/rocketmq-client-go"
	"github.com/xiaojiaoyu100/rocketmq-client-go/consumer"
	"github.com/xiaojiaoyu100/rocketmq-client-go/primitive"
)

type Handler func(*M) error

type ErrorCallback func(err error, msg *primitive.MessageExt)

type Consumer struct {
	consumer        rocketmq.PushConsumer
	option          *ConsumerOption
	handleFuncMap   map[string]Handler
	signal          chan os.Signal
	callback        ErrorCallback
	subscribeTopics []string
	mutex           sync.Mutex
}

func NewConsumer(cred *RocketMQCredentials, setter ...ConsumerSetter) (*Consumer, error) {
	opts := &ConsumerOption{
		cred:           cred,
		Broadcasting:   false,
		ConsumeOrderly: false,
	}

	for _, apply := range setter {
		apply(opts)
	}

	if opts.MaxReconsumeTimes == 0 {
		opts.MaxReconsumeTimes = 16
	}
	if opts.MaxTopicCount == 0 {
		opts.MaxTopicCount = 16
	}
	if opts.InstanceName == "" {
		opts.InstanceName = strconv.Itoa(os.Getpid())
	}

	consumerModel := consumer.Clustering
	if opts.Broadcasting {
		consumerModel = consumer.BroadCasting
	}
	consumeOrder := opts.ConsumeOrderly

	nameServers := strings.Split(opts.cred.NameServer, ",")
	c, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer(nameServers),
		consumer.WithNamespace(opts.cred.NameSpace),
		consumer.WithGroupName(opts.cred.GroupName),
		consumer.WithInstance(opts.InstanceName),
		consumer.WithCredentials(primitive.Credentials{
			AccessKey: opts.cred.AccessKey,
			SecretKey: opts.cred.SecretKey,
		}),
		consumer.WithMaxTopicCount(opts.MaxTopicCount+1),
		consumer.WithMaxReconsumeTimes(opts.MaxReconsumeTimes),
		consumer.WithConsumerModel(consumerModel),
		consumer.WithConsumerOrder(consumeOrder),
	)
	if err != nil {
		return nil, fmt.Errorf("new rocketMQ consumer error = %v", err)
	}

	inst := &Consumer{
		consumer:        c,
		option:          opts,
		handleFuncMap:   make(map[string]Handler, 0),
		signal:          make(chan os.Signal, 1),
		subscribeTopics: make([]string, 0, opts.MaxTopicCount),
	}
	return inst, nil
}

func (c *Consumer) SetErrorCallback(fn func(err error, msg *primitive.MessageExt)) {
	c.callback = fn
}

func (c *Consumer) Subscribe(topic, tag string, handler Handler) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.subscribeTopics) >= c.option.MaxTopicCount {
		return fmt.Errorf("consumer subscribe topic large than config")
	}

	if tag == "" {
		tag = _TagAll
	}
	tags := strings.Split(tag, "||")
	for _, t := range tags {
		c.handleFuncMap[c.generateKey(topic, t)] = handler
	}

	err := c.consumer.Subscribe(
		topic,
		consumer.MessageSelector{Type: consumer.TAG, Expression: tag},
		c.processMessage)

	return fmt.Errorf("subscribe topic error = %v", err)
}

func (c *Consumer) generateKey(topic, tag string) string {
	return fmt.Sprintf("%s#%s", topic, tag)
}

func (c *Consumer) processMessage(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	for _, msg := range messages {
		topic, tag := msg.Topic, msg.GetTags()
		fmt.Printf("msg = %v\n", msg)

		if strings.HasPrefix(topic, _RetryPrefix) {
			topic = msg.GetProperty(_RetryOriginTopic)
			if topic == "" {
				Logger.Errorf("get message - %v without topic - %v", msg, topic)
				continue
			}
		}
		if strings.HasPrefix(topic, c.option.cred.NameSpace) {
			topic = strings.ReplaceAll(topic, c.option.cred.NameSpace+"%", "")
		}
		key := c.generateKey(topic, tag)

		m := &M{
			Topic: topic,
			Tag:   tag,
			Key:   msg.GetKeys(),
			Body:  msg.Body,
		}
		if fn, ok := c.handleFuncMap[key]; ok {
			err := fn(m)
			if err != nil {
				return consumer.ConsumeRetryLater, nil
			}
		}
		if fn, ok := c.handleFuncMap[c.generateKey(topic, "*")]; ok {
			err := fn(m)
			if err != nil {
				return consumer.ConsumeRetryLater, nil
			}
		}
	}
	return consumer.ConsumeSuccess, nil
}

func (c *Consumer) Start() error {
	return c.consumer.Start()
}

func (c *Consumer) Stop() error {
	return c.consumer.Shutdown()
}

func DefaultErrorCallback(err error, msg *primitive.MessageExt) {
	Logger.WithFields(logrus.Fields{
		"topic":          msg.Topic,
		"tag":            msg.GetTags(),
		"messageid":      msg.MsgId,
		"reconsumetimes": msg.ReconsumeTimes,
	}).WithError(err).Warnf("handle rocket mq msg error")
}
