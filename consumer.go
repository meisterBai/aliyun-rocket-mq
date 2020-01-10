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

	"github.com/xiaojiaoyu100/aliyun-rocket-mq/log"
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
	topicTagsMap    map[string][]string
	mutex           sync.Mutex
	onsClient       *OnsClient
}

func NewConsumer(cred *RocketMQCredentials, setter ...ConsumerSetter) (*Consumer, error) {
	os.Setenv("ROCKETMQ_GO_LOG_LEVEL", "warn")
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

	if !strings.HasPrefix(opts.cred.GroupName, "GID_") {
		opts.cred.GroupName = "GID_" + opts.cred.GroupName
	}

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

	var onsClient *OnsClient
	if opts.cred.Region != "" {
		onsClient, err = NewOnsClient(
			opts.cred.Region, opts.cred.AccessKey, opts.cred.SecretKey, opts.cred.NameSpace)
		if err != nil {
			return nil, fmt.Errorf("new onsClient error = %w", err)
		}
	}

	inst := &Consumer{
		consumer:        c,
		option:          opts,
		handleFuncMap:   make(map[string]Handler, 0),
		signal:          make(chan os.Signal, 1),
		subscribeTopics: make([]string, 0, opts.MaxTopicCount),
		topicTagsMap:    make(map[string][]string, 0),
		onsClient:       onsClient,
	}
	return inst, nil
}

func (c *Consumer) autoCreateConsumerGroup() bool {
	return c.onsClient != nil
}

func (c *Consumer) createConsumerGroupNotExists(group string) error {
	groups, err := c.onsClient.ListGroup()
	if err != nil {
		return fmt.Errorf("list consumer group error = %w", err)
	}

	groupMap := make(map[string]struct{})
	for _, g := range groups {
		groupMap[g.GroupName] = struct{}{}
	}

	// 消费者组不存在，自动创建
	if _, ok := groupMap[group]; !ok {
		err = c.onsClient.CreateGroup(group, group)
		if err != nil {
			err = fmt.Errorf("create consumer group error = %w", err)
		}
	}
	return err
}

func (c *Consumer) SetErrorCallback(fn func(err error, msg *primitive.MessageExt)) {
	c.callback = fn
}

func (c *Consumer) uniqueTags(tags []string) []string {
	m := make(map[string]struct{})
	for _, t := range tags {
		if t == _TagAll {
			continue
		}
		m[t] = struct{}{}
	}

	res := make([]string, 0, len(m))
	for k := range m {
		res = append(res, k)
	}
	return res
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

	// 支持在同一个topic上重复订阅
	if subscribeTags, ok := c.topicTagsMap[topic]; ok {
		subscribeTags = append(subscribeTags, tags...)
		c.topicTagsMap[topic] = c.uniqueTags(subscribeTags)
	} else {
		c.topicTagsMap[topic] = c.uniqueTags(tags)
	}
	subscribeTags := c.topicTagsMap[topic]
	if len(subscribeTags) == 0 {
		tag = _TagAll
	} else {
		tag = strings.Join(subscribeTags, "||")
	}

	err := c.consumer.Subscribe(
		topic,
		consumer.MessageSelector{Type: consumer.TAG, Expression: tag},
		c.processMessage)

	if err != nil {
		return fmt.Errorf("subscribe topic error = %v", err)
	}
	return nil
}

func (c *Consumer) generateKey(topic, tag string) string {
	return fmt.Sprintf("%s#%s", topic, tag)
}

func (c *Consumer) processMessage(ctx context.Context, messages ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	for _, msg := range messages {
		topic, tag := msg.Topic, msg.GetTags()

		if strings.HasPrefix(topic, _RetryPrefix) {
			topic = msg.GetProperty(_RetryOriginTopic)
			if topic == "" {
				log.Inst().Errorf("get message - %v without topic - %v", msg, topic)
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
			MsgId: msg.MsgId,
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
	if c.autoCreateConsumerGroup() {
		err := c.createConsumerGroupNotExists(c.option.cred.GroupName)
		if err != nil {
			return err
		}
	}
	return c.consumer.Start()
}

func (c *Consumer) Stop() error {
	return c.consumer.Shutdown()
}

func DefaultErrorCallback(err error, msg *primitive.MessageExt) {
	log.Inst().WithFields(logrus.Fields{
		"topic":          msg.Topic,
		"tag":            msg.GetTags(),
		"messageid":      msg.MsgId,
		"reconsumetimes": msg.ReconsumeTimes,
	}).WithError(err).Warnf("handle rocket mq msg error")
}
