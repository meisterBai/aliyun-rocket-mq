package alirmq

// rocket mq connection config
type RocketMQCredentials struct {
	NameServer string
	AccessKey  string
	SecretKey  string
	NameSpace  string
	GroupName  string
	Region     string
}

type ProducerOption struct {
	cred               *RocketMQCredentials
	GroupName          string
	SendMessageTimeout int
	InstanceName       string
	UnitName           string
	Region             string
	Retry              int
}

type ProducerSetter func(option *ProducerOption)

func WithProducerGroupName(name string) ProducerSetter {
	return func(opt *ProducerOption) {
		opt.GroupName = name
	}
}

func WithSendMessageTimeout(timeout int) ProducerSetter {
	return func(opt *ProducerOption) {
		opt.SendMessageTimeout = timeout
	}
}

func WithProducerInstanceName(name string) ProducerSetter {
	return func(opt *ProducerOption) {
		opt.InstanceName = name
	}
}

func WithRetry(retry int) ProducerSetter {
	return func(opt *ProducerOption) {
		opt.Retry = retry
	}
}

type ConsumerOption struct {
	cred              *RocketMQCredentials
	InstanceName      string
	Broadcasting      bool  // 是否开启广播
	ConsumeOrderly    bool  // 是否顺序消费
	MaxReconsumeTimes int32 //  失败消息重试次数
	MaxTopicCount     int   // 一个consumer 消费多少topic
	ConsumeTimeout    int   // 消息消费耗时
}

type ConsumerSetter func(option *ConsumerOption)

func WithInstanceName(name string) ConsumerSetter {
	return func(opt *ConsumerOption) {
		opt.InstanceName = name
	}
}

func WithBroadcasting(broadcast bool) ConsumerSetter {
	return func(opt *ConsumerOption) {
		opt.Broadcasting = broadcast
	}
}

func WithConsumeOrderly(orderly bool) ConsumerSetter {
	return func(opt *ConsumerOption) {
		opt.ConsumeOrderly = orderly
	}
}

func WithReconsumerTime(n int32) ConsumerSetter {
	return func(opt *ConsumerOption) {
		opt.MaxReconsumeTimes = n
	}
}

func WithMaxTopicCount(count int) ConsumerSetter {
	return func(opts *ConsumerOption) {
		opts.MaxTopicCount = count
	}
}

func WithConsumeTimeout(n int) ConsumerSetter {
	return func(opts *ConsumerOption) {
		opts.ConsumeTimeout = n
	}
}
