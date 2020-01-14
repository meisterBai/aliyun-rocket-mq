package alirmq

import (
	"fmt"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ons"
)

type OnsClient struct {
	Region     string
	AccessKey  string
	SecretKey  string
	InstanceId string
	Domain     string
	client     *ons.Client
}

type TopicInfo struct {
	Topic       string
	MessageType int
	Remark      string
	CreateTime  int64
}

type GroupInfo struct {
	GroupName string
	Remark    string
}

func NewOnsClient(region, accessKey, secretKey, instanceId string) (*OnsClient, error) {
	if region == "" {
		return nil, fmt.Errorf("region should not be empty")
	}
	if accessKey == "" {
		return nil, fmt.Errorf("accessKey should not be empty")
	}
	if secretKey == "" {
		return nil, fmt.Errorf("secretKey should not be empty")
	}
	if instanceId == "" {
		return nil, fmt.Errorf("instanceId should not be empty")
	}

	client, err := ons.NewClientWithAccessKey(region, accessKey, secretKey)
	if err != nil {
		return nil, err
	}

	return &OnsClient{
		Region:     region,
		AccessKey:  accessKey,
		SecretKey:  secretKey,
		InstanceId: instanceId,
		Domain:     fmt.Sprintf(OnsDomainFormatUrl, region),
		client:     client,
	}, nil
}

func (c *OnsClient) ListTopic() ([]*TopicInfo, error) {
	request := ons.CreateOnsTopicListRequest()
	request.Method = "GET"
	request.Scheme = "http"
	request.Domain = c.Domain
	request.InstanceId = c.InstanceId

	response, err := c.client.OnsTopicList(request)
	if err != nil {
		return nil, err
	}

	res := make([]*TopicInfo, 0)
	publishInfoDo := response.Data.PublishInfoDo
	for _, p := range publishInfoDo {
		res = append(res, &TopicInfo{
			Topic:       p.Topic,
			MessageType: p.MessageType,
			Remark:      p.Remark,
			CreateTime:  int64(p.CreateTime),
		})
	}
	return res, nil
}

func (c *OnsClient) CreateTopic(topic, remark string, messageType int) error {
	request := ons.CreateOnsTopicCreateRequest()
	request.Method = "POST"
	request.Scheme = "http"
	request.Domain = c.Domain
	request.InstanceId = c.InstanceId
	request.MessageType = requests.NewInteger(messageType)
	request.Topic = topic
	request.Remark = remark

	_, err := c.client.OnsTopicCreate(request)
	return err
}

func (c *OnsClient) DeleteTopic(topic string) error {
	request := ons.CreateOnsTopicDeleteRequest()
	request.Method = "POST"
	request.Scheme = "http"
	request.Domain = c.Domain
	request.InstanceId = c.InstanceId
	request.Topic = topic

	_, err := c.client.OnsTopicDelete(request)
	return err
}

func (c *OnsClient) ListGroup() ([]*GroupInfo, error) {
	request := ons.CreateOnsGroupListRequest()
	request.Method = "GET"
	request.Scheme = "http"
	request.Domain = c.Domain
	request.InstanceId = c.InstanceId

	response, err := c.client.OnsGroupList(request)
	if err != nil {
		return nil, err
	}

	res := make([]*GroupInfo, 0)
	subscribeInfoDo := response.Data.SubscribeInfoDo
	for _, g := range subscribeInfoDo {
		res = append(res, &GroupInfo{
			GroupName: g.GroupId,
			Remark:    g.Remark,
		})
	}
	return res, nil
}

// 创建组
func (c *OnsClient) CreateGroup(group, remark string) error {
	request := ons.CreateOnsGroupCreateRequest()
	request.Method = "POST"
	request.Scheme = "http"
	request.Domain = c.Domain
	request.InstanceId = c.InstanceId
	request.GroupId = group
	request.Remark = remark

	_, err := c.client.OnsGroupCreate(request)
	return err
}

// 删除组
func (c *OnsClient) DeleteGroup(group string) error {
	request := ons.CreateOnsGroupDeleteRequest()
	request.Method = "POST"
	request.Scheme = "http"
	request.Domain = c.Domain
	request.InstanceId = c.InstanceId
	request.GroupId = group

	_, err := c.client.OnsGroupDelete(request)
	return err
}

type Sub struct {
	Topic string
	Tag   string
}

// 获取消费者组订阅的topic
func (c *OnsClient) ListConsumerGroupSub(group string) ([]*Sub, error) {
	request := ons.CreateOnsGroupSubDetailRequest()
	request.Method = "GET"
	request.Scheme = "http"
	request.Domain = c.Domain
	request.InstanceId = c.InstanceId
	request.GroupId = group

	response, err := c.client.OnsGroupSubDetail(request)
	if err != nil {
		return nil, err
	}

	subscriptions := response.Data.SubscriptionDataList.SubscriptionDataListItem
	res := make([]*Sub, 0, len(subscriptions))
	for _, s := range subscriptions {
		res = append(res, &Sub{
			Topic: s.Topic,
			Tag:   s.SubString,
		})
	}
	return res, nil
}

// 清空消费者组某个topic的消息
func (c *OnsClient) ClearConsumerTopicMessage(group, topic string) error {
	request := ons.CreateOnsConsumerResetOffsetRequest()
	request.Method = "POST"
	request.Scheme = "http"
	request.Domain = c.Domain
	request.InstanceId = c.InstanceId
	request.GroupId = group
	request.Topic = topic
	request.Type = requests.NewInteger(0)

	_, err := c.client.OnsConsumerResetOffset(request)
	return err
}

// 清空消费者组堆积的所有消息, 耗时可能比较久
func (c *OnsClient) ClearConsumerAllTopicMessage(group string) error {
	topics, err := c.ListConsumerGroupSub(group)
	if err != nil {
		return fmt.Errorf("ListSubscribeTopic error = %w", err)
	}

	for _, row := range topics {
		err = c.ClearConsumerTopicMessage(group, row.Topic)
		if err != nil {
			return fmt.Errorf("group=%s, topic=%s, clear message error=%w", group, row.Topic, err)
		}
	}
	return nil
}

// 回溯消息
func (c *OnsClient) ConsumerResetOffset(group, topic string, resetTimeStamp int64) error {
	request := ons.CreateOnsConsumerResetOffsetRequest()
	request.Method = "POST"
	request.Scheme = "http"
	request.Domain = c.Domain
	request.InstanceId = c.InstanceId
	request.GroupId = group
	request.Topic = topic
	request.Type = requests.Integer(1)
	request.ResetTimestamp = requests.Integer(resetTimeStamp)

	_, err := c.client.OnsConsumerResetOffset(request)
	return err
}
