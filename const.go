package alirmq

const (
	_TagAll           = "*"
	_RetryOriginTopic = "_retry_origin_topic"
	_RetryPrefix      = "%RETRY%"
)

const (
	OnsDomainFormatUrl        = "ons.%s.aliyuncs.com"
	NormalMessageType         = 0
	PartitionOrderMessageType = 1
	GlobalOrderMessageType    = 2
	TransactionMessageType    = 4
	DelayMessageType          = 5
)
