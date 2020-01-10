package alirmq

type Message struct {
	Topic      string
	Tag        string
	Body       []byte
	Key        string
	Properties map[string]string
}

func newDefaultMessage(topic string, body []byte) *Message {
	return &Message{
		Topic:      topic,
		Body:       body,
		Tag:        _TagAll,
		Properties: make(map[string]string),
	}
}

type MessageSetter func(message *Message)

func WithTag(tag string) MessageSetter {
	return func(m *Message) {
		m.Tag = tag
	}
}

func WithKey(key string) MessageSetter {
	return func(m *Message) {
		m.Key = key
	}
}

func WithProperty(key, value string) MessageSetter {
	return func(m *Message) {
		m.Properties[key] = value
	}
}

type M struct {
	Topic string
	Tag   string
	Key   string
	Body  []byte
}

