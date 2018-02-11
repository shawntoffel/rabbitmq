package rabbitmq

type Config struct {
	Url           string
	QueueName     string
	Exchange      string
	ExchangeType  string
	Durable       bool
	ConsumerCount int
}
