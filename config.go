package rabbitmq

type Config struct {
	Url           string
	QueueName     string
	Exchange      string
	ExchangeType  string
	ExchangeOnly  bool
	Durable       bool
	ConsumerCount int
}
