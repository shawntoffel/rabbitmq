package rabbitmq

type Config struct {
	Url           string
	QueueName     string
	Exchange      string
	ConsumerCount int
}
