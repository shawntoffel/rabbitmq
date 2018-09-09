package rabbitmq

const (
	DefaultUrl = "amqp://localhost:5672"
)

type Config struct {
	Url           string
	QueueName     string
	Exchange      string
	ExchangeType  string
	Durable       bool
	ConsumerCount int
}
