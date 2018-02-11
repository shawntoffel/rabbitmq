package rabbitmq

import (
	"github.com/streadway/amqp"
)

type Action func([]byte) error

type RabbitMq interface {
	Initialize() error
	InitializeAsPublisher() error
	Publish(body []byte) error
	Listen() (<-chan []byte, error)
	Close()
}

type rabbitMq struct {
	Config     Config
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Queue      *amqp.Queue
}

func NewRabbitMq(config Config) RabbitMq {
	rmq := rabbitMq{}
	rmq.Config = config

	return &rmq
}

func (r *rabbitMq) Initialize() error {
	err := r.initializeChannel()

	if err != nil {
		return err
	}

	err = r.initializeQueue()

	if err != nil {
		return err
	}

	usingCustomExchange, err := r.initializeExchange()

	if err != nil {
		return err
	}

	if usingCustomExchange {
		err = r.Channel.QueueBind(
			r.Config.QueueName,
			"",
			r.Config.Exchange,
			false,
			nil,
		)
	}

	return nil
}

func (r *rabbitMq) InitializeAsPublisher() error {
	err := r.initializeChannel()

	if err != nil {
		return err
	}

	_, err = r.initializeExchange()

	return err
}

func (r *rabbitMq) Publish(body []byte) error {

	err := r.Channel.Publish(
		r.Config.Exchange,
		r.Config.QueueName, // routing key
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			Body: body,
		},
	)

	return err
}

func (r *rabbitMq) Listen() (<-chan []byte, error) {

	received := make(chan []byte, 0)

	messages, err := r.Channel.Consume(
		r.Config.QueueName, // queue
		"",                 // consumer
		true,               // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)

	if err != nil {
		return received, err
	}

	go func() {
		for message := range messages {
			received <- message.Body
		}
	}()

	return received, nil
}

func (r *rabbitMq) Close() {
	defer r.Connection.Close()
	defer r.Channel.Close()
}

func (r *rabbitMq) initializeChannel() error {
	conn, err := amqp.Dial(r.Config.Url)

	if err != nil {
		return err
	}

	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	r.Connection = conn
	r.Channel = ch

	return nil
}

func (r *rabbitMq) initializeQueue() error {
	ch := r.Channel

	q, err := ch.QueueDeclare(
		r.Config.QueueName,
		r.Config.Durable,
		false,
		false,
		false,
		nil,
	)

	r.Queue = &q

	return err
}

func (r *rabbitMq) initializeExchange() (bool, error) {
	if r.Config.Exchange == "" {
		return false, nil
	}

	ch := r.Channel

	err := ch.ExchangeDeclare(
		r.Config.Exchange,
		r.Config.ExchangeType,
		r.Config.Durable,
		false,
		false,
		false,
		nil,
	)

	return true, err
}
