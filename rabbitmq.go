package rabbitmq

import (
	"github.com/streadway/amqp"
)

type Action func([]byte) error

type RabbitMq interface {
	Initialize() error
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
	conn, err := amqp.Dial(r.Config.Url)

	if err != nil {
		return err
	}

	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		r.Config.QueueName,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	r.Connection = conn
	r.Channel = ch
	r.Queue = &q

	return nil
}

func (r *rabbitMq) Publish(body []byte) error {

	err := r.Channel.Publish(
		r.Config.Exchange,
		r.Queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Body: body,
		},
	)

	return err
}

func (r *rabbitMq) Listen() (<-chan []byte, error) {

	received := make(chan []byte, 0)

	messages, err := r.Channel.Consume(
		r.Queue.Name, // queue
		"",           // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
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
