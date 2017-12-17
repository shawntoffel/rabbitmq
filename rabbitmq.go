package rabbitmq

import (
	"github.com/streadway/amqp"
)

type RabbitMq interface {
	Initialize() error
	Publish(body []byte, contentType string) error
	Listen(action Action) error
	Close()
}

type rabbitMq struct {
	Config     Config
	Connection *amqp.Connection
	Channel    *amqp.Channel
	Queue      *amqp.Queue
}

type Action func([]byte) error

func NewRabbitMq(config Config) RabbitMq {
	return &rabbitMq{Config: config}
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

func (r *rabbitMq) Publish(body []byte, contentType string) error {

	err := r.Channel.Publish(
		r.Config.Exchange,
		r.Queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        body,
		},
	)

	return err
}

func (r *rabbitMq) Listen(action Action) error {
	msgs, err := r.Channel.Consume(
		r.Queue.Name, // queue
		"",           // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)

	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			action(d.Body)
		}
	}()

	forever := make(chan bool)
	<-forever

	return nil
}

func (r *rabbitMq) Close() {
	defer r.Connection.Close()
	defer r.Channel.Close()
}
