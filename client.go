package rabbitmq

import (
	"sync"
)

type Client interface {
	Publish(body []byte) error
	Listen(action Action)
	Errors() <-chan error
	Close()
}

type client struct {
	Config    Config
	Publisher RabbitMq
	Error     chan error
}

func NewClient(config Config) (Client, <-chan error) {
	publisher := getQueue(config)

	errors := make(chan error, 0)

	return &client{config, publisher, errors}, errors
}

func (c *client) Publish(body []byte) error {
	return c.Publisher.Publish(body)
}

func (c *client) Listen(action Action) {
	wg := sync.WaitGroup{}

	for i := 0; i < c.Config.ConsumerCount; i++ {
		wg.Add(1)
		go receive(&wg, c.Config, action, c.Error)
	}

	wg.Wait()
}

func (c *client) Errors() <-chan error {
	return c.Error
}

func (c *client) Close() {
	c.Publisher.Close()
	close(c.Error)
}

func receive(wg *sync.WaitGroup, config Config, action Action, errors chan error) {
	listener := getQueue(config)

	defer listener.Close()
	defer wg.Done()

	listener.Listen(action)

	go func() {
		for err := range listener.Errors() {
			errors <- err
		}
	}()

	forever := make(chan bool)
	<-forever
}

func getQueue(config Config) RabbitMq {
	queue := NewRabbitMq(config)
	queue.Initialize()

	return queue
}
