package rabbitmq

import (
	"encoding/json"
	"sync"
)

type Client interface {
	Publish(body interface{}) error
	Listen(action Action) error
	Close()
}

type client struct {
	Config    Config
	Publisher RabbitMq
}

func NewClient(config Config) (Client, error) {
	publisher := getQueue(config)

	return &client{config, publisher}, nil
}

func (c *client) Publish(body interface{}) error {
	marshaled, err := json.Marshal(body)

	if err != nil {
		return err
	}

	err = c.Publisher.Publish(marshaled, "application/json")

	return err
}

func (c *client) Listen(action Action) error {
	wg := sync.WaitGroup{}

	for i := 0; i < c.Config.ConsumerCount; i++ {
		wg.Add(1)
		go receive(&wg, c.Config, action)
	}

	wg.Wait()

	return nil
}

func (c *client) Close() {
	c.Publisher.Close()
}

func receive(wg *sync.WaitGroup, config Config, action Action) error {
	listener := getQueue(config)

	defer listener.Close()
	defer wg.Done()

	err := listener.Listen(action)

	return err
}

func getQueue(config Config) RabbitMq {
	queue := NewRabbitMq(config)
	queue.Initialize()

	return queue
}
