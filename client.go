package rabbitmq

type Client interface {
	Publish(body []byte) error
	Listen(action Action) <-chan error
	Close()
}

type client struct {
	Config    Config
	Publisher RabbitMq
}

func NewClient(config Config) (Client, error) {
	publisher := NewRabbitMq(config)
	err := publisher.Initialize()

	return &client{config, publisher}, err
}

func (c *client) Publish(body []byte) error {
	return c.Publisher.Publish(body)
}

func (c *client) Listen(action Action) <-chan error {

	errors := make(chan error, 0)

	for i := 0; i < c.Config.ConsumerCount; i++ {
		go func(id int) {
			worker := NewWorker(id, c.Config, action)

			workerErrors := worker.Start()

			go func() {
				for err := range workerErrors {
					errors <- err
				}
			}()
		}(i)
	}

	return errors
}

func (c *client) Close() {
	c.Publisher.Close()
}
