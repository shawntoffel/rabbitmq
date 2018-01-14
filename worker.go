package rabbitmq

type Worker interface {
	Start() <-chan error
	Stop()
}

type worker struct {
	Id       int
	RabbitMq RabbitMq
	Action   Action
	Quit     chan bool
}

func NewWorker(id int, config Config, action Action) Worker {
	queue := NewRabbitMq(config)
	queue.Initialize()

	quit := make(chan bool, 0)

	return &worker{id, queue, action, quit}
}

func (w *worker) Start() <-chan error {
	errors := make(chan error, 0)

	messages, err := w.RabbitMq.Listen()

	if err != nil {
		errors <- err

		return errors
	}

	go func() {
		for {
			select {
			case message := <-messages:
				err := w.Action(message)

				if err != nil {
					errors <- err
				}
			case <-w.Quit:
				return
			}
		}
	}()

	return errors
}

func (w *worker) Stop() {
	go func() {
		w.Quit <- true
	}()
}
