package amqp

type Consumer interface {
	Consume() error
}
