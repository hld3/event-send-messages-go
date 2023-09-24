package sender

import (
	"github.com/streadway/amqp"
)

type RabbitMQSender struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func NewSender() (*RabbitMQSender, error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &RabbitMQSender{
		conn:    conn,
		channel: ch,
		queue:   q,
	}, nil
}

func (s *RabbitMQSender) SendMessage(body string, eventType string) error {
	return s.channel.Publish(
		"",
		s.queue.Name,
		false,
		false,
		amqp.Publishing{
			Headers: map[string]interface{}{
				"eventType": eventType,
			},
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
}

func (s *RabbitMQSender) Close() {
	s.channel.Close()
	s.conn.Close()
}

