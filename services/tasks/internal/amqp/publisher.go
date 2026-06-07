package amqp

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher публикует сообщения в очередь RabbitMQ.
type Publisher struct {
	ch *amqp.Channel
}

func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &Publisher{ch: ch}, nil
}

// PublishJob публикует произвольный объект в заданную очередь.
func (p *Publisher) PublishJob(ctx context.Context, queue string, job any) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return p.ch.PublishWithContext(
		ctx,
		"",    // default exchange
		queue, // routing key
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

// DeclareQueues объявляет основную очередь и DLQ.
func DeclareQueues(ch *amqp.Channel) error {
	// Сначала объявляем DLQ
	_, err := ch.QueueDeclare("task_jobs_dlq", true, false, false, false, nil)
	if err != nil {
		return err
	}
	// Основная очередь без привязки DLX (управление через код worker)
	_, err = ch.QueueDeclare("task_jobs", true, false, false, false, nil)
	return err
}

func (p *Publisher) Close() error { return p.ch.Close() }
