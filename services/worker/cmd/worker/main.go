package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/omnikk/pz14/services/worker/internal/store"
)

const maxAttempts = 3

// TaskJob — структура сообщения из очереди.
type TaskJob struct {
	Job       string `json:"job"`
	TaskID    string `json:"task_id"`
	Attempt   int    `json:"attempt"`
	MessageID string `json:"message_id"`
}

func main() {
	rabbitURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("rabbit connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	// Объявляем очереди (idempotent)
	if err := declareQueues(ch); err != nil {
		log.Fatalf("declare queues: %v", err)
	}

	// Prefetch: одно сообщение за раз
	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("qos: %v", err)
	}

	msgs, err := ch.Consume("task_jobs", "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}

	processed := store.NewProcessedStore()

	log.Printf("worker started (maxAttempts=%d, prefetch=1)", maxAttempts)

	for d := range msgs {
		handleMessage(d, ch, processed)
	}
}

func handleMessage(d amqp.Delivery, ch *amqp.Channel, processed *store.ProcessedStore) {
	var job TaskJob
	if err := json.Unmarshal(d.Body, &job); err != nil {
		log.Printf("ERROR: bad message: %v | raw: %s", err, string(d.Body))
		_ = d.Nack(false, false) // не повторять — формат неверный
		return
	}

	log.Printf("received: job=%s task_id=%s attempt=%d message_id=%s",
		job.Job, job.TaskID, job.Attempt, job.MessageID)

	// ── Идемпотентность ──────────────────────────────────────────────────────
	if processed.Exists(job.MessageID) {
		log.Printf("  → SKIP: message_id=%s already processed (idempotent)", job.MessageID)
		_ = d.Ack(false)
		return
	}

	// ── Обработка ────────────────────────────────────────────────────────────
	if err := processTask(job); err != nil {
		log.Printf("  → ERROR: attempt=%d/%d: %v", job.Attempt, maxAttempts, err)

		job.Attempt++
		if job.Attempt <= maxAttempts {
			// Повторная попытка: публикуем сообщение заново с увеличенным attempt
			if pubErr := publishJob(ch, "task_jobs", job); pubErr != nil {
				log.Printf("  → ERROR: retry publish failed: %v", pubErr)
			} else {
				log.Printf("  → RETRY: re-queued as attempt=%d", job.Attempt)
			}
		} else {
			// Исчерпаны попытки → DLQ
			if pubErr := publishJob(ch, "task_jobs_dlq", job); pubErr != nil {
				log.Printf("  → ERROR: DLQ publish failed: %v", pubErr)
			} else {
				log.Printf("  → DLQ: message_id=%s moved to task_jobs_dlq after %d attempts", job.MessageID, maxAttempts)
			}
		}
		_ = d.Ack(false) // исходное сообщение подтверждаем в любом случае
		return
	}

	// ── Успех ────────────────────────────────────────────────────────────────
	processed.MarkDone(job.MessageID)
	log.Printf("  → SUCCESS: task_id=%s processed on attempt=%d", job.TaskID, job.Attempt)
	_ = d.Ack(false)
}

// processTask — имитация тяжёлой обработки.
// task_id == "t_fail" всегда вызывает ошибку для демонстрации DLQ.
func processTask(job TaskJob) error {
	time.Sleep(500 * time.Millisecond) // имитация работы
	if job.TaskID == "t_fail" {
		return fmt.Errorf("simulated processing error for task_id=%s", job.TaskID)
	}
	return nil
}

// publishJob сериализует job и публикует в заданную очередь.
func publishJob(ch *amqp.Channel, queue string, job TaskJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(
		context.Background(),
		"",    // default exchange
		queue, // routing key = queue name
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

// declareQueues объявляет основную очередь и DLQ.
func declareQueues(ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare("task_jobs_dlq", true, false, false, false, nil); err != nil {
		return err
	}
	_, err := ch.QueueDeclare("task_jobs", true, false, false, false, nil)
	return err
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
