package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"

	amqplib "github.com/rabbitmq/amqp091-go"

	mqpub "github.com/omnikk/pz14/services/tasks/internal/amqp"
	"github.com/omnikk/pz14/services/tasks/internal/jobs"
)

func main() {
	rabbitURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	port := getenv("TASKS_PORT", "8082")

	conn, err := amqplib.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("rabbit connect: %v", err)
	}
	defer conn.Close()

	// Объявляем очереди
	ch, _ := conn.Channel()
	if err := mqpub.DeclareQueues(ch); err != nil {
		log.Fatalf("declare queues: %v", err)
	}
	ch.Close()

	pub, err := mqpub.NewPublisher(conn)
	if err != nil {
		log.Fatalf("publisher init: %v", err)
	}
	defer pub.Close()

	mux := http.NewServeMux()

	// GET /health
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// POST /v1/jobs/process-task
	mux.HandleFunc("/v1/jobs/process-task", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			TaskID string `json:"task_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.TaskID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_id required"})
			return
		}

		job := jobs.TaskJob{
			Job:       "process_task",
			TaskID:    body.TaskID,
			Attempt:   1,
			MessageID: fmt.Sprintf("msg-%d", rand.Int63()),
		}

		if err := pub.PublishJob(context.Background(), "task_jobs", job); err != nil {
			log.Printf("publish error: %v", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "publish failed"})
			return
		}

		log.Printf("job enqueued: task_id=%s message_id=%s", job.TaskID, job.MessageID)
		writeJSON(w, http.StatusAccepted, map[string]string{
			"status":  "accepted",
			"task_id": job.TaskID,
		})
	})

	log.Printf("tasks service (job producer) listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
