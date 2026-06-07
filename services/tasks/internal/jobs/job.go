// Package jobs описывает формат сообщения-задачи для очереди.
package jobs

// TaskJob — сообщение, которое ставится в очередь task_jobs.
type TaskJob struct {
	Job       string `json:"job"`
	TaskID    string `json:"task_id"`
	Attempt   int    `json:"attempt"`
	MessageID string `json:"message_id"`
}
