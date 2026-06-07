// Package store хранит множество обработанных message_id для идемпотентной обработки.
package store

import "sync"

// ProcessedStore — in-memory хранилище обработанных сообщений.
// В production здесь был бы Redis или Postgres.
type ProcessedStore struct {
	mu    sync.RWMutex
	items map[string]bool
}

func NewProcessedStore() *ProcessedStore {
	return &ProcessedStore{items: make(map[string]bool)}
}

// Exists возвращает true, если сообщение с данным ID уже обработано.
func (s *ProcessedStore) Exists(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.items[id]
}

// MarkDone помечает сообщение как обработанное.
func (s *ProcessedStore) MarkDone(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[id] = true
}
