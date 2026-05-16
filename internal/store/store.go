package store

import (
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("key not found")

type Record struct {
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Record
}

func New() *Store {
	return &Store{data: make(map[string]Record)}
}

func (s *Store) Put(key, value string, version int64) Record {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, exists := s.data[key]
	if exists && current.Version > version {
		return current
	}

	record := Record{
		Key:       key,
		Value:     value,
		Version:   version,
		UpdatedAt: time.Now().UTC(),
	}
	s.data[key] = record
	return record
}

func (s *Store) Get(key string) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	record, ok := s.data[key]
	if !ok {
		return Record{}, ErrNotFound
	}
	return record, nil
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
