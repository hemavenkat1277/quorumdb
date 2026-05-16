package store

import "testing"

func TestPutKeepsNewestVersion(t *testing.T) {
	s := New()

	s.Put("name", "old", 10)
	s.Put("name", "stale", 5)
	s.Put("name", "new", 15)

	record, err := s.Get("name")
	if err != nil {
		t.Fatalf("get record: %v", err)
	}
	if record.Value != "new" {
		t.Fatalf("expected newest value, got %q", record.Value)
	}
}
