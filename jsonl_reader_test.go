package ratchet_processors

import (
	"bytes"
	"testing"
)

func TestJSONLReader(t *testing.T) {
	// JSONL test data
	in := bytes.NewBufferString(`{"A": "1", "B": "2", "C": "3", "D": "4"}
{"A": "a", "B": "b", "C": "c", "D": "d"}
`)

	r := NewJSONLReader(in)

	testRatchetProcessor(t, r, nil, []string{
		`{"A":"1","B":"2","C":"3","D":"4"}`,
		`{"A":"a","B":"b","C":"c","D":"d"}`,
	})
}
