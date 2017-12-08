package ratchet_processors

import (
	"bytes"
	"testing"
)

func TestCSVReader(t *testing.T) {
	// CSV test data
	in := bytes.NewBufferString(`
A,B,C,D
1,2,3,4
a,b,c,d
`)

	r, err := NewCSVReader(in)
	if err != nil {
		t.Fatalf("failed to initialize CSVReader: %s", err)
	}

	testRatchetProcessor(t, r, nil, []string{
		`[{"A":"1","B":"2","C":"3","D":"4"}]`,
		`[{"A":"a","B":"b","C":"c","D":"d"}]`,
	})
}
