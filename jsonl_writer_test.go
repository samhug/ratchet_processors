package ratchet_processors

import (
	"bytes"
	"testing"
)

func TestJSONLWriter(t *testing.T) {
	// JSONL test data
	out := new(bytes.Buffer)

	r := NewJSONLWriter(out)
	in := []string{`{"A":"1","B":"2","C":"3","D":"4"}`, `{"A":"a","B":"b","C":"c","D":"d"}`}
	testRatchetProcessor(t, r, in, nil)

	expOut := `{"A":"1","B":"2","C":"3","D":"4"}` + "\n" + `{"A":"a","B":"b","C":"c","D":"d"}` + "\n"
	if expOut != out.String() {
		t.Fatalf("expected:\n%s\nbut got:\n%s", expOut, out.String())
	}
}
