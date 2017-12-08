package ratchet_processors

import (
	"bytes"
	"testing"
)

func TestJSONWriter(t *testing.T) {
	// JSON test data
	out := new(bytes.Buffer)

	r := NewJSONWriter(out)
	in := []string{`{"A":"1","B":"2","C":"3","D":"4"}`, `{"A":"a","B":"b","C":"c","D":"d"}`}
	testRatchetProcessor(t, r, in, nil)

	expOut := `[{"A":"1","B":"2","C":"3","D":"4"},{"A":"a","B":"b","C":"c","D":"d"}]`
	if eq, err := testDataEqual(expOut, out.String()); err != nil {
		t.Errorf("unable to compare values %s", err)
	} else if !eq {
		t.Fatalf("expected %q but got %q", expOut, out.Bytes())
	}
}
