package ratchet_processors

import (
	"testing"
)

func TestJSTransform(t *testing.T) {
	// JSON test data
	in := []string{`
	{
		"A": 1,
		"B": 2,
		"C": 3,
		"D": 4
	}`, `
	{
		"A": 2,
		"B": 4,
		"C": 6,
		"D": 8
	}`,
	}

	r, err := NewJsTransform(`
for (i=0; i<data.length; i+=1) {
	data[i]["A"] += 1;
	data[i]["B"] += 2;
	data[i]["C"] += 3;
	data[i]["D"] += 4;

	output(data[i]);
}
`)
	if err != nil {
		t.Fatalf("unable to initialize JSTransform: %s", err)
	}

	testRatchetProcessor(t, r, in, []string{
		`{"A":2,"B":4,"C":6,"D":8}`, `{"A":3,"B":6,"C":9,"D":12}`,
	})
}
