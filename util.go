package ratchet_processors

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/rhansen2/ratchet"
	"github.com/rhansen2/ratchet/data"
)

type testInput struct {
	inputData []string
}

// ProcessData implements the ratchet.DataProcessor interface
func (p *testInput) ProcessData(_ data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	for _, d := range p.inputData {
		outputChan <- []byte(d)
	}
}

// Finish implements the ratchet.DataProcessor interface
func (p *testInput) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {

}

type testOutput struct {
	Data []string
}

func (p *testOutput) ProcessData(d data.JSON, _ chan data.JSON, killChan chan error, ctx context.Context) {
	p.Data = append(p.Data, string(d))
	//fmt.Println("OUT:", d)
}
func (p *testOutput) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {

}

func testDataEqual(s1, s2 string) (bool, error) {
	var o1 interface{}
	var o2 interface{}

	var err error
	err = json.Unmarshal([]byte(s1), &o1)
	if err != nil {
		return false, fmt.Errorf("failed to marshal string 1 :: %s", err.Error())
	}
	err = json.Unmarshal([]byte(s2), &o2)
	if err != nil {
		return false, fmt.Errorf("failed to marshal string 2 :: %s", err.Error())
	}

	return reflect.DeepEqual(o1, o2), nil
}

func testRatchetProcessor(t *testing.T, r ratchet.DataProcessor, inputData []string, expectedOutput []string) {
	t.Helper()

	out := &testOutput{}

	processors := []ratchet.DataProcessor{}

	if inputData != nil {
		processors = append(processors, &testInput{inputData})
	}
	processors = append(processors, r, out)

	pipeline := ratchet.NewPipeline(context.TODO(), func() {}, processors...)
	err := <-pipeline.Run()
	if err != nil {
		t.Fatal(err)
	}

	if len(expectedOutput) != len(out.Data) {
		t.Fatalf("expected %d outputs, got %d\nEXPECTED: %q\n GOT: %q", len(expectedOutput), len(out.Data), expectedOutput, out.Data)
	}

	for i := 0; i < len(expectedOutput); i++ {
		if eq, err := testDataEqual(expectedOutput[i], out.Data[i]); err != nil {
			t.Errorf("failed to compare values for output %d: %s", i, err)
		} else if !eq {
			t.Errorf("expected output %d to be %q got %q", i, expectedOutput[i], out.Data[i])
		}
	}
}
