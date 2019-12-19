package ratchet_processors

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"

	"github.com/licaonfee/ratchet/data"
	"github.com/licaonfee/ratchet/processors"
	"github.com/licaonfee/ratchet/util"
)

type JSONLReader struct {
	scanner *bufio.Scanner
}

// Assert JSONLReader satisfies the interface processors.DataProcessor
var _ processors.DataProcessor = &JSONLReader{}

// NewJSONLReader returns a new JSONLReader wrapping the given io.Reader object
func NewJSONLReader(r io.Reader) *JSONLReader {
	return &JSONLReader{
		scanner: bufio.NewScanner(r),
	}
}

func (r *JSONLReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	var line []byte
	for r.scanner.Scan() {
		line = r.scanner.Bytes()

		if !json.Valid(line) {
			util.KillPipelineIfErr(errors.New("Not valid JSON"), killChan)
		}

		// scanner.Bytes will overwrite our slice on the next iteration so we send a copy
		// to the output channel
		outputChan <- append([]byte(nil), line...)
	}

	if err := r.scanner.Err(); err != nil {
		util.KillPipelineIfErr(err, killChan)
	}
}

func (r *JSONLReader) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *JSONLReader) String() string {
	return "JSONLReader"
}
