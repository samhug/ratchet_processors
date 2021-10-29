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
	reader *bufio.Reader
}

// Assert JSONLReader satisfies the interface processors.DataProcessor
var _ processors.DataProcessor = &JSONLReader{}

// NewJSONLReader returns a new JSONLReader wrapping the given io.Reader object
func NewJSONLReader(r io.Reader) *JSONLReader {
	return &JSONLReader{
		reader: bufio.NewReader(r),
	}
}

func (r *JSONLReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// TODO: This allocates more than is necessary but at least it can handle large lines
Outer:
	for {
		var line []byte
		for {
			chunk, isPrefix, err := r.reader.ReadLine()
			if err != nil {
				if err == io.EOF {
					break Outer
				}
				util.KillPipelineIfErr(err, killChan)
			}
			line = append(line, chunk...)
			if !isPrefix {
				break
			}
		}

		if !json.Valid(line) {
			util.KillPipelineIfErr(errors.New("Not valid JSON"), killChan)
		}

		outputChan <- line
	}
}

func (r *JSONLReader) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *JSONLReader) String() string {
	return "JSONLReader"
}
