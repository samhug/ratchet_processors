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
	var err error
	for {
		buf, err := r.reader.ReadBytes('\n')
		if err != nil {
			break
		}

		// Strip trailing newline
		buf = buf[:len(buf)-1]

		if !json.Valid(buf) {
			util.KillPipelineIfErr(errors.New("Not valid JSON"), killChan)
		}

		outputChan <- buf
	}

	if err != io.EOF {
		util.KillPipelineIfErr(err, killChan)
	}
}

func (r *JSONLReader) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *JSONLReader) String() string {
	return "JSONLReader"
}
