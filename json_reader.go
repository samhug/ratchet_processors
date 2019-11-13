package ratchet_processors

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"

	"github.com/licaonfee/ratchet/data"
	"github.com/licaonfee/ratchet/processors"
	"github.com/licaonfee/ratchet/util"
)

type JSONReader struct {
	reader io.Reader
}

// Assert JSONReader satisfies the interface processors.DataProcessor
var _ processors.DataProcessor = &JSONReader{}

// NewJSONReader returns a new JSONReader wrapping the given io.Reader object
func NewJSONReader(r io.Reader) *JSONReader {
	return &JSONReader{
		reader: r,
	}
}

func (r *JSONReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	buf, err := ioutil.ReadAll(r.reader)
	util.KillPipelineIfErr(err, killChan)

	if !json.Valid(buf) {
		util.KillPipelineIfErr(errors.New("Not valid JSON"), killChan)
	}

	outputChan <- buf
}

func (r *JSONReader) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *JSONReader) String() string {
	return "JSONReader"
}
