package ratchet_processors

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/util"
)

type JSONReader struct {
	reader io.Reader
}

// NewJSONReader returns a new JSONReader wrapping the given io.Reader object
func NewJSONReader(r io.Reader) *JSONReader {
	return &JSONReader{
		reader: r,
	}
}

func (r *JSONReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	buf, err := ioutil.ReadAll(r.reader)
	util.KillPipelineIfErr(err, killChan, ctx)

	if !json.Valid(buf) {
		util.KillPipelineIfErr(errors.New("Not valid JSON"), killChan, ctx)
	}

	outputChan <- buf
}

func (r *JSONReader) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {
}

func (r *JSONReader) String() string {
	return "JSONReader"
}
