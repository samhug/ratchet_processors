package ratchet_processors

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/util"
)

type JSONLReader struct {
	reader *bufio.Reader
}

// NewJSONLReader returns a new JSONLReader wrapping the given io.Reader object
func NewJSONLReader(r io.Reader) *JSONLReader {
	return &JSONLReader{
		reader: bufio.NewReader(r),
	}
}

func (r *JSONLReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	var err error
	for {
		buf, err := r.reader.ReadBytes('\n')
		if err != nil {
			break
		}

		// Strip trailing newline
		buf = buf[:len(buf)-1]

		if !json.Valid(buf) {
			util.KillPipelineIfErr(errors.New("Not valid JSON"), killChan, ctx)
		}

		outputChan <- buf
	}

	if err != io.EOF {
		util.KillPipelineIfErr(err, killChan, ctx)
	}
}

func (r *JSONLReader) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {
}

func (r *JSONLReader) String() string {
	return "JSONLReader"
}
