package ratchet_processors

import (
	"context"
	"io"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/util"
)

type JSONLWriter struct {
	writer io.Writer
}

// NewJSONLWriter returns a new JSONLWriter wrapping the given io.Writer object
func NewJSONLWriter(w io.Writer) *JSONLWriter {
	return &JSONLWriter{
		writer: w,
	}
}

func (w *JSONLWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	_, err := w.writer.Write(d)
	util.KillPipelineIfErr(err, killChan, ctx)

	_, err = w.writer.Write([]byte("\n"))
	util.KillPipelineIfErr(err, killChan, ctx)
}

func (w *JSONLWriter) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {
}

func (w *JSONLWriter) String() string {
	return "JSONLWriter"
}
