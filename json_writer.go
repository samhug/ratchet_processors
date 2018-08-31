package ratchet_processors

import (
	"context"
	"io"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/util"
)

type JSONWriter struct {
	writer io.Writer
	buffer []data.JSON
}

// NewJSONWriter returns a new JSONWriter wrapping the given io.Writer object
func NewJSONWriter(w io.Writer) *JSONWriter {
	return &JSONWriter{
		writer: w,
		buffer: make([]data.JSON, 0),
	}
}

func (w *JSONWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	w.buffer = append(w.buffer, d)
}

func (w *JSONWriter) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {

	_, err := w.writer.Write([]byte("["))
	util.KillPipelineIfErr(err, killChan, ctx)

	last := len(w.buffer) - 1
	for i, d := range w.buffer {
		_, err = w.writer.Write(d)
		util.KillPipelineIfErr(err, killChan, ctx)

		if i != last {
			_, err = w.writer.Write([]byte(","))
		}
		util.KillPipelineIfErr(err, killChan, ctx)
	}

	_, err = w.writer.Write([]byte("]"))
	util.KillPipelineIfErr(err, killChan, ctx)
}

func (w *JSONWriter) String() string {
	return "JSONWriter"
}
