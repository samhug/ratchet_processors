package ratchet_processors

import (
	"io"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

type JSONWriter struct {
	writer io.Writer
}

// NewJSONWriter returns a new JSONWriter wrapping the given io.Writer object
func NewJSONWriter(w io.Writer) *JSONWriter {
	return &JSONWriter{
		writer: w,
	}
}

func (w *JSONWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	_, err := w.writer.Write(d)
	util.KillPipelineIfErr(err, killChan)
}

func (w *JSONWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (w *JSONWriter) String() string {
	return "JSONWriter"
}
