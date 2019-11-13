package ratchet_processors

import (
	"io"

	"github.com/licaonfee/ratchet/data"
	"github.com/licaonfee/ratchet/processors"
	"github.com/licaonfee/ratchet/util"
)

type JSONLWriter struct {
	writer io.Writer
}

// Assert JSONLWriter satisfies the interface processors.DataProcessor
var _ processors.DataProcessor = &JSONLWriter{}

// NewJSONLWriter returns a new JSONLWriter wrapping the given io.Writer object
func NewJSONLWriter(w io.Writer) *JSONLWriter {
	return &JSONLWriter{
		writer: w,
	}
}

func (w *JSONLWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	_, err := w.writer.Write(d)
	util.KillPipelineIfErr(err, killChan)

	_, err = w.writer.Write([]byte("\n"))
	util.KillPipelineIfErr(err, killChan)
}

func (w *JSONLWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (w *JSONLWriter) String() string {
	return "JSONLWriter"
}
