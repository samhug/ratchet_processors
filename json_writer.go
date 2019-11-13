package ratchet_processors

import (
	"io"

	"github.com/licaonfee/ratchet/data"
	"github.com/licaonfee/ratchet/processors"
	"github.com/licaonfee/ratchet/util"
)

type JSONWriter struct {
	writer io.Writer
	buffer []data.JSON
}

// Assert JSONWriter satisfies the interface processors.DataProcessor
var _ processors.DataProcessor = &JSONWriter{}

// NewJSONWriter returns a new JSONWriter wrapping the given io.Writer object
func NewJSONWriter(w io.Writer) *JSONWriter {
	return &JSONWriter{
		writer: w,
		buffer: make([]data.JSON, 0),
	}
}

func (w *JSONWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	w.buffer = append(w.buffer, d)
}

func (w *JSONWriter) Finish(outputChan chan data.JSON, killChan chan error) {

	_, err := w.writer.Write([]byte("["))
	util.KillPipelineIfErr(err, killChan)

	last := len(w.buffer) - 1
	for i, d := range w.buffer {
		_, err = w.writer.Write(d)
		util.KillPipelineIfErr(err, killChan)

		if i != last {
			_, err = w.writer.Write([]byte(","))
		}
		util.KillPipelineIfErr(err, killChan)
	}

	_, err = w.writer.Write([]byte("]"))
	util.KillPipelineIfErr(err, killChan)
}

func (w *JSONWriter) String() string {
	return "JSONWriter"
}
