package ratchet_processors

import (
	"io"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
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

func (w *JSONWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	w.buffer = append(w.buffer, d)
}

func (w *JSONWriter) Finish(outputChan chan data.JSON, killChan chan error) {

	_, err := w.writer.Write([]byte("["))
	util.KillPipelineIfErr(err, killChan)

	for _, d := range w.buffer {
		_, err = w.writer.Write(d)
		util.KillPipelineIfErr(err, killChan)

		_, err = w.writer.Write([]byte(","))
		util.KillPipelineIfErr(err, killChan)
	}

	_, err = w.writer.Write([]byte("]"))
	util.KillPipelineIfErr(err, killChan)
}

func (w *JSONWriter) String() string {
	return "JSONWriter"
}
