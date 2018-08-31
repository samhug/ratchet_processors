package ratchet_processors

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/rhansen2/ratchet"
	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/util"
)

// CSVReader is a ratchet DataProcessor for extracting data from CSV files
type CSVReader struct {
	reader  *csv.Reader
	headers []string
}

// Assert CSVReader satisfies the interface ratchet.DataProcessor
var _ ratchet.DataProcessor = &CSVReader{}

// NewCSVReader creates a new CSVReader that will read CSV data from an io.Reader
func NewCSVReader(reader io.Reader) (*CSVReader, error) {

	csvReader := csv.NewReader(reader)

	headers, err := csvReader.Read()
	if err == io.EOF {
		return nil, fmt.Errorf("unable to read headers from CSV: EOF received")
	}
	if err != nil {
		return nil, fmt.Errorf("error reading headers from CSV: %s", err)
	}

	return &CSVReader{
		reader:  csvReader,
		headers: headers,
	}, nil
}

// ProcessData will be called for each data sent from the previous stage.
func (r *CSVReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	r.forEachData(killChan, ctx, func(d data.JSON) {
		outputChan <- d
	})
}

func (r *CSVReader) forEachData(killChan chan error, ctx context.Context, forEach func(d data.JSON)) {

	for {
		row, err := r.reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			util.KillPipelineIfErr(fmt.Errorf("Error reading CSV rows: %s", err), killChan, ctx)
		}

		fields := make([]interface{}, len(row))
		for i, v := range row {
			fields[i] = v
		}

		rows := [][]interface{}{fields}

		d, err := data.JSONFromHeaderAndRows(r.headers, rows)
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("Error marshaling CSV rows: %s", err), killChan, ctx)
		}
		forEach(d)
	}

}

// Finish will be called after the previous stage has finished sending data,
func (r *CSVReader) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {
}

func (r *CSVReader) String() string {
	return "CSVReader"
}
