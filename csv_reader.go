package ratchet_processors

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

type CSVReader struct {
	reader  *csv.Reader
	headers []string
}

func NewCSVReader(reader io.Reader) (*CSVReader, error) {

	csvReader := csv.NewReader(reader)

	headers, err := csvReader.Read()
	if err == io.EOF {
		return nil, fmt.Errorf("Unable to read headers from CSV. EOF recieved.")
	}
	if err != nil {
		return nil, fmt.Errorf("Error reading headers from CSV: %s", err)
	}

	return &CSVReader{
		reader:  csvReader,
		headers: headers,
	}, nil
}

func (r *CSVReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	r.ForEachData(killChan, func(d data.JSON) {
		outputChan <- d
	})
}

func (r *CSVReader) ForEachData(killChan chan error, forEach func(d data.JSON)) {

	for {
		row, err := r.reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			util.KillPipelineIfErr(fmt.Errorf("Error reading CSV rows: %s", err), killChan)
		}

		fields := make([]interface{}, len(row))
		for i, v := range row {
			fields[i] = v
		}

		rows := [][]interface{}{fields}

		d, err := data.JSONFromHeaderAndRows(r.headers, rows)
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("Error marshaling CSV rows: %s", err), killChan)
		}
		forEach(d)
	}

}

func (r *CSVReader) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *CSVReader) String() string {
	return "JsTransform"
}
