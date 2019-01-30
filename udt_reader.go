package ratchet_processors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/util"
	"github.com/samhug/udt"

	pb "gopkg.in/cheggaaa/pb.v1"
)

// UdtReader connects to a UDT server via SSH and runs the specified query.
type UdtReader struct {
	client *udt.Client
	query  *UdtQueryConfig
}

// UdtEnvConfig holds configuration info for a UDT connection
type UdtEnvConfig = udt.EnvConfig

// UdtQueryConfig represents a query to be run on a UDT server
type UdtQueryConfig = udt.QueryConfig

// NewUdtReader returns a new UdtReader that will run the given query and send
// each record one at a time.
func NewUdtReader(client *udt.Client, query *UdtQueryConfig) (*UdtReader, error) {

	return &UdtReader{
		client: client,
		query:  query,
	}, nil
}

// ProcessData implements the ratchet.DataProcessor interface
func (r *UdtReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	r.runUDTQuery(killChan, ctx, func(d data.JSON) {
		outputChan <- d
	})
}

// Finish implements the ratchet.DataProcessor interface
func (r *UdtReader) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {}

// String returns a string containing the type of DataProcessor
func (r *UdtReader) String() string {
	return "UdtReader"
}

func (r *UdtReader) runUDTQuery(killChan chan error, ctx context.Context, forEach func(d data.JSON)) {

	log.Printf("Selecting %s records ...\n", r.query.File)

	q, err := udt.NewQueryBatched(r.client, r.query)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("UDT query failed: %s", err), killChan, ctx)
		return
	}
	defer func() {
		if err := q.Close(); err != nil {
			util.KillPipelineIfErr(fmt.Errorf("error closing query: %s", err), killChan, ctx)
		}
	}()

	log.Printf("Selected %d records ...\n", q.Count())

	bar := pb.StartNew(q.Count())
	bar.Prefix("Fetching " + r.query.File + " records: ")

	for {
		record, err := q.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			util.KillPipelineIfErr(fmt.Errorf("failed to read udt record: %s", err), killChan, ctx)
			return
		}

		data, err := json.Marshal(record)
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("failed to encode udt record: %s", err), killChan, ctx)
			return
		}
		forEach(data)

		bar.Increment()
	}
	bar.Finish()
}
