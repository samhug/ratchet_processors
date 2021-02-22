package ratchet_processors

import (
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/licaonfee/ratchet/data"
	"github.com/licaonfee/ratchet/processors"
	"github.com/licaonfee/ratchet/util"
	"github.com/samhug/udt"

	pb "github.com/cheggaaa/pb/v3"
)

// UdtReader connects to a UDT server via SSH and runs the specified query.
type UdtReader struct {
	client *udt.Client
	query  *UdtQueryConfig
}

// Assert UdtReader satisfies the interface processors.DataProcessor
var _ processors.DataProcessor = &UdtReader{}

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
func (r *UdtReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	r.runUDTQuery(killChan, func(d data.JSON) {
		outputChan <- d
	})
}

// Finish implements the ratchet.DataProcessor interface
func (r *UdtReader) Finish(outputChan chan data.JSON, killChan chan error) {}

// String returns a string containing the type of DataProcessor
func (r *UdtReader) String() string {
	return "UdtReader"
}

func (r *UdtReader) runUDTQuery(killChan chan error, forEach func(d data.JSON)) {

	log.Printf("Selecting %s records ...\n", r.query.File)

	q, err := udt.NewQueryBatched(r.client, r.query)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("UDT query failed: %s", err), killChan)
		return
	}
	defer func() {
		if err := q.Close(); err != nil {
			util.KillPipelineIfErr(fmt.Errorf("error closing query: %s", err), killChan)
		}
	}()

	log.Printf("Selected %d records ...\n", q.Count())

	bar := pb.New(q.Count())
	bar.SetTemplateString(`{{ green "Fetching records:" }} {{ string . "file" | blue }} {{ bar . "[" "=" ">" "-" "]"}} {{speed . "%s records/sec" "..." | blue }} {{percent . | green}}`)
	bar.Set("file", r.query.File)
	bar.Start()

	for {
		record, err := q.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			util.KillPipelineIfErr(fmt.Errorf("failed to read udt record: %s", err), killChan)
			return
		}

		data, err := json.Marshal(record)
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("failed to encode udt record: %s", err), killChan)
			return
		}
		forEach(data)

		bar.Increment()
	}
	bar.Finish()
}
