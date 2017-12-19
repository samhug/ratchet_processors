package ratchet_processors

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
	"github.com/samuelhug/udt"
	"golang.org/x/crypto/ssh"
)

// UdtReader connects to a UDT server via SSH and runs the specified query.
type UdtReader struct {
	conn   *udt.Connection
	config *UdtConfig
	query  string
}

type UdtConfig struct {
	Address  string
	Username string
	Password string
	UdtBin   string
	UdtHome  string
}

type UdtFieldInfo struct {
	Name    string
	Type    string
	IsMulti bool
}

// NewUdtReader returns a new UdtReader that will run the given query and send
// each record one at a time.
func NewUdtReader(config *UdtConfig, query string) (*UdtReader, error) {
	sshConfig := &ssh.ClientConfig{
		User: config.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(config.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshClient, err := ssh.Dial("tcp", config.Address, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to (%s) as user (%s): %s", config.Address, config.Username, err)
	}

	conn := udt.NewConnection(sshClient, config.UdtBin, config.UdtHome)
	return &UdtReader{
		conn:   conn,
		config: config,
		query:  query,
	}, nil
}

func (r *UdtReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	r.runUDTQuery(killChan, func(d data.JSON) {
		outputChan <- d
	})
}

func (r *UdtReader) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (r *UdtReader) String() string {
	return "UdtReader"
}

func (r *UdtReader) runUDTQuery(killChan chan error, forEach func(d data.JSON)) {

	query := udt.NewQuery(fmt.Sprintf("%s TOXML", r.query))
	results, err := query.Run(r.conn)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("error running UDT query: %s", err), killChan)
		return
	}
	defer results.Close()

	for {
		record, err := results.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			util.KillPipelineIfErr(fmt.Errorf("error reading udt result record: %s", err), killChan)
			return
		}

		data, err := json.Marshal(record)
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("error encoding udt result record as JSON: %s", err), killChan)
			return
		}
		forEach(data)
	}
}
