package ratchet_processors

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/logger"
	"github.com/dailyburn/ratchet/util"
	"github.com/hashicorp/go-uuid"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// UdtReader connects to a UDT server via SSH and runs the specified query.
type UdtReader struct {
	sshClient    *ssh.Client
	config       *UdtConfig
	query        string
	recordSchema []*UdtFieldInfo
}

type UdtConfig struct {
	Address  string
	Username string
	Password string
}

const (
	UDTString  string = "string"
	UDTInt            = "int"
	UDTDecimal        = "decimal"
	UDTBool           = "bool"
)

type UdtFieldInfo struct {
	Name    string
	Type    string
	IsMulti bool
}

// NewUdtReader returns a new UdtReader that will run the given query and send
// each record one at a time.
func NewUdtReader(config *UdtConfig, query string, recordSchema []*UdtFieldInfo) (*UdtReader, error) {
	sshConfig := &ssh.ClientConfig{
		User: config.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(config.Password),
		},
	}

	sshClient, err := ssh.Dial("tcp", config.Address, sshConfig)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to (%s) as user (%s): %s", config.Address, config.Username, err)
	}

	return &UdtReader{
		sshClient:    sshClient,
		config:       config,
		query:        query,
		recordSchema: recordSchema,
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

	u, err := uuid.GenerateUUID()
	if err != nil {
		panic(err)
	}

	tempFile := fmt.Sprintf("_HOLD_/%s.txt", u)

	command := fmt.Sprintf("%s TO %s", r.query, tempFile)

	// Initiate PHANTOM process
	pid, comoFile, err := r.startUDTPhantom(command)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Unable to start UDT PHANTOM process: %s", err), killChan)
	}

	//log.Printf("Waiting for Remote Process to Terminate...\n")
	err = r.processWait(pid)
	//TODO: This always returns an error. Why? It still wait's for process completion though.
	//if err != nil {
	//	util.KillPipelineIfErr(fmt.Errorf("Error waiting for process to terminate: %s", err), killChan)
	//}

	// Retrieve COMO file and verify the command ran successfuly
	output, err := r.retrieveAndDelete(comoFile)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Error retrieving UDT output from (%s): %s", comoFile, err), killChan)
	}

	exp := fmt.Sprintf("COMMAND IS %s\nPHANTOM process %d has completed.", regexp.QuoteMeta(command), pid)
	re := regexp.MustCompile(exp)
	if re.Match(output) == false {
		util.KillPipelineIfErr(fmt.Errorf("Unexpected finish state in PHANTOM process:\n%s", output), killChan)
	}

	// Retrieve list output file
	output, err = r.retrieveAndDelete(tempFile)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Error retrieving LIST output from (%s): %s", tempFile, err), killChan)
	}

	// Iterate through each line of output and parse the record
	buf := bufio.NewReader(bytes.NewBuffer(output))
	for {
		d, err := r.readRecord(buf)
		if err != nil {
			if err == io.EOF {
				break
			}

			util.KillPipelineIfErr(fmt.Errorf("Error reading record from UDT output: %s", err), killChan)
		}

		forEach(d)
	}
}

func args(a ...interface{}) []interface{} { return a }

func (r *UdtReader) startUDTPhantom(command string) (pid int, comoPath string, err error) {
	session, err := r.sshClient.NewSession()
	if err != nil {
		return 0, "", fmt.Errorf("Unable to create SSH session: %s", err)
	}
	defer session.Close()

	var stderr bytes.Buffer
	session.Stderr = &stderr

	if err := session.Run(fmt.Sprintf("/usr/udthome/bin/udt PHANTOM %s", strconv.Quote(command))); err != nil {
		return 0, "", fmt.Errorf("running udt process: %s", err)
	}

	output := stderr.String()

	/* We're expecting output of the form:
	 *
	 * PHANTOM process 3342548 started.
	 * COMO file is '_PH_/tsp29397_3342548'.
	 */
	re := regexp.MustCompile("PHANTOM process (\\d+) started\\.\nCOMO file is '(.+)'\\.")
	match := re.FindStringSubmatch(output)
	if match == nil || len(match) != 3 {
		return 0, "", fmt.Errorf("Error parsing process output:\n%s\n%s", output, err)
	}
	pid, err = strconv.Atoi(match[1])
	if err != nil {
		panic("Surely a sign of the end times...")
	}

	comoPath = match[2]

	return pid, comoPath, nil
}

func (r *UdtReader) processWait(pid int) error {

	// Wait for UDT process to exit
	session, err := r.sshClient.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	if err := session.Run(fmt.Sprintf("/usr/bin/wait %d", pid)); err != nil {
		return fmt.Errorf("Error waiting for udt process to terminate: %s", err)
	}

	return nil
}

func (r *UdtReader) retrieveAndDelete(path string) ([]byte, error) {

	sftp, err := sftp.NewClient(r.sshClient)
	if err != nil {
		return nil, fmt.Errorf("Unable to initiat SFTP session: %s", err)
	}
	defer sftp.Close()

	f, err := sftp.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to open remote file (%s) using SFTP: %s", path, err)
	}
	defer f.Close()

	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("Unable to read from remote file (%s) using SFTP: %s", path, err)
	}

	f.Close()

	if err = sftp.Remove(path); err != nil {
		return nil, fmt.Errorf("Error deleting remote temporary file (%s) using SFTP: %s", path, err)
	}

	return buf, nil
}

func (r *UdtReader) readRecord(reader *bufio.Reader) (data.JSON, error) {

	// Expects record to be a string with fields delimited by an 0xFE character
	// For Example:
	// 001!610-220þ610-220þ65-2575

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimRight(line, "\n")

		// If the line is empty, get the next one
		if len(line) == 0 {
			continue
		}

		// Field Delimiter:		\xfe
		// MV Field Delimiter:	\xfd

		record := make(map[string]interface{})

		field_strs := strings.Split(line, "\xfe")

		for i, field_str := range field_strs {

			fieldInfo := r.recordSchema[i]
			if fieldInfo.IsMulti {
				var sub_fields []interface{}

				sub_field_strs := strings.Split(field_str, "\xfd")
				for _, v := range sub_field_strs {
					o, err := r.parseValue(v, fieldInfo.Type)
					if err != nil {
						logger.Error(fmt.Errorf("Error parsing multi-valued field (%s): %s\n %v", fieldInfo.Name, err, field_strs))
						continue
					}
					sub_fields = append(sub_fields, o)
				}

				record[fieldInfo.Name] = sub_fields
			} else {
				o, err := r.parseValue(field_str, fieldInfo.Type)
				if err != nil {
					logger.Error(fmt.Errorf("Error parsing field (%s): %s", fieldInfo.Name, err))
					continue
				}
				record[fieldInfo.Name] = o
			}
		}

		return data.NewJSON(record)
	}
}

func (r *UdtReader) parseValue(value string, dataType string) (interface{}, error) {

	//value = strings.TrimSpace(value)

	if value == "" {
		return nil, nil
	}

	switch dataType {
	default:
		return nil, fmt.Errorf("Unsupported type '%s'", dataType)
	case UDTString:
		return value, nil
	case UDTInt:
		return strconv.ParseInt(value, 10, 32)
	case UDTBool:
		return strconv.ParseBool(value)
	case UDTDecimal:
		return strconv.ParseFloat(value, 32)
	}
}
