package ratchet_processors

import (
	"fmt"

	"github.com/licaonfee/ratchet/data"
	"github.com/licaonfee/ratchet/processors"
	"github.com/licaonfee/ratchet/util"
	"github.com/robertkrimen/otto"
)

type JSConfig struct {
	InitScript string
	MainScript string
}

type JsTransform struct {
	vm     *otto.Otto
	script *otto.Script
}

// Assert JsTransform satisfies the interface processors.DataProcessor
var _ processors.DataProcessor = &JsTransform{}

func NewJsTransform(script string) (*JsTransform, error) {

	vm := otto.New()

	// Add logging function
	if err := vm.Set("logWrite", func(call otto.FunctionCall) otto.Value {
		fmt.Printf("JS_LOG: %s\n", call.Argument(0).String())
		return otto.NullValue()
	}); err != nil {
		return nil, fmt.Errorf("error adding debug command to Javascript VM: %s", err)
	}

	vmScript, err := vm.Compile("", script)
	if err != nil {
		return nil, fmt.Errorf("Error compiling script: %s", err)
	}

	return &JsTransform{
		vm:     vm,
		script: vmScript,
	}, nil
}

// ProcessData implements the ratchet.DataProcessor interface
func (t *JsTransform) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	objs, err := data.ObjectsFromJSON(d)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Unable to unmarshal JSON object for JS transformation: %s", err), killChan)
	}

	if err := t.vm.Set("data", objs); err != nil {
		util.KillPipelineIfErr(fmt.Errorf("error inserting data into JS VM: %s", err), killChan)
	}

	// Register output callback function with the VM
	if err := t.vm.Set("output", func(call otto.FunctionCall) otto.Value {

		if len(call.ArgumentList) != 1 {
			util.KillPipelineIfErr(fmt.Errorf("Javascript VM error: 'output' function takes 1 argument"), killChan)
		}

		obj, err := call.ArgumentList[0].Export()
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("Error exporting JS value: %s", err), killChan)
		}

		d, err = data.NewJSON(obj)
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("Unable to marshal JS transformed object: %s", err), killChan)
		}

		outputChan <- d

		return otto.NullValue()
	}); err != nil {
		util.KillPipelineIfErr(fmt.Errorf("error adding output callback to Javascript VM: %s", err), killChan)
	}

	_, err = t.vm.Run(t.script)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Runtime error in JS transformation: %s", err), killChan)
	}
}

// Finish implements the ratchet.DataProcessor interface
func (t *JsTransform) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (t *JsTransform) String() string {
	return "JsTransform"
}
