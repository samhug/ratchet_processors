package ratchet_processors

import (
	"context"
	"fmt"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/util"
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
func (t *JsTransform) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	objs, err := data.ObjectsFromJSON(d)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Unable to unmarshal JSON object for JS transformation: %s", err), killChan, ctx)
	}

	if err := t.vm.Set("data", objs); err != nil {
		util.KillPipelineIfErr(fmt.Errorf("error inserting data into JS VM: %s", err), killChan, ctx)
	}

	// Register output callback function with the VM
	if err := t.vm.Set("output", func(call otto.FunctionCall) otto.Value {

		if len(call.ArgumentList) != 1 {
			util.KillPipelineIfErr(fmt.Errorf("Javascript VM error: 'output' function takes 1 argument"), killChan, ctx)
		}

		obj, err := call.ArgumentList[0].Export()
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("Error exporting JS value: %s", err), killChan, ctx)
		}

		d, err = data.NewJSON(obj)
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("Unable to marshal JS transformed object: %s", err), killChan, ctx)
		}

		outputChan <- d

		return otto.NullValue()
	}); err != nil {
		util.KillPipelineIfErr(fmt.Errorf("error adding output callback to Javascript VM: %s", err), killChan, ctx)
	}

	_, err = t.vm.Run(t.script)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Runtime error in JS transformation: %s", err), killChan, ctx)
	}
}

// Finish implements the ratchet.DataProcessor interface
func (t *JsTransform) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {
}

func (t *JsTransform) String() string {
	return "JsTransform"
}
