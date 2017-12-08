package ratchet_processors

import (
	"fmt"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
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
	vm.Set("logWrite", func(call otto.FunctionCall) otto.Value {
		fmt.Printf("JS_LOG: %s\n", call.Argument(0).String())
		return otto.Value{}
	})

	vmScript, err := vm.Compile("", script)
	if err != nil {
		return nil, fmt.Errorf("Error compiling script: %s", err)
	}

	return &JsTransform{
		vm:     vm,
		script: vmScript,
	}, nil
}

func (t *JsTransform) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	objs, err := data.ObjectsFromJSON(d)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Unable to unmarshal JSON object for JS transformation: %s", err), killChan)
	}

	t.vm.Set("data", objs)

	_, err = t.vm.Run(t.script)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Runtime error in JS transformation: %s", err), killChan)
	}

	val, err := t.vm.Get("data")
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Error executing JS transformation: %s", err), killChan)
	}

	newObj, err := val.Export()
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Error exporting JS value: %s", err), killChan)
	}

	//d, err = data.NewJSON(newObjs)
	d, err = data.NewJSON(newObj)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Unable to marshal JS transformed object: %s", err), killChan)
	}

	outputChan <- d
}

func (t *JsTransform) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (t *JsTransform) String() string {
	return "JsTransform"
}
