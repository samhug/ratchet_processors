package ratchet_processors

import (
	"fmt"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
	"github.com/samuelhug/otto"
)

type JSConfig struct {
	InitScript string
	MainScript string
}

type JsTransform struct {
	vm     *otto.Otto
	script string
}

func NewJsTransform(script string) *JsTransform {
	return &JsTransform{
		vm:     otto.New(),
		script: script,
	}
}

func (t *JsTransform) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	objs, err := data.ObjectsFromJSON(d)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Unable to unmarshal JSON object for JS transformation: %s", err), killChan)
	}

	var newObjs []map[string]interface{}

	for _, obj := range objs {

		t.vm.Set("obj", obj)

		_, err := t.vm.Run(t.script)
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("Runtime error in JS transformation: %s", err), killChan)
		}

		val, err := t.vm.Get("obj")
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("Error executing JS transformation: %s", err), killChan)
		}

		newObj, err := val.Export()
		if err != nil {
			util.KillPipelineIfErr(fmt.Errorf("Error exporting JS value: %s", err), killChan)
		}

		newObjs = append(newObjs, newObj.(map[string]interface{}))
	}

	d, err = data.NewJSON(newObjs)
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
