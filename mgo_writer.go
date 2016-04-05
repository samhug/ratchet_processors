package ratchet_processors

import (
	"fmt"
	"gopkg.in/mgo.v2"

	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

type MgoWriter struct {
	session *mgo.Session
	bulk    *mgo.Bulk
}

type MgoConfig struct {
	Server     string
	Db         string
	Collection string
}

func NewMgoWriter(config *MgoConfig) (*MgoWriter, error) {

	session, err := mgo.Dial(config.Server)
	if err != nil {
		return nil, fmt.Errorf("Unable to connect to db: %s", err)
	}

	collection := session.DB(config.Db).C(config.Collection)
	bulk := collection.Bulk()

	loader := &MgoWriter{
		session: session,
		bulk:    bulk,
	}

	return loader, nil
}

func (w *MgoWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {

	// use util helper to convert Data into []map[string]interface{}
	objects, err := data.ObjectsFromJSON(d)
	util.KillPipelineIfErr(err, killChan)

	for _, o := range objects {
		w.bulk.Insert(o)
	}
}

func (w *MgoWriter) Finish(outputChan chan data.JSON, killChan chan error) {
	_, err := w.bulk.Run()
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Error running bulk insert: %s", err), killChan)
	}
}

func (w *MgoWriter) String() string {
	return "MgoWriter"
}
