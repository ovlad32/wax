package appnode

import (
	"github.com/pkg/errors"
	"os"
	"os/signal"
	"sync"
	"net/http"
	"github.com/gorilla/mux"
	"fmt"
	"time"
	"encoding/json"
	"context"
)



type slaveApplicationNodeType struct {
	*applicationNodeType
	//
	payloadSizeAdjustments map[CommandType]int64
	workerMux              sync.RWMutex
	workers                map[WorkerIdType]WorkerInterface
}


func (node *slaveApplicationNodeType) startServices() (err error) {

	err = node.registerCommandProcessors()

	if err != nil {
		err = errors.Wrapf(err, "could register command processors")
		return err
	}

	err = node.initNATSService()


	if err != nil {
		err = errors.Wrapf(err, "could not start NATS service")
		return err
	}

	server, err := node.initRestApiRouting()
	if err != nil {
		return err
	}

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt, os.Kill)
	go func() {
		_ = <-osSignal

		if err = server.Shutdown(context.Background()); err != nil {
			err = errors.Wrapf(err, "could not shutdown REST server")
			node.logger.Error(err)
		}

		//err = node.closeAllCommandSubscription()
		if err != nil {
			err = errors.Wrapf(err, "could not close command subscriptions")
			node.logger.Error(err)
		}
		node.logger.Warnf("Slave '%v' shut down", node.NodeId())
		os.Exit(0)
	}()
	return
}

func (node *slaveApplicationNodeType) registerMaxPayloadSize(maxLoadedMsg *CommandMessageType) (adjustment int64, err error) {
	subject := "a subject length does not matter now, so make it arbitrary size but long enough"
	encoded, err := node.encodedConn.Enc.Encode(subject, maxLoadedMsg)
	if err != nil {
		err = errors.Wrapf(err, "could not encode command message %v", maxLoadedMsg.Command)
		return
	}
	adjustment = node.encodedConn.Conn.MaxPayload() - int64(len(encoded))
	node.payloadSizeAdjustments[maxLoadedMsg.Command] = adjustment
	return
}

func (node *slaveApplicationNodeType) registerCommandProcessors() (err error){
	//node.commandProcessorsMap[parishClose] = node.parishCloseFunc()
	node.commandProcessorsMap[parishStopWorker] = node.parishStopWorkerFunc()

	node.commandProcessorsMap[fileStats] = node.fileStatsProcessorFunc()

	node.commandProcessorsMap[copyFileDataSubscribe] = node.copyFileDataSubscriptionProcessorFunc()
	node.commandProcessorsMap[copyFileLaunch] = node.copyFileLaunchProcessorFunc()

	//node.commandProcessorsMap[categorySplitOpen] = node.categorySplitOpenFunc()
	//node.commandProcessorsMap[categorySplitClose] = node.categorySplitCloseFunc()
	return
}

func (node *slaveApplicationNodeType) workersHandlerFunc() func (http.ResponseWriter,*http.Request)  {
	return func (w http.ResponseWriter,r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		workerList := make([]map[string]interface{},0,len(node.workers))
		for _,worker := range node.workers {
			workerList = append(workerList, worker.JSONMap())
		}
		je := json.NewEncoder(w)
		err := je.Encode(workerList)
		if err!= nil {
			http.Error(w,err.Error(),http.StatusOK)
			return
		}
		return
	}
}



func (node *slaveApplicationNodeType) initRestApiRouting() (srv *http.Server, err error) {

	r := mux.NewRouter()

	r.HandleFunc("/workers/", node.workersHandlerFunc()).Methods("GET")
	//r.HandleFunc("/table/categorysplit", node.CategorySplitHandlerFunc()).Methods("POST")
	//r.HandleFunc("/util/copyfile", node.copyFileHandlerFunc()).Methods("POST")

	//defer node.wg.Done()
	address := fmt.Sprintf(":%d", node.config.RestAPIPort)
	srv = &http.Server{
		Handler: r,
		Addr:    address,
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}
	node.logger.Infof("REST API server has started at %v....", address)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			if err == http.ErrServerClosed {
				node.logger.Warn("REST API server closed")
				return
			}
			err = errors.Wrapf(err, "REST API server broke at %v: %v", address)
			node.logger.Fatal(err)
		}
	}()

	return
}


/*
func (node slaveApplicationNodeType) reportError(
		command CommandType,
		incomingErr error,
		entries... *CommandMessageParamEntryType,
) (err error) {

	errMsg := &CommandMessageType{
		Command: command,
		Err:     incomingErr,
	}

	errMsg.Params = CommandMessageParamMap{
		slaveCommandSubjectParam:SubjectType(node.commandSubscription.Subject),
	}

	for _,e := range entries {
		errMsg.Params[e.Name] = e.Value
	}

	err = node.encodedConn.Publish(masterCommandSubject, errMsg)
	if err != nil {
		err = errors.Wrapf(err, "could not publish error message")
		return
	}
	if err = node.encodedConn.Flush(); err != nil {
		err = errors.Wrapf(err, "could not flush published error message")
		return
	}
	if err = node.encodedConn.LastError(); err != nil {
		err = errors.Wrapf(err, "could not wire published error message")
		return
	}
	return
}

*/