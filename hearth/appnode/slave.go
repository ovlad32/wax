package appnode

import (
	"github.com/pkg/errors"
	"os"
	"os/signal"
	"sync"
)



type slaveApplicationNodeType struct {
	*applicationNodeType
	//
	payloadSizeAdjustments map[CommandType]int64
	workerMux              sync.RWMutex
	workers                map[SubjectType]WorkerInterface
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


	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt, os.Kill)
	go func() {
		_ = <-osSignal

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
	node.commandProcessorsMap[parishClose] = node.parishCloseFunc()
	node.commandProcessorsMap[parishStopWorker] = node.parishStopWorkerFunc()
	node.commandProcessorsMap[copyFileOpen] = node.copyFileOpenFunc()
	node.commandProcessorsMap[categorySplitOpen] = node.categorySplitOpenFunc()
	node.commandProcessorsMap[categorySplitClose] = node.categorySplitCloseFunc()
	return
}

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

