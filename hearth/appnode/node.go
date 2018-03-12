package appnode

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/ovlad32/wax/hearth/appnode/worker"
	"github.com/ovlad32/wax/hearth/appnode/command"
)



type applicationNodeType struct {
	config        ApplicationNodeConfigType
	ctx           context.Context
	ctxCancelFunc context.CancelFunc

	encodedConn         *nats.EncodedConn
	commandSubscription *nats.Subscription
	//
	//
	logger                 *logrus.Logger
	commandFuncMap command.FuncMap
	//natsFuncMap map[command.Command]func(nats.Msg
}


func MasterCommandSubject() Subject{
	return masterCommandSubject
}


func SlaveCommandSubject(id Id) Subject{
	return Subject(fmt.Sprintf("COMMAND/%v", id))
}


func (node *applicationNodeType) Id() Id {
	return node.config.NodeId
}


func (node *applicationNodeType) connectToNATS() (err error) {
	conn, err := nats.Connect(
		node.config.NATSEndpoint,
		nats.Name(string(node.config.NodeId)),
	)
	if err != nil {
		err = errors.Wrapf(err, "could not connect to NATS at '%v'", node.config.NATSEndpoint)
		node.logger.Error(err)
		return
	}

	gob.Register(Subject(""))
	gob.Register(Id(""))
	gob.Register(worker.Id(("")))
	gob.Register(errors.New(""))
	gob.Register(fmt.Errorf(""))

	node.encodedConn, err = nats.NewEncodedConn(conn, nats.GOB_ENCODER)

	if err != nil {
		err = errors.Wrap(err, "could not create encoder for NATS")
		node.logger.Error(err)
		return
	}
	node.logger.Info("Node has been connected to NATS")
	return
}
/*
func (node applicationNodeType) CallCommandByNodeId(
	nodeId NodeIdType,
	command CommandType,
	entries ...*CommandMessageParamEntryType,
) (response *CommandMessageType, err error) {
	return node.RequestCommandBySubject(
		SlaveCommandSubject(nodeId),
		command,
		entries...
	)
}*/

func (node applicationNodeType) RequestCommandBySubject(
	subject Subject,
	command command.Command,
	entries... *command.ParamEntry,
) (response *command.Message, err error) {
	outgoingMessage := &command.Message{
		Command: command,
		Params:  make(command.ParamMap),
	}
	incomingMessage := new(command.Message)

	for _, e := range entries {
		if !e.Key.IsEmpty() {
			outgoingMessage.Params[e.Key] = e.Value
		}
	}

	err = node.encodedConn.Request(
		subject.String(),
		outgoingMessage,
		incomingMessage,
		nats.DefaultTimeout,
	)

	if err != nil {
		err = errors.Wrapf(err, "could not make command request %v ", command)
		node.logger.Error(err)
		return
	}

	err = node.encodedConn.Flush()
	if err != nil {
		err = errors.Wrapf(err, "could not flush command request %v ", command)
		node.logger.Error(err)
		return
	}

	if err = node.encodedConn.LastError(); err != nil {
		err = errors.Wrapf(err, "error in NATS while wiring command request %v ", command)
		node.logger.Error(err)
		return
	}

	if incomingMessage == nil {
		err = errors.Errorf("%v-command response is not initialized", command)
		return
	}
	if incomingMessage.Command != command {
		err = errors.Wrapf(err, "%-command response command %v is not expected. Expected: %v", command, incomingMessage.Command,command)
		return
	}
	if incomingMessage.Err != nil {
		err = errors.Errorf("%v-command response with error: %v", command, incomingMessage.Err)
		return
	}
	response = incomingMessage
	return
}

func (node applicationNodeType) PublishCommandResponse(
	subject string,
	command command.Command,
	entries... *command.ParamEntry,
) (err error) {

	response := &command.Message{
		Command:command,
		Params:make(command.ParamMap),
	}

	for _, e := range entries {
	//	fmt.Printf("Key:%v/Value:%v",e.Key,e.Value)
		if !e.Key.IsEmpty() {
			if e.Key == errorParam {
				response.Err = e.Value.(error)
			} else {
				response.Params[e.Key] = e.Value
			}
		}
	}
	err = node.encodedConn.Publish(subject, response)
	if err != nil {
		err = errors.Wrapf(err, "could not publish '%v' response to subject %v ",command,subject)
		return
	}
	return
}

func (node applicationNodeType) PublishCommand(
	subject Subject,
	command command.Command,
	entries... *command.ParamEntry,
) (err error) {

	response := &command.Message{
		Command: command,
		Params:make(command.ParamMap),
	}
	for _, e := range entries {
		if !e.Key.IsEmpty() {
			response.Params[e.Key] = e.Value
		}
	}
	err = node.encodedConn.Publish(subject.String(), response)
	if err != nil {
		err = errors.Wrapf(err, "could not publish '%v' response ", command)
		return
	}

	err = node.encodedConn.Flush()
	if err != nil {
		err = errors.Wrapf(err, "could not flush published command %v ", command)
		node.logger.Error(err)
		return
	}

	if err = node.encodedConn.LastError(); err != nil {
		err = errors.Wrapf(err, "error in NATS while wiring published command %v ", command)
		node.logger.Error(err)
		return
	}
	return
}



func (node applicationNodeType) Subscribe(
	subject Subject,
	processor command.Func,
) (result *nats.Subscription, err error) {

	subscription, err := node.encodedConn.Subscribe(
		subject.String(),
		processor,
	)

	if err != nil {
		err = errors.Wrapf(err,"could not create subscription for subject: %v ",subject)
		node.logger.Error(err)
		return
	}
	err = node.encodedConn.Flush()
	if err != nil {
		err = errors.Wrapf(err,"could not flush created subscription for subject: %v",subject)
		node.logger.Error(err)
		return
	}

	if err = node.encodedConn.LastError(); err != nil {
		err = errors.Wrapf(err,"error in NATS while wiring flushed subscription: %v",subject)
		node.logger.Error(err)
		return
	}
	result = subscription
	return
}
