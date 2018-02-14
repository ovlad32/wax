package appnode

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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
	commandInterceptorsMap commandInterceptorsMapType
}

func (node *applicationNodeType) NodeId() NodeIdType {
	return node.config.NodeId
}

func (node applicationNodeType) commandSubject(id NodeIdType) SubjectType {
	return SubjectType(fmt.Sprintf("COMMAND/%v", id))
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

	gob.Register(SubjectType(""))
	gob.Register(NodeIdType(""))

	node.encodedConn, err = nats.NewEncodedConn(conn, nats.GOB_ENCODER)

	if err != nil {
		err = errors.Wrap(err, "could not create encoder for NATS")
		node.logger.Error(err)
		return
	}
	node.logger.Info("Node has been connected to NATS")
	return
}

func (node applicationNodeType) CallCommandByNodeId(
	nodeId NodeIdType,
	command CommandType,
	entries ...CommandMessageParamEntryType,
) (response *CommandMessageType, err error) {
	return node.CallCommandBySubject(
		node.commandSubject(nodeId),
		command,
		entries...
	)
}

func (node applicationNodeType) CallCommandBySubject(
	subject SubjectType,
	command CommandType,
	entries ...CommandMessageParamEntryType,
) (response *CommandMessageType, err error) {
	outgoingMessage := &CommandMessageType{
		Command: command,
		Params:  make(CommandMessageParamMap),
	}
	incomingMessage := new(CommandMessageType)

	for _, e := range entries {
		if !e.Name.IsEmpty() {
			outgoingMessage.Params[e.Name] = e.Value
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
		err = errors.Wrapf(err, "error given via NATS while wiring command request %v ", command)
		node.logger.Error(err)
		return
	}
	response = incomingMessage
	return
}

func (node applicationNodeType) publishCommandResponse(
	subject string,
	command CommandType,
	entries ...CommandMessageParamEntryType,) (err error) {

	response := &CommandMessageType{
		Command:command,
	}
	for _, e := range entries {
		if !e.Name.IsEmpty() {
			response.Params[e.Name] = e.Value
		}
	}
	err = node.encodedConn.Publish(subject, response)
	if err != nil {
		err = errors.Wrapf(err, "could not publish '%v' response ",command)
		return
	}
	return
}
