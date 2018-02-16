package appnode

import (
	"fmt"
	"github.com/pkg/errors"
)


func (node *masterApplicationNodeType) makeCommandSubscription() (err error) {
	masterSubject := MasterCommandSubject()
	node.logger.Infof("Create MASTER command subscription '%v'", masterSubject )

	node.commandSubscription, err = node.Subscribe(
		masterSubject,
		node.commandSubscriptionFunc(),
		)
	if err != nil {
		err = errors.Wrapf(err, "could not create Master subscription")
		node.logger.Error(err)
	} else {
		node.logger.Info(
			"Master command subscription has been created",
		)
	}
	return
}

func (node *masterApplicationNodeType) commandSubscriptionFunc() commandProcessorFuncType {
	return func(replySubject string, incomingMessage *CommandMessageType) (err error) {
		node.logger.Infof("Master node command message: %v", incomingMessage.Command)
		if processor, found := node.commandProcessorsMap[incomingMessage.Command];found {
			err = processor(replySubject,incomingMessage)
			if err != nil {
				//TODO:
			}
		} else {
			panic(fmt.Sprintf("%v: cannot recognize incoming message command '%v' ",incomingMessage.Command))
		}
		return
	}
}

func (node *masterApplicationNodeType) closeAllCommandSubscription() (err error) {

	node.slaveCommandMux.Lock()
	defer node.slaveCommandMux.Unlock()
	for _, subj:= range node.slaveCommandSubjects {
		response, err := node.CallCommandBySubject(
			subj,
			parishClose,
		)

		if err != nil {
			node.logger.Error(err)
		}

		if response.Command != parishClosed {
			err = errors.Errorf("Expected command '%v' got '%v'",parishClosed,response.Command)
			node.logger.Error(err)
		}
	}
	node.slaveCommandSubjects = make(map[NodeIdType]SubjectType)
	node.logger.Warnf("Slave command subscriptions have been closed")

	err = node.commandSubscription.Unsubscribe()
	if err != nil {
		err = errors.Wrapf(err,"Could not unsubscribe from command subject")
		node.logger.Error(err)
	} else {
		node.logger.Warnf("MASTER command subscription has been closed")
	}
	return
}
