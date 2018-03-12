package appnode

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/ovlad32/wax/hearth/appnode/command"
	"github.com/ovlad32/wax/hearth/appnode/parish"
)


func (node *MasterNode) makeCommandSubscription() (err error) {
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

func (node *MasterNode) commandSubscriptionFunc() command.Func {
	return func(subject,replySubject string, incomingMessage *command.Message) (err error) {
		node.logger.Infof("Master node command command: %v", incomingMessage.Command)
		if processor, found := node.commandFuncMap[incomingMessage.Command];found {
			err = processor(subject,replySubject,incomingMessage)
			if err != nil {
				//TODO:
			}
		} else {
			panic(fmt.Sprintf("%v: cannot recognize incoming command command '%v' ",node.Id(),incomingMessage.Command))
		}
		return
	}
}

func (node *MasterNode) closeAllCommandSubscription() (err error) {

	node.slaveCommandMux.Lock()
	defer node.slaveCommandMux.Unlock()
	for _, subj:= range node.slaveCommandSubjects {
		err = node.PublishCommand(
			subj,
			parish.Open,
		)
		//if err != nil {
		//	node.logger.Error(err)
		//}

	}
	node.slaveCommandSubjects = make(map[Id]Subject)
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
