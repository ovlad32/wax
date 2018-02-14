package appnode

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/nats-io/gnatsd/logger"
)


func (node *masterApplicationNodeType) makeCommandSubscription() (err error) {

	node.logger.Infof("Create MASTER command subscription '%v'", masterCommandSubject)


	node.commandSubscription, err = node.encodedConn.Subscribe(
		masterCommandSubject,
		node.commandSubscriptionFunc(),
	)

	err = node.encodedConn.Flush()
	if err != nil {
		err = errors.Wrapf(err, "Error while subscription being flushed")
		node.logger.Error(err)
	}

	if err = node.encodedConn.LastError(); err != nil {
		err = errors.Wrap(err, "error given via NATS while making Master command subscription")
		node.logger.Error(err)
	}


	node.logger.Info(
		"Master command subscription has been created",
	)
	return
}


func (node *masterApplicationNodeType) commandSubscriptionFunc() func(string, string, *CommandMessageType) {
	var err error
	return func(subj, reply string, msg *CommandMessageType) {
		node.logger.Infof("Master node command message: %v", msg.Command)
		if processor, found := node.commandProcessorsMap[msg.Command];found {
			err = processor(reply,msg)
			if err != nil {
				//TODO:
			}
		} else {
			panic(fmt.Sprintf("%v: cannot recognize incoming message command '%v' ", masterCommandSubject, msg.Command))
		}
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



func (node masterApplicationNodeType) parishOpenFunc() commandProcessorFuncType {
	return func(reply string, msg *CommandMessageType) (err error) {
		slaveCommandSubject := msg.ParamSubject(slaveCommandSubjectParam)
		if slaveCommandSubject.IsEmpty() {
			node.logger.Warn("gotten slave command subject is empty!")
			return
		}
		slaveId := msg.ParamNodeId(slaveIdParam)
		if slaveId.IsEmpty() {
			node.logger.Warn("gotten slaveId is empty!")
			return
		}

		node.logger.Infof("Start registering new Slave '%v' with command subject '%v'", slaveId, slaveCommandSubject)


		params := make([]CommandMessageParamEntryType,0)
		node.slaveCommandMux.RLock()
		if prev, found := node.slaveCommandSubjects[slaveId]; found {
			node.slaveCommandMux.RUnlock()
			params = append(params, CommandMessageParamEntryType{
				Name:ResubscribedParam,
				Value:true,
			});
			node.logger.Infof(
				"New Slave '%v' with command subject '%v' had been registered previously with '%v'",
				slaveId,
				slaveCommandSubject,
				prev,
			)
		} else {
			node.slaveCommandMux.RUnlock()
		}
		err = node.applicationNodeType.publishCommandResponse(reply, parishOpened, params...)
		if err != nil {
			err = errors.Wrapf(err, "could not reply of opening a new slave")
			return
		}
		node.slaveCommandMux.Lock()
		node.slaveCommandSubjects[slaveId] = slaveCommandSubject
		node.slaveCommandMux.Unlock()
		node.logger.Infof("A new node with command subject '%v' has been successfully registered", slaveCommandSubject)
		return
	}

}