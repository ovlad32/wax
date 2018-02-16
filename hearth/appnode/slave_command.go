package appnode

import (
	"github.com/pkg/errors"
)


func (node *slaveApplicationNodeType) makeCommandSubscription() (err error) {

	slaveSubject := SlaveCommandSubject(node.NodeId())
	node.logger.Infof("Creating Slave '%v' command subscription '%v'...", node.NodeId(), slaveSubject)
	node.commandSubscription, err = node.encodedConn.Subscribe(
		string(slaveSubject),
		node.commandSubscriptionFunc(),
	)
	if err != nil {
		err = errors.Wrapf(err, "could not create slave command subject subscription for Slave '%v' ", node.NodeId())
		node.logger.Error(err)
		return
	}

	if err = func() (err error) {

		err = node.encodedConn.Flush()
		if err != nil {
			err = errors.Wrapf(err, "could not flush slave command subject subscription for Slave '%v'", node.NodeId())
			node.logger.Error(err)
			return
		}

		if err = node.encodedConn.LastError(); err != nil {
			err = errors.Wrapf(err, "error given via NATS while propagating slave command subject subscription for Slave '%v'", node.NodeId())
			node.logger.Error(err)
			return
		}
		response, err := node.CallCommandBySubject(
			masterCommandSubject,
			parishOpen,
			NewCommandMessageParams(2).
				Append(slaveCommandSubjectParam,slaveSubject).
				Append(slaveIdParam,node.NodeId())...

		)

		/*
		&CommandMessageParamEntryType{
				Name:  slaveCommandSubjectParam,
				Value: slaveSubject,
			},
			&CommandMessageParamEntryType{
				Name:  slaveIdParam,
				Value: node.NodeId(),
			},
		*/
		if err != nil {
			err = errors.Wrapf(err, "could not inform master about Slave '%v' command subject creation", node.NodeId())
			node.logger.Error(err)
			return
		}
		node.logger.Infof("Slave '%v' registration has been done", node.NodeId())
		//if response.Command == parishOpened
		//TODO:  parishOpened
		if response.ParamBool(slaveResubscribedParam, false) {
			node.logger.Warnf("Slave '%v' command subscription had been created before...", node.NodeId())
			//Todo: clean
		}

		return
	}(); err != nil {
		node.commandSubscription.Unsubscribe()
		node.commandSubscription = nil
	} else {
		node.logger.Infof("Slave '%v' command subscription has been created", node.NodeId())
	}
	return
}

func (node *slaveApplicationNodeType) commandSubscriptionFunc() func(string, string, *CommandMessageType) {
	var err error
	return func(subj, reply string, msg *CommandMessageType) {
		node.logger.Infof("Slave '%v' node command message: %v", node.NodeId(), msg.Command)
		if processor, found := node.commandProcessorsMap[msg.Command]; found {
			err = processor(reply, msg)
			if err != nil {
				//TODO:
			}
		} else {
			err = errors.Errorf("Slave '%v' cannot recognize incoming message command '%v'", node.NodeId(), msg.Command)
			node.logger.Fatal()
		}
	}
}





