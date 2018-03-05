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
		response, err := node.RequestCommandBySubject(
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
		if response.Command != parishOpen {
			err = errors.Errorf("unexpected response command '%v', got '%v'",parishOpen,response.Command)
			node.logger.Fatal("Slave '%v' registration has been done", node.NodeId())
		}
		node.logger.Infof("Slave '%v' registration has been done", node.NodeId())
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
	return func(subject, replySubject string, msg *CommandMessageType) {
		node.logger.Infof("Slave '%v' node command message: %v", node.NodeId(), msg.Command)
		if processor, found := node.commandFuncMap[msg.Command]; found {
			err = processor(subject,replySubject, msg)
			if err != nil {
				//TODO:
			}
		} else {
			err = errors.Errorf("Slave '%v' cannot recognize incoming message command '%v'", node.NodeId(), msg.Command)
			node.logger.Fatal(err)
		}
	}
}



func (node *slaveApplicationNodeType) registerCommandProcessors() (err error){
	//node.commandProcessorsMap[parishClose] = node.parishCloseFunc()
	node.commandFuncMap[parishStopWorker] = node.parishStopWorkerFunc()

	node.commandFuncMap[fileStats] = node.fileStatsCommandFunc()

	node.commandFuncMap[copyFileOpen] = node.copyFileOpenCommandFunc()
	node.commandFuncMap[copyFileCreate] = node.copyFileCreateCommandFunc()
	node.commandFuncMap[copyFileLaunch] = node.copyFileLaunchCommandFunc()
	node.commandFuncMap[copyFileTerminate] = node.copyFileTerminateCommandFunc()

	//node.commandProcessorsMap[categorySplitOpen] = node.categorySplitOpenFunc()
	//node.commandProcessorsMap[categorySplitClose] = node.categorySplitCloseFunc()
	return
}



