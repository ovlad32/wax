package appnode

import (
	"github.com/pkg/errors"
	"github.com/ovlad32/wax/hearth/appnode/command"
)


func (node *SlaveNode) makeCommandSubscription() (err error) {

	slaveSubject := SlaveCommandSubject(node.Id())
	node.logger.Infof("Creating Slave '%v' command subscription '%v'...", node.Id(), slaveSubject)
	node.commandSubscription, err = node.encodedConn.Subscribe(
		string(slaveSubject),
		node.commandSubscriptionFunc(),
	)
	if err != nil {
		err = errors.Wrapf(err, "could not create slave command subject subscription for Slave '%v' ", node.NodeId())
		node.logger.Error(err)
		return
	}
	err = node.encodedConn.Flush()
	if err != nil {
		err = errors.Wrapf(err, "could not flush slave command subject subscription for Slave '%v'", node.Id())
		node.logger.Error(err)
		return
	}

	if err = node.encodedConn.LastError(); err != nil {
		err = errors.Wrapf(err, "error given via NATS while propagating slave command subject subscription for Slave '%v'", node.Id())
		node.logger.Error(err)
		return
	}

	if err = func() (err error) {
		response, err := node.RequestCommandBySubject(
			MasterCommandSubject(),
			parishOpen,
			command.NewParams(2).
				Append(slaveCommandSubjectParam,slaveSubject).
				Append(slaveIdParam,node.Id())...

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
			err = errors.Wrapf(err, "could not inform master about Slave '%v' command subject creation", node.Id())
			node.logger.Error(err)
			return
		}
		if response.Command != parishOpen {
			err = errors.Errorf("unexpected response command '%v', got '%v'",parishOpen,response.Command)
			node.logger.Fatal("Slave '%v' registration has been done", node.Id())
		}
		node.logger.Infof("Slave '%v' registration has been done", node.Id())
		if response.ParamBool(slaveResubscribedParam, false) {
			node.logger.Warnf("Slave '%v' command subscription had been created before...", node.Id())
			//Todo: clean
		}

		return
	}(); err != nil {
		node.commandSubscription.Unsubscribe()
		node.commandSubscription = nil
	} else {
		node.logger.Infof("Slave '%v' command subscription has been created", node.Id())
	}
	return
}

func (node *SlaveNode) commandSubscriptionFunc() func(string, string, *command.Message) {
	var err error
	return func(subject, replySubject string, msg *command.Message) {
		node.logger.Infof("Slave '%v' node command command: %v", node.Id(), msg.Command)
		if processor, found := node.commandFuncMap[msg.Command]; found {
			err = processor(subject,replySubject, msg)
			if err != nil {
				//TODO:
			}
		} else {
			err = errors.Errorf("Slave '%v' cannot recognize incoming command command '%v'", node.Id(), msg.Command)
			node.logger.Fatal(err)
		}
	}
}



func (node *SlaveNode) registerCommandProcessors() (err error){
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



