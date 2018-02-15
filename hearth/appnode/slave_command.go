package appnode

import (
	"github.com/pkg/errors"
	"os"
)


func (node *slaveApplicationNodeType) makeCommandSubscription() (err error) {

	slaveSubject := node.commandSubject(node.NodeId())
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
			&CommandMessageParamEntryType{
				Name:  slaveCommandSubjectParam,
				Value: slaveSubject,
			},
			&CommandMessageParamEntryType{
				Name:  slaveIdParam,
				Value: node.NodeId(),
			},
		)

		if err != nil {
			err = errors.Wrapf(err, "could not inform master about Slave '%v' command subject creation", node.NodeId())
			node.logger.Error(err)
			return
		}
		node.logger.Infof("Slave '%v' registration has been done", node.NodeId())
		//if response.Command == parishOpened
		//TODO:  parishOpened
		if response.ParamBool(ResubscribedParam, false) {
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

func (node slaveApplicationNodeType) parishCloseFunc() commandProcessorFuncType {
	return func(reply string, msg *CommandMessageType) (err error) {
		node.logger.Warnf("Signal of closing Slave '%v' command subscription has been received", node.NodeId())
		if node.commandSubscription != nil {
			err := node.commandSubscription.Unsubscribe()
			if err != nil {
				node.logger.Error(err)
			}
		}
		node.encodedConn.Publish(
			reply,
			&CommandMessageType{
				Command: parishClosed,
			})
		/*err := node.CloseRegularWorker(
			reply,
			msg,
			parishClosed,
		)
		if err != nil {
			panic(err.Error())
		}*/
		node.encodedConn.Close()
		os.Exit(0)
		return
	}
}



func (node slaveApplicationNodeType) parishStopWorkerFunc() commandProcessorFuncType {
	return func(reply string, msg *CommandMessageType) (err error) {
		//node.logger.Warnf("Signal Slave '%v' command subscription has been received", node.NodeId())

		//of stopping worker on

		subject := msg.ParamSubject(workerSubjectParam);
		worker := node.FindWorkerBySubject(subject)
		if worker == nil {

		} else {
			worker.CancelCurrentJob();
		}

		if node.commandSubscription != nil {
			err := node.commandSubscription.Unsubscribe()
			if err != nil {
				node.logger.Error(err)
			}
		}
		node.encodedConn.Publish(
			reply,
			&CommandMessageType{
				Command: parishClosed,
			})
		/*err := node.CloseRegularWorker(
			reply,
			msg,
			parishClosed,
		)
		if err != nil {
			panic(err.Error())
		}*/
		node.encodedConn.Close()
		os.Exit(0)
		return
	}
}




