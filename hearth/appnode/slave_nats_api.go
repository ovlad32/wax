package appnode

import (
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"os"
)

const (
	slaveCommandSubjectParam CommandMessageParamType = "slaveCommandSubject"
	slaveIdParam             CommandMessageParamType = "slaveId"
	ResubscribedParam        CommandMessageParamType = "resubscribed"
)

func (node *slaveApplicationNodeType) initNatsService() (err error) {

	err = node.connectToNATS()
	if err != nil {
		return err
	}

	err = node.makeCommandSubscription()
	if err != nil {
		return err
	}
	return
}

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

		response := new(CommandMessageType)
		err = node.encodedConn.Request(
			masterCommandSubject,
			&CommandMessageType{
				Command: parishOpen,
				Params: CommandMessageParamMap{
					slaveCommandSubjectParam: node.commandSubscription.Subject,
					slaveIdParam:             node.NodeId(),
				},
			},
			response,
			nats.DefaultTimeout,
		)

		if err != nil {
			err = errors.Wrapf(err, "could not inform master about Slave '%v' command subject creation", node.NodeId())
			node.logger.Error(err)
			return
		}

		err = node.encodedConn.Flush()
		if err != nil {
			err = errors.Wrapf(err, "could not flush Slave '%v' registration request", node.NodeId())
			node.logger.Error(err)
			return
		}

		if err = node.encodedConn.LastError(); err != nil {
			err = errors.Wrapf(err, "error given via NATS while making Slave '%v' registration of command subscription", node.NodeId())
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
	return func(subj, reply string, msg *CommandMessageType) {
		node.logger.Infof("Slave %v got command message: %v", node.NodeId(), msg.Command)
		switch msg.Command {
		case parishClose:
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
		case copyFileOpen:
			worker, err := newCopyFileWorker(
				node.encodedConn,
				reply, msg,
				node.logger,
			)
			if err != nil {
				panic(err.Error())
			}
			node.AppendWorker(worker)

		case categorySplitOpen:
			worker, err := newCategorySplitWorker(
				node.encodedConn,
				reply, msg,
				node.logger,
			)
			if err != nil {
				panic(err.Error())
			}
			node.AppendWorker(worker)
		case categorySplitClose:
			err := node.CloseRegularWorker(reply, msg, categorySplitClosed)
			if err != nil {
				panic(err.Error())
			}
		default:
			panic(msg.Command)
		}
	}
}
