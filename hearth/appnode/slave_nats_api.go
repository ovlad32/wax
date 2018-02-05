package appnode

import (
	"os"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
)

const (
	slaveCommandSubjectParam  CommandMessageParamType = "slaveCommandSubject"
	ResubscribedParam CommandMessageParamType = "resubscribed"
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


func (node slaveApplicationNodeType) commandSubject() string {
	return fmt.Sprintf("COMMAND/%v", node.config.NodeName)
}

func (node *slaveApplicationNodeType) makeCommandSubscription() (err error) {

	slaveSubject := node.commandSubject()
	node.logger.Infof("Creating Slave '%v' command subscription '%v'...",node.NodeId(),slaveSubject)
	node.commandSubscription, err = node.encodedConn.Subscribe(
		slaveSubject,
		node.commandSubscriptionFunc(),
	)
	if err != nil {
		err = errors.Wrapf(err,"could not create slave command subject subscription for Slave '%v' ",node.NodeId())
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
			MASTER_COMMAND_SUBJECT,
			&CommandMessageType{
				Command: PARISH_OPEN,
				Params: map[CommandMessageParamType]interface{}{
					slaveCommandSubjectParam: node.commandSubscription.Subject,
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
			err = errors.Wrapf(err, "could not flush Slave '%v' registration request",node.NodeId())
			node.logger.Error(err)
			return
		}

		if err = node.encodedConn.LastError(); err != nil {
			err = errors.Wrapf(err, "error given via NATS while making Slave '%v' registration of command subscription",node.NodeId())
			node.logger.Error(err)
			return
		}

		node.logger.Infof("Slave '%v' registration has been done",node.NodeId())
		//if response.Command == PARISH_OPENED
		//TODO:  PARISH_OPENED
		if response.ParamBool(ResubscribedParam,false)  {
			node.logger.Warnf("Slave '%v' command subscription had been created before...",node.NodeId())
			//Todo: clean
		}

		return
	} (); err != nil {
		node.commandSubscription.Unsubscribe()
		node.commandSubscription = nil
	} else {
		node.logger.Infof("Slave '%v' command subscription has been created",node.NodeId())
	}
	return
}


func (node *slaveApplicationNodeType) commandSubscriptionFunc() func(string, string, *CommandMessageType) {
	return func(subj, reply string, msg *CommandMessageType) {
		switch msg.Command {
		case PARISH_CLOSE:
			node.logger.Warnf("Signal of closing Slave '%v' command subscription has been received",node.NodeId())
			if	node.commandSubscription != nil {
				err := node.commandSubscription.Unsubscribe()
				if err != nil {
					node.logger.Error(err)
				}
			}
			node.encodedConn.Publish(
				reply,
				&CommandMessageType{
					Command:PARISH_CLOSED,
				})
			/*err := node.CloseRegularWorker(
				reply,
				msg,
				PARISH_CLOSED,
			)
			if err != nil {
				panic(err.Error())
			}*/
			node.encodedConn.Close()
			os.Exit(0)
		case CATEGORY_SPLIT_OPEN:
			worker, err := newCategorySplitWorker(
				node.encodedConn,
				reply, msg,
				node.logger,
			)
			if err != nil {
				panic(err.Error())
			}
			node.AppendWorker(worker)
		case CATEGORY_SPLIT_CLOSE:
			err := node.CloseRegularWorker(reply,msg,CATEGORY_SPLIT_CLOSED)
			if err != nil {
				panic(err.Error())
			}
		default:
			panic(msg.Command)
		}
	}
}
