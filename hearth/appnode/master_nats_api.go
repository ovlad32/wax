package appnode

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/nats-io/go-nats"
)

func (node *masterApplicationNodeType) initNatsService() (err error) {

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


func (node *masterApplicationNodeType) makeCommandSubscription() (err error) {

	node.logger.Infof("Create MASTER command subscription '%v'",MASTER_COMMAND_SUBJECT)

	node.commandSubscription,err = node.encodedConn.Subscribe(
		MASTER_COMMAND_SUBJECT,
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
	return func(subj, reply string, msg *CommandMessageType) {
		switch msg.Command {
		case PARISH_OPEN:

			commandSubject := msg.ParamString(slaveCommandSubjectParam,"")
			if commandSubject == "" {
				node.logger.Warn("gotten command subject is empty!")
				return
			} else {
				node.logger.Infof("Start registering a new node with command subject '%v'",commandSubject)
			}
			response := &CommandMessageType{}
			response.Command = PARISH_OPENED

			prev := node.FindWorkerById(commandSubject)

			if prev != nil {
				response.Params = map[CommandMessageParamType]interface{} {
					ResubscribedParam:true,
				}
				node.logger.Infof("A new node with command subject '%v' had been registered previously",commandSubject)
			} else {
				node.slaveCommandSubjects = append(node.slaveCommandSubjects, commandSubject)
			}
			worker := basicWorker{
				id:commandSubject,
			}
			node.AppendWorker(worker)
			err := node.encodedConn.Publish(reply,response)
			if err != nil {
				err = errors.Wrapf(err,"could not reply of opening a new slave")
				return
			}
			node.logger.Infof("A new node with command subject '%v' has been successfully registered",commandSubject)
		default:
			panic (fmt.Sprintf("%v: cannot recognize incoming message command '%v' ",MASTER_COMMAND_SUBJECT,msg.Command))
		}
	}
}

func (node *masterApplicationNodeType) closeAllCommandSubscription()  (err error) {

	for index, subj := range node.slaveCommandSubjects {
		response := new(CommandMessageType)
		err = node.encodedConn.Request(subj,
			&CommandMessageType{
				Command:PARISH_CLOSE,
			},
			response,
			nats.DefaultTimeout,
		)
		if err!=nil {
			node.logger.Error(err)
		}
		if response.Command != PARISH_CLOSED {
			node.logger.Error(response.Command)
		}
		//worker := node.FindWorkerById(subj)

		node.RemoveWorkerById(subj)
		node.slaveCommandSubjects[index] = ""
	}
	node.logger.Warnf("Slave command subscriptions have been closed")

	err = node.commandSubscription.Unsubscribe()
	if err != nil {

	} else{
		node.logger.Warnf("MASTER command subscription has been closed")
	}
	return
}

