package appnode

import (
	"fmt"
	"github.com/pkg/errors"
)


const MASTER_COMMAND_SUBJECT  = "COMMAND.MASTER"

func (node *ApplicationNodeType) StartMasterNode() (err error) {
	//err = node.initAppNodeService()

	if err != nil {
		return err
	}


	node.initRestApiRouting()

	/*
		osSignal := make(chan os.Signal,1)
		signal.Notify(osSignal,os.Interrupt)
		func() {
			for {
				select {
					_ = <-osSignal

				}
			}
		}
	*/
	return
}


func (node *ApplicationNodeType) MakeMasterCommandSubscription() (err error) {

	if !node.config.Master {
		panic(fmt.Sprintf("AppNode is NOT Master"))
	}

	node.commandSubscription,err = node.encodedConn.Subscribe(
		MASTER_COMMAND_SUBJECT,
		node.MasterCommandSubscriptionFunc(),
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

func (node *ApplicationNodeType) MasterCommandSubscriptionFunc() func(string, string, *CommandMessageType) {
	return func(subj, reply string, msg *CommandMessageType) {
		switch msg.Command {
		case PARISH_OPEN:

			commandSubject := msg.ParamString(workerSubjectParam,"")
			if commandSubject == "" {

			}
			response := &CommandMessageType{}
			response.Command = PARISH_OPENED

			prev := node.FindWorkerById(commandSubject)

			if prev != nil {
				response.Params = map[CommandMessageParamType]interface{} {
					ResubscribedParam:true,
				}
			} else {
				node.slaveCommandSubjects = append(node.slaveCommandSubjects,commandSubject)
			}
			worker := basicWorker{
				id:commandSubject,
			}
			node.AppendWorker(worker)
			err := node.encodedConn.Publish(reply,response)
			if err != nil {
				err = errors.Wrapf(err,"could not reply of opening a new slave")
			}
		default:
			panic (fmt.Sprintf("%v: cannot recognize incoming message command %v",MASTER_COMMAND_SUBJECT,msg.Command))
		}
	}
}

