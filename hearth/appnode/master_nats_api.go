package appnode

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/nats-io/go-nats"
)

func (node *masterApplicationNodeType) initNatsService() (err error) {

	func() {
		err = node.connectToNATS()

		if err != nil {
			return
		}
		err = node.makeCommandSubscription()
		if err != nil {
			return
		}
	}()
	if err != nil {
		err = errors.Wrapf(err, "could not start NATS service")
	}
	return
}


func (node *masterApplicationNodeType) makeCommandSubscription() (err error) {

	node.logger.Infof("Create MASTER command subscription '%v'", masterCommandSubject)

	node.commandSubscription,err = node.encodedConn.Subscribe(
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
	return func(subj, reply string, msg *CommandMessageType) {
		switch msg.Command {
		case parishOpen:

			slaveCommandSubject := msg.ParamString(slaveCommandSubjectParam,"")
			if slaveCommandSubject  == "" {
				node.logger.Warn("gotten slave command subject is empty!")
				return
			}
			slaveId := msg.ParamString(slaveIdParam,"")

			if slaveId  == "" {
				node.logger.Warn("gotten slaveId is empty!")
				return
			}
			node.logger.Infof("Start registering new Slave '%v' with command subject '%v'",slaveId,slaveCommandSubject )
			response := &CommandMessageType{}
			response.Command = parishOpened
			node.workerMux.RLock()
			if prev, found := node.slaveCommandSubjects[slaveId]; found{
				node.workerMux.RUnlock()
				response.Params = CommandMessageParamMap {
					ResubscribedParam:true,
				}
				node.logger.Infof(
					"New Slave '%v' with command subject '%v' had been registered previously with '%v'",
					slaveId,
					slaveCommandSubject,
					prev,
				)
			}
			node.workerMux.RUnlock()
			node.workerMux.Lock()
			node.slaveCommandSubjects[slaveId] = slaveCommandSubject
			node.workerMux.Unlock()
			err := node.encodedConn.Publish(reply,response)
			if err != nil {
				err = errors.Wrapf(err,"could not reply of opening a new slave")
				return
			}
			node.logger.Infof("A new node with command subject '%v' has been successfully registered",slaveCommandSubject)
		default:
			panic (fmt.Sprintf("%v: cannot recognize incoming message command '%v' ", masterCommandSubject,msg.Command))
		}
	}
}

func (node *masterApplicationNodeType) closeAllCommandSubscription()  (err error) {

	node.workerMux.Lock()
	for _, commandSubject := range node.slaveCommandSubjects {
		response := new(CommandMessageType)
		err = node.encodedConn.Request(commandSubject,
			&CommandMessageType{
				Command: parishClose,
			},
			response,
			nats.DefaultTimeout,
		)
		if err!=nil {
			node.logger.Error(err)
		}
		if response.Command != parishClosed {
			node.logger.Error(response.Command)
		}
	}
	node.slaveCommandSubjects = make(map[string]string)
	node.workerMux.Unlock()
	node.logger.Warnf("Slave command subscriptions have been closed")

	err = node.commandSubscription.Unsubscribe()
	if err != nil {

	} else{
		node.logger.Warnf("MASTER command subscription has been closed")
	}
	return
}

