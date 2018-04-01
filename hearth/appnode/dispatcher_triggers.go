package appnode

import (
	"github.com/ovlad32/wax/hearth/appnode/message"
	"github.com/ovlad32/wax/hearth/appnode/task"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

/*
TODO:closeAllCommandSubscription
func (node *MasterNode) closeAllCommandSubscription() (err error) {

	node.slaveCommandMux.Lock()
	defer node.slaveCommandMux.Unlock()
	for _, subj:= range node.slaveCommandSubjects {
		err = node.Publish(
			subj,
			message.New(parish.TerminateWorker),
		)
		//if err != nil {
		//	node.logger.Error(err)
		//}

	}
	node.slaveCommandSubjects = nil
	node.Logger().Warnf("Slave command subscriptions have been closed")

	err = node.commandSubscription.Unsubscribe()
	if err != nil {
		err = errors.Wrapf(err,"Could not unsubscribe from command subject")
		node.Logger().Error(err)
	} else {
		node.Logger().Warnf("MASTER command subscription has been closed")
	}
	return
}
*/

func onAgentRegister(n *node) message.Trigger {
	return func(_, replySubject string, msg *message.Body) (err error) {
		request, ok := msg.Request.(*MxAgent)
		if !ok {
			err = errors.New("Request type has not been recognized")
			n.Logger().Error(err)
			return
		}

		if request.CommandSubject == "" {
			err = errors.New("Agent command subject is empty")
			n.Logger().Error(err)
			return
		}
		if request.NodeId == "" {
			err = errors.New("Agent Id is empty")
			n.Logger().Error(err)
			return
		}

		n.Logger().Infof(
			"Start registering new Agent '%v' with command subject '%v'",
			request.NodeId,
			request.CommandSubject,
		)

		_, found := n.Tasks.Find(request.NodeId)
		resp := message.New(msg.Command).
			PutResponse(
				&MxAgent{
					RegisteredBefore: found,
				},
			)

		if found {
			n.Logger().Infof(
				"Agent '%v' with command subject '%v' had been registered previously",
				request.NodeId,
				request.CommandSubject,
			)
		}
		err = n.Publish(replySubject, resp)
		if err != nil {
			err = errors.Wrapf(err,
				"could not reply about new Agent '%v' registration",
				request.NodeId,
			)
			n.Logger().Error(err)
			return
		}
		agent := task.New(
			"Agent",
			&n.Communication,
			task.LoggerOption(
				func() *logrus.Logger {
					return nil
				},
			)...,
		)

		n.Tasks.Register(request.NodeId, agent)

		n.Logger().Infof(
			"Agent '%v' with command subject '%v' has been successfully registered",
			request.NodeId,
			request.CommandSubject,
		)
		return
	}
}

func onAgentUnregister(n *node) message.Trigger {
	return func(_ string, replySubject string, incomingMessage *message.Body) (err error) {
		request, ok := incomingMessage.Request.(*MxAgent)

		if !ok {
			err = errors.New("Request type has not been recognized")
			n.Logger().Error(err)
			return
		}

		if request.CommandSubject == "" {
			err = errors.New("Agent command subject is empty")
			n.Logger().Error(err)
			return
		}
		if request.NodeId == "" {
			err = errors.New("Agent Id is empty")
			n.Logger().Error(err)
			return
		}
		n.Tasks.Remove(request.NodeId)
		n.Logger().Warnf("Agent '%v' has been unregistered", request.NodeId)
		return
	}
}
