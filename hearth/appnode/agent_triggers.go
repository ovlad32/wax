package appnode

import (
	"github.com/ovlad32/wax/hearth/appnode/message"
	"github.com/pkg/errors"
	"os"
	"fmt"
	"github.com/ovlad32/wax/hearth/di"
)

func onAgentTerminate(n *node) message.Trigger {
	return func(subject, replySubject string, msg *message.Body) (err error) {
		n.Logger().Warnf("Slave '%v': Shutdown signal received", n.Id())

		n.Tasks.TerminateAll()

		if n.commandSubscription != nil {
			err = n.Publish(
				DispatcherCommandSubject(),
				message.New(agentUnregister).
					PutRequest(
						&MxAgent{
							NodeId: n.Id(),
						},
					),
			)
			if err != nil {
				err = errors.Wrapf(err,
					"could not notify dispatcher about command subscription closing")
				n.Logger().Error(err)
			}

			err = n.commandSubscription.Unsubscribe()

			if err != nil {
				err = errors.Wrapf(err,
					"could not close command subscription")
				n.Logger().Error(err)
			}
		}
		n.DisconnectFromNATS()

		n.Logger().Warn("Disconnected from NATS, bye...")
		os.Exit(0)
		return
	}
}

func onActionRun(n *node) message.Trigger {
	return func(_ string, replySubject string, incomingMessage *message.Body) (err error) {
		replyMessage := message.New(incomingMessage.Command)

		runnable, isAble := incomingMessage.Request.(di.Runner)
		if err = func() (err error) {
			if !isAble {
				err = errors.New("could not recognize incoming request type")
				return
			}

			if instance, isAble  := runnable.(di.NatsCommunicable); isAble {
				instance.SetCommunication(&n.Communication)
			}
			if instance, isAble  := runnable.(di.NodeIdInjectable); isAble {
				instance.SetNodeId(n.Id())
			}

			if err = runnable.Run(); err != nil {
				return
			}
			return
		}(); err != nil {
			n.Logger().Error(err)
			replyMessage.Error = err
		}

		if runnable != nil {
			replyMessage.PutResponse(runnable)
		}

		err = n.Reply(replySubject, replyMessage)
		if err != nil {
			err = errors.Wrapf(err, "could not send reply message for %v", incomingMessage.Command)
			n.Logger().Error(err)
		}
		return
	}
}


func onFileCopyOpenPipe (n *node) message.Trigger {
	return func(_ string, replySubject string, incomingMessage *message.Body) (err error) {
		mx, ok := incomingMessage.Request.(MxFileCopy)
		if err = func() (err error) {
			if !ok {
				err = errors.New("could not recognize incoming request type")
				return
			}
			if mx.DstPathToFile == "" {
				err = errors.New("Path to destination file is empty")
				return
			}
			mx.DataSubject = fmt.Sprintf("$v/%v",incomingMessage,)

				n.SubscribeBareFunc(
				""
			)



		}(); err != nil {

		}


		return
	}
}