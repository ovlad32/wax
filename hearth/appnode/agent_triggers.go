package appnode

/*
func (node PeerNode) parishTerminateTrigger() message.Trigger {
	return func(subject,replySubject string, msg *message.Body) (err error) {
		node.Logger().Warnf("Slave '%v': Shutdown signal received", node.Id())

		allTerminated := node.Storage.TerminateAll()

		if node.commandSubscription != nil {
			err = node.commandSubscription.Unsubscribe()
			if err != nil {
				err = errors.Wrapf(err,"could not terminate from command subscription")
				node.Logger().Error(err)
			}
		}

		node.DisconnectFromNATS()
		node.Logger().Warn("Disconnected from NATS")
		if allTerminated {
			os.Exit(0)
		} else {
			node.Logger().Warn("Waiting for graceful termination all the tasks...")
		}
		return
	}
}*/




