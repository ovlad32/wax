package appnode

/*
func (node *SlaveNode) createCommandSubscription() (err error) {

	slaveSubject := SlaveCommandSubject(node.Id())
	node.Logger().Infof(
		"Creating Agent '%v' command subscription '%v'...",
		node.Id(),
		slaveSubject,
	)

	if err = func() (err error) {
		node.commandSubscription, err = node.SubscribeMessageTrigger(
			slaveSubject,
			node.commandTrigger(),
		)
		if err != nil {
			err = errors.Wrapf(err, "could not create slave '%v' command subscription", node.Id())
			return
		}

		response, err := node.Request(
			MasterCommandSubject(),
			message.New(parish.Open).
				Append(slaveCommandSubjectParam, slaveSubject).
				Append(slaveIdParam, node.Id()),
		)

		if err != nil {
			err = errors.Wrapf(err, "could not inform master about Slave '%v' command subject creation", node.Id())
			return
		}

		node.Logger().Infof("Slave '%v' registration has been done", node.Id())
		if registered, found := response.Bool(slaveResubscribedParam); registered && found {
			node.Logger().Warnf("Slave '%v' command subscription had been created before...", node.Id())
		}
		return
	}(); err != nil {
		if node.commandSubscription != nil {
			node.commandSubscription.Unsubscribe()
			node.commandSubscription = nil
		}
		node.Logger().Fatal(err)
	} else {
		node.Logger().Infof("Slave '%v' command subscription has been created", node.Id())
	}
	return
}

func (node *SlaveNode) commandTrigger() message.Trigger {
	return func(subject, replySubject string, msg *message.Body) (err error) {
		node.Logger().Infof("Slave '%v' node command command: %v", node.Id(), msg.Command)
		if processor, found := node.commandFuncMap[msg.Command]; found {
			err = processor(subject, replySubject, msg)
			if err != nil {
				//TODO:
			}
		} else {
			err = errors.Errorf("Slave '%v' cannot recognize incoming command command '%v'", node.Id(), msg.Command)
			node.Logger().Fatal(err)
		}
		return
	}
}

func (node *SlaveNode) registerCommandProcessors() (err error) {
	//node.commandProcessorsMap[parishClose] = node.parishCloseFunc()
	node.commandFuncMap[parish.TerminateWorker] = node.parishTerminateWorkerFunc()

	//	node.commandFuncMap[fileStats] = node.fileStatsCommandFunc()

	//	node.commandFuncMap[copyFileOpen] = node.copyFileOpenCommandFunc()
	//	node.commandFuncMap[copyFileCreate] = node.copyFileCreateCommandFunc()
	//	node.commandFuncMap[copyFileLaunch] = node.copyFileLaunchCommandFunc()
	//	node.commandFuncMap[copyFileTerminate] = node.copyFileTerminateCommandFunc()

	//node.commandProcessorsMap[categorySplitOpen] = node.categorySplitOpenFunc()
	//node.commandProcessorsMap[categorySplitClose] = node.categorySplitCloseFunc()
	return
}
*/