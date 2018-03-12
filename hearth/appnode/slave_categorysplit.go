package appnode

/*

const (
	categorySplitOpen   CommandType = "SPLIT.CREATE"
	categorySplitOpened CommandType = "SPLIT.CREATED"
	categorySplitClose  CommandType = "SPLIT.CLOSE"
	categorySplitClosed CommandType = "SPLIT.CLOSED"
)

const (
	tableInfoIdParam CommandMessageParamType = "tableInfoId"
	splitIdParam     CommandMessageParamType = "splitId"
)


type categorySplitWorker struct {
	basicWorkerType
	enc    *nats.EncodedConn
	logger *logrus.Logger
}

func (agent *categorySplitWorker) subscriptionFunc() func(msg nats.Msg) {
	return func(msg nats.Msg) {
		panic("not implemented yet")
	}
}


func (node *slaveApplicationNodeType) categorySplitOpenFunc() commandProcessorFuncType {
	return func(replySubject string, incomingMessage *CommandMessageType) (err error) {
		worker, err := newCategorySplitWorker(
			node.encodedConn,
			replySubject, incomingMessage,
			node.logger,
		)
		if err != nil {
			panic(err.Error())
		}
		node.AppendWorker(worker)
		return
	}
}

func (node *slaveApplicationNodeType) categorySplitCloseFunc() commandProcessorFuncType {
	return func(replySubject string, incomingMessage *CommandMessageType) (err error) {
		err = node.CloseRegularWorker(replySubject, incomingMessage, categorySplitClosed)
		if err != nil {
			panic(err.Error())
		}
		return
	}
}


func newCategorySplitWorker(
	enc *nats.EncodedConn,
	replySubject string,
	command *CommandMessageType,
	logger *logrus.Logger,
) (result *categorySplitWorker, err error) {

	worker := &categorySplitWorker{
		basicWorker: basicWorker{
			encodedConn: enc,
		},
		logger: logger,
	}

	tableInfoId := command.ParamInt64(tableInfoIdParam, -1)
	splitId := command.ParamInt64(splitIdParam, -1)
	if tableInfoId == -1 {
		err = fmt.Errorf("%v == -1", tableInfoIdParam)
		return
	}
	if splitId == -1 {
		err = fmt.Errorf("%v == -1", splitIdParam)
		logger.Error(err)
		return
	}

	subjectName := fmt.Sprintf("SPLIT/%v/%v", tableInfoId, splitId)

	worker.subscription, err = enc.Subscribe(
		subjectName,
		worker.subscriptionFunc(),
	)
	if err != nil {
		logger.Error(err)
		return
	}
	if func() (err error) {
		err = enc.Flush()
		if err != nil {
			logger.Error(err)
			return
		}

		if err = enc.LastError(); err != nil {
			logger.Error(err)
			return
		}

		err = enc.Publish(replySubject,
			&CommandMessageType{
				Command: categorySplitOpened,
				Params: map[CommandMessageParamType]interface{}{
					workerSubjectParam: worker.subscription.Subject,
				},
			})
		if err != nil {
			err = errors.Errorf("could not reply %v: %v ", command.Command, err)
			logger.Error(err)
			return
		}
		return
	} != nil {
		worker.subscription.Unsubscribe()
		worker.subscription = nil
	}
	worker.subject = SubjectType(subjectName)
	result = worker
	return
}

*/