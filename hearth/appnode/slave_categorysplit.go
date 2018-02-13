package appnode

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)


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
	basicWorker
	enc    *nats.EncodedConn
	logger *logrus.Logger
}

func (agent *categorySplitWorker) subscriptionFunc() func(msg nats.Msg) {
	return func(msg nats.Msg) {
		panic("not implemented yet")
	}
}

func newCategorySplitWorker(
	enc *nats.EncodedConn,
	replySubject string,
	message *CommandMessageType,
	logger *logrus.Logger,
) (result *categorySplitWorker, err error) {

	worker := &categorySplitWorker{
		basicWorker: basicWorker{
			encodedConn: enc,
		},
		logger: logger,
	}

	tableInfoId := message.ParamInt64(tableInfoIdParam, -1)
	splitId := message.ParamInt64(splitIdParam, -1)
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
			err = errors.Errorf("could not reply %v: %v ", message.Command, err)
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
