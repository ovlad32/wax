package appnode

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/pkg/errors"
)

type categorySplitWorker struct {
	basicWorker
	enc *nats.EncodedConn
	logger *logrus.Logger
}
const (
	tableInfoIdParam CommandMessageParamType = "tableInfoId"
	splitIdParam CommandMessageParamType = "splitId"
)

func (agent *categorySplitWorker) categorySplitSubscriptionFunc() func(msg nats.Msg) {
 	return func (msg nats.Msg) {
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
			enc: enc,
			logger:logger,
	}


	tableInfoId :=  message.ParamInt64(tableInfoIdParam,-1)
	splitId :=  message.ParamInt64(splitIdParam,-1)
	if tableInfoId == -1 {
		err = fmt.Errorf("%v == -1",tableInfoIdParam)
		return
	}
	if splitId == -1 {
		err = fmt.Errorf("%v == -1",splitIdParam)
		logger.Error(err)
		return
	}

	subjectName := fmt.Sprintf("SPLIT/%v/%v",tableInfoId,splitId)
	worker.subscriptions = make([]*nats.Subscription,1)

	worker.subscriptions[0], err = enc.Subscribe(
		subjectName,
		worker.categorySplitSubscriptionFunc(),
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
				Command: CATEGORY_SPLIT_OPENED,
				Params: map[CommandMessageParamType]interface{}{
					workerSubjectParam: worker.subscriptions[0].Subject,
				},
			})
		if err != nil {
			err = errors.Errorf("could not reply %v: %v ", message.Command, err)
			logger.Error(err)
			return
		}
		return
	} != nil {
		worker.subscriptions[0].Unsubscribe()
		worker.subscriptions = nil
	}
	worker.id = subjectName
	result = worker
	return
}


