package appnode

import (
	"fmt"
	nats "github.com/nats-io/go-nats"
)

type categorySplitWorker struct {
	//NATS section
	enc *nats.EncodedConn
	sub *nats.Subscription
	id string
}


func (agent *categorySplitWorker) categorySplitSubscriptionFunc() func(msg nats.Msg) {
 	return func (msg nats.Msg) {
 		panic("not implemented yet")
	}
}

func newCategorySplitWorker(
	enc *nats.EncodedConn,
	sourceSubject, replySubject string,
	message *CommandMessageType,
) (result *categorySplitWorker, err error) {

	worker := &categorySplitWorker{
		enc:enc,
	}


	tableInfoId :=  message.ParmInt64("tableInfoId",-1)
	splitId :=  message.ParmInt64("splitId",-1);
	if tableInfoId == -1 {
		err = fmt.Errorf("tableInfoId == -1")
		return
	}
	if splitId == -1 {
		err = fmt.Errorf("splitId == -1")
		return
	}

	subjectName := fmt.Sprintf("SPLIT/%v/%v",tableInfoId,splitId)

	worker.sub, err = worker.enc.QueueSubscribe(
		subjectName,
		subjectName,
		worker.categorySplitSubscriptionFunc(),
	)
	if err != nil {
		return
	}
	err = worker.enc.Flush()
	if err != nil {
		return
	}

	if err = worker.enc.LastError(); err != nil {
		return
	}


	err = worker.enc.Publish(replySubject,
		&CommandMessageType{
			Command:CATEGORY_SPLIT_OPENED,
			Parm:map[string]interface{}{
				"workerSubject":worker.sub.Subject,
			},
		})
	if err != nil {
		return
	}

	err = worker.enc.Flush()
	if err != nil {
		return
	}

	if err = worker.enc.LastError(); err != nil {
		return
	}

	worker.id = subjectName
	result = worker
	return
}

func (agent *categorySplitWorker) Close() (err error) {
	if agent.sub != nil {
		err = agent.sub.Unsubscribe()
	}
	panic("close agent")
	return
}


func (a categorySplitWorker) Id() string {
	return a.id
}


