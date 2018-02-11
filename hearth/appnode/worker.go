package appnode

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
)

const (
	workerSubjectParam CommandMessageParamType = "workerSubject"
)

type WorkerInterface interface {
	Id() SubjectType
	Subscriptions() []*nats.Subscription
}

type WorkerHolderInterface interface {
	Append(a WorkerInterface)
	FindBy(subject SubjectType) WorkerInterface
	RemoveBy(subject SubjectType)
	CloseRegularWorker(subject SubjectType)
}

type basicWorker struct {
	//NATS section
	encodedConn   *nats.EncodedConn
	subscriptions []*nats.Subscription
	id           SubjectType
}

func newWorkersMap() (map[SubjectType]WorkerInterface) {
	return make(map[SubjectType]WorkerInterface)
}


func (worker basicWorker) Subscriptions() []*nats.Subscription {
	return worker.subscriptions
}

func (worker basicWorker) Id() SubjectType {
	return worker.id
}

func (worker basicWorker) reportError(command CommandType, incoming error) (err error) {
	errMsg := &CommandMessageType{
		Command: command,
		Err:     incoming,
	}
	if len(worker.subscriptions) > 0 {
		errMsg.Params = CommandMessageParamMap{
			workerSubjectParam: worker.subscriptions[0],
		}
	}

	err = worker.encodedConn.Publish(masterCommandSubject, errMsg)
	if err != nil {
		err = errors.Wrapf(err, "could not publish error message")
		return
	}
	if err = worker.encodedConn.Flush(); err != nil {
		err = errors.Wrapf(err, "could not flush published error message")
		return
	}
	if err = worker.encodedConn.LastError(); err != nil {
		err = errors.Wrapf(err, "could not wire published error message")
		return
	}
	return
}

func (node *slaveApplicationNodeType) AppendWorker(a WorkerInterface) {
	if node.workers == nil {
		node.workerMux.Lock()
		if node.workers == nil {
			node.workers = newWorkersMap()
		}
		node.workers[a.Id()] = a
		node.workerMux.Unlock()
	} else {
		node.workerMux.Lock()
		node.workers[a.Id()] = a
		node.workerMux.Unlock()
	}
}

func (node *slaveApplicationNodeType) FindWorkerCommandSubject(id NodeIdType) WorkerInterface {
	return node.FindWorkerById(NodeIdType(node.commandSubject(id)))
}

func (node *slaveApplicationNodeType) FindWorkerById(id NodeIdType) WorkerInterface {
	if node.workers == nil {
		return nil
	}
	node.workerMux.RLock()
	{
		for k, v := range node.workers {
			fmt.Print(k)
			for _, s := range v.Subscriptions() {
				fmt.Print(", ", s.Subject)
			}
			fmt.Println()
		}
	}

	worker, found := node.workers[id]
	node.workerMux.RUnlock()
	if found {
		return worker
	}
	return nil
}

func (node *slaveApplicationNodeType) RemoveWorkerById(id SubjectType) {
	node.workerMux.Lock()
	delete(node.workers, id)
	node.workerMux.Unlock()
}

func (node *slaveApplicationNodeType) CloseRegularWorker(
	replySubject string,
	message *CommandMessageType,
	replyCommand CommandType) (err error) {

	id := message.ParamSubject(workerSubjectParam)
	if id.IsEmpty() {
		err = errors.Errorf("Parameter %v is empty", workerSubjectParam)
		node.logger.Error(err)
		return err
	}

	worker := node.FindById(id)
	if worker == nil {
		err = errors.Errorf("could not find %v worker", id)
		node.logger.Error(err)
		return err
	}

	for _, s := range worker.Subscriptions() {
		err = s.Unsubscribe()
		if err != nil {
			err = errors.Wrapf(err, "could not close %v worker ", id)
			node.logger.Error(err)
			return err
		}
	}
	err = node.encodedConn.Publish(
		replySubject,
		&CommandMessageType{
			Command: replyCommand,
		},
	)
	if err != nil {
		err = errors.Wrapf(err, "could not publish reply of closing %v worker id", id)
		node.logger.Error(err)
		return err
	}
	node.RemoveWorkerById(id)
	return
}
