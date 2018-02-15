package appnode

import (
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"context"
)

const (
	workerSubjectParam CommandMessageParamType = "workerSubject"

)

type WorkerInterface interface {
	CommandSubject() (subject SubjectType)
	UnsubscribeCommandSubject() (err error)
	CancelCurrentJob()
}

type WorkerHolderInterface interface {
	Append(a WorkerInterface)
	FindBy(subject SubjectType) WorkerInterface
	RemoveBy(subject SubjectType)
	CloseRegularWorker(subject SubjectType)
}

type basicWorkerType struct {
	//NATS section
	node *slaveApplicationNodeType
	subscription   *nats.Subscription
	subject        SubjectType
	jobContext context.Context
	jobCancelFunc context.CancelFunc
}

func newWorkersMap() (map[SubjectType]WorkerInterface) {
	return make(map[SubjectType]WorkerInterface)
}


func (worker *basicWorkerType) UnsubscribeCommandSubject() (err error) {
	if worker.subscription != nil {
		err = worker.subscription.Unsubscribe()
		if err != nil {
			err = errors.Wrapf(err, "could not unsubscribe worker command subject %v", worker.CommandSubject())
			if worker.node!=nil && worker.node.logger != nil {
				worker.node.logger.Error(err)
			}
			return err
		}
		worker.subscription = nil
	}
	return
}

func (worker *basicWorkerType) CommandSubject() (subject SubjectType) {
	return worker.subject
}

func (worker *basicWorkerType) CancelCurrentJob() {
	if worker.jobCancelFunc != nil {
		worker.jobCancelFunc()
	}
}
/*
func (worker basicWorker) reportError(command CommandType, incoming error) (err error) {

	errMsg := &CommandMessageType{
		Command: command,
		Err:     incoming,
	}
	if !worker.subject.IsEmpty()  {
		errMsg.Params = CommandMessageParamMap{
			workerSubjectParam: worker.subscription,
			slaveCommandSubjectParam:
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
*/
func (node *slaveApplicationNodeType) AppendWorker(a WorkerInterface) {
	if node.workers == nil {
		node.workerMux.Lock()
		if node.workers == nil {
			node.workers = newWorkersMap()
		}
		node.workers[a.CommandSubject()] = a
		node.workerMux.Unlock()
	} else {
		node.workerMux.Lock()
		node.workers[a.CommandSubject()] = a
		node.workerMux.Unlock()
	}
}



func (node *slaveApplicationNodeType) FindWorkerBySubject(subject SubjectType) WorkerInterface {
	if node.workers == nil {
		return nil
	}
	node.workerMux.RLock()
	/*{
		for k, v := range node.workers {
			fmt.Print(k)
			for _, s := range v.Subscriptions() {
				fmt.Print(", ", s.Subject)
			}
			fmt.Println()
		}
	}*/

	worker, found := node.workers[subject]
	node.workerMux.RUnlock()
	if found {
		return worker
	}
	return nil
}

func (node *slaveApplicationNodeType) RemoveWorkerBySubject(subject SubjectType) {
	node.workerMux.Lock()
	delete(node.workers, subject)
	node.workerMux.Unlock()
}

func (node *slaveApplicationNodeType) CloseRegularWorker(
	replySubject string,
	message *CommandMessageType,
	replyCommand CommandType) (err error) {

	subject := message.ParamSubject(workerSubjectParam)
	if subject.IsEmpty() {
		err = errors.Errorf("Parameter %v is empty", workerSubjectParam)
		node.logger.Error(err)
		return err
	}

	worker := node.FindWorkerBySubject(subject)
	if worker == nil {
		err = errors.Errorf("could not find %v worker", subject)
		node.logger.Error(err)
		return err
	}

	err = worker.UnsubscribeCommandSubject()
	if err != nil {
		return err
	}

	err = node.encodedConn.Publish(
		replySubject,
		&CommandMessageType{
			Command: replyCommand,
		},
	)
	if err != nil {
		err = errors.Wrapf(err, "could not publish reply of closing %v worker id",  worker.CommandSubject())
		node.logger.Error(err)
		return err
	}
	node.RemoveWorkerBySubject(worker.CommandSubject())
	return
}
