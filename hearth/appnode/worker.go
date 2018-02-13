package appnode

import (
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
)

const (
	workerSubjectParam CommandMessageParamType = "workerSubject"
)

type WorkerInterface interface {
	Subject() SubjectType
	Subscription() *nats.Subscription
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
	subscription *nats.Subscription
	subject           SubjectType
}

func newWorkersMap() (map[SubjectType]WorkerInterface) {
	return make(map[SubjectType]WorkerInterface)
}


func (worker basicWorker) Subscription() *nats.Subscription {
	return worker.subscription
}

func (worker basicWorker) Subject() SubjectType {
	return worker.subject
}

func (worker basicWorker) reportError(command CommandType, incoming error) (err error) {
	errMsg := &CommandMessageType{
		Command: command,
		Err:     incoming,
	}
	if !worker.subject.IsEmpty()  {
		errMsg.Params = CommandMessageParamMap{
			workerSubjectParam: worker.subscription,
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
		node.workers[a.Subject()] = a
		node.workerMux.Unlock()
	} else {
		node.workerMux.Lock()
		node.workers[a.Subject()] = a
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

	 if worker.Subscription() != nil {
		err = worker.Subscription().Unsubscribe()
		if err != nil {
			err = errors.Wrapf(err, "could not close %v worker ", worker.Subject())
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
		err = errors.Wrapf(err, "could not publish reply of closing %v worker id",  worker.Subject())
		node.logger.Error(err)
		return err
	}
	node.RemoveWorkerBySubject( worker.Subject())
	return
}
