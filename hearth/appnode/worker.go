package appnode

import (
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"context"
	"fmt"
	"github.com/nats-io/nuid"
	"reflect"
)

const (
	workerSubjectParam CommandMessageParamType = "workerSubject"
	workerIdParam CommandMessageParamType = "workerId"
)

type WorkerIdType string

func newWorkerId() WorkerIdType {
	return WorkerIdType(fmt.Sprintf("worker/%v",nuid.Next()))
}
func (c WorkerIdType) String() string {
	return string(c)
}
func (c WorkerIdType) IsEmpty() bool {
	return c == ""
}


type WorkerInterface interface {
	Id() WorkerIdType
	CounterpartSubject() (subject SubjectType)
	Unsubscribe() (err error)
	TaskCanceled()
	TaskDone()
	JSONMap() map[string]interface{}
}

type WorkerHolderInterface interface {
	AppendWorker(a WorkerInterface)
	FindWorker(id WorkerIdType) WorkerInterface
	RemoveWorker(id WorkerIdType)
	CloseRegularWorker(id WorkerIdType)
}

type basicWorkerType struct {
	//NATS section
	id                 WorkerIdType
	node               *slaveApplicationNodeType
	subscription       *nats.Subscription
	counterpartSubject SubjectType
	taskCancelContext  context.Context
	taskCancelFunc     context.CancelFunc
}


func newWorkersMap() (map[WorkerIdType]WorkerInterface) {
	return make(map[WorkerIdType]WorkerInterface)
}


func (worker *basicWorkerType) Unsubscribe() (err error) {
	if worker.subscription != nil {
		name := worker.subscription.Subject
		err = worker.subscription.Unsubscribe()
		if err != nil {
			err = errors.Wrapf(err, "could not unsubscribe worker subject %v", name )
			if worker.node!=nil && worker.node.logger != nil {
				worker.node.logger.Error(err)
			}
			return err
		}
		worker.subscription = nil
	}
	return
}


func (worker *basicWorkerType) CounterpartSubject() (subject SubjectType) {
	return worker.counterpartSubject
}

func (worker *basicWorkerType) Id() (WorkerIdType) {
	return worker.id
}

func (worker *basicWorkerType) TaskCanceled() {
	if worker.taskCancelFunc != nil {
		worker.taskCancelFunc()
	}
}

func(worker basicWorkerType ) JSONMap() map[string]interface{} {
	result:= make(map[string]interface{})
	t := reflect.TypeOf(worker)
	result["worker"] = t.String()

	if !worker.counterpartSubject.IsEmpty() {
		result["counterpartSubject"] = worker.counterpartSubject
	}
	if worker.subscription != nil {
		result["subscription"] = worker.subscription.Subject
	}
	return result
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
		node.workers[a.Id()] = a
		node.workerMux.Unlock()
	} else {
		node.workerMux.Lock()
		node.workers[a.Id()] = a
		node.workerMux.Unlock()
	}
}



func (node *slaveApplicationNodeType) FindWorker(id WorkerIdType) WorkerInterface {
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

	worker, found := node.workers[id]
	node.workerMux.RUnlock()
	if found {
		return worker
	}
	return nil
}

func (node *slaveApplicationNodeType) RemoveWorker(w WorkerInterface) {
	node.workerMux.Lock()
	delete(node.workers, w.Id())
	node.workerMux.Unlock()
}

func (node *slaveApplicationNodeType) CloseRegularWorker(
	replySubject string,
	message *CommandMessageType,
	replyCommand CommandType) (err error) {

	id := message.ParamWorkerId(workerIdParam)
	if id.IsEmpty() {
		err = errors.Errorf("Parameter %v is empty", workerIdParam)
		node.logger.Error(err)
		return err
	}

	worker := node.FindWorker(id)
	if worker == nil {
		err = errors.Errorf("could not find %v worker", id)
		node.logger.Error(err)
		return err
	}

	err = worker.Unsubscribe()
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
		err = errors.Wrapf(err, "could not publish reply of closing %v worker id",  worker.Id())
		node.logger.Error(err)
		return err
	}
	node.RemoveWorker(worker)
	return
}
