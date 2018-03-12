package worker

import (
	"context"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"github.com/ovlad32/wax/hearth/appnode/command"
	"github.com/pkg/errors"
	"reflect"
	"github.com/ovlad32/wax/hearth/appnode"
)

type Id string

func NewId() Id {
	return Id(fmt.Sprintf("worker/%v", nuid.Next()))
}
func (c Id) String() string {
	return string(c)
}
func (c Id) IsEmpty() bool {
	return c == ""
}

type InvokeInterface interface {
	Invoke() error
	Terminate() error
}

type WrapperInterface interface {
	WorkerId() Id
	CounterpartSubject() (subject appnode.Subject)
	Unsubscribe() (err error)
	JSONMap() map[string]interface{}
}
type WorkerInterface interface {
	InvokeInterface
	WrapperInterface
}

type WorkerHolderInterface interface {
	AppendWorker(a WorkerInterface)
	FindWorker(id Id) WorkerInterface
	RemoveWorker(w WorkerInterface)
	CloseRegularWorker(w WorkerInterface)
	Done(w WorkerInterface)
	Error(w WorkerInterface, err error)
}

type Basic struct {
	//NATS section
	id                 Id
	node               *appnode.SlaveNode
	subscription       *nats.Subscription
	counterpartSubject appnode.Subject
	taskCancelContext  context.Context
	taskCancelFunc     context.CancelFunc
}

func NewMap() map[Id]WorkerInterface {
	return make(map[Id]WorkerInterface)
}

func (worker *basicWorkerType) Unsubscribe() (err error) {
	if worker.subscription != nil {
		name := worker.subscription.Subject
		err = worker.subscription.Unsubscribe()
		if err != nil {
			err = errors.Wrapf(err, "could not unsubscribe worker subject %v", name)
			if worker.node != nil && worker.node.logger != nil {
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

func (worker *basicWorkerType) Id() WorkerIdType {
	return worker.id
}

func (worker *basicWorkerType) Terminate() {
	if worker.taskCancelFunc != nil {
		worker.taskCancelFunc()
	}
}

func (worker basicWorkerType) JSONMap() map[string]interface{} {
	result := make(map[string]interface{})
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
		err = errors.Wrapf(err, "could not publish error command")
		return
	}
	if err = worker.encodedConn.Flush(); err != nil {
		err = errors.Wrapf(err, "could not flush published error command")
		return
	}
	if err = worker.encodedConn.LastError(); err != nil {
		err = errors.Wrapf(err, "could not wire published error command")
		return
	}
	return
}
*/
func (node *appnode.SlaveNode) AppendWorker(a WorkerInterface) {
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

func (node *appnode.SlaveNode) FindWorker(id worker.Id) (result WorkerInterface, err error) {
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

	result, found := node.workers[id]
	node.workerMux.RUnlock()
	if !found {
		err = errors.Errorf("could not find worker with WorkerId=%v", id)
	}
	return
}

func (node *appnode.SlaveNode) RemoveWorker(w WorkerInterface) {
	node.workerMux.Lock()
	delete(node.workers, w.Id())
	node.workerMux.Unlock()
}

/*
func (node *slaveApplicationNodeType) CloseRegularWorker(
	replySubject string,
	command *CommandMessageType,
	replyCommand CommandType) (err error) {

	id := command.ParamWorkerId(workerIdParam)
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
*/
