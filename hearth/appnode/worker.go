package appnode

import (
	"github.com/pkg/errors"
	"github.com/nats-io/go-nats"
	"fmt"
)


const (
	workerSubjectParam CommandMessageParamType =  "workerSubject"
)
type WorkerInterface interface {
	Id() string
	Subscriptions() []*nats.Subscription
}

type WorkerHolderInterface interface {
	AppendWorker(a WorkerInterface)
	FindWorkerById(id string) WorkerInterface
	RemoveWorkerById(id string)
	CloseRegularWorker(id string)
}

type basicWorker struct {
	//NATS section
	encodedConn *nats.EncodedConn
	subscriptions []*nats.Subscription
	id string
}



func (worker basicWorker) Subscriptions() []*nats.Subscription {
	return worker.subscriptions
}


func (worker basicWorker) Id() string {
	return worker.id
}

func (worker basicWorker) reportError(command CommandType, incoming error) (err error){
	err = worker.encodedConn.Publish(masterCommandSubject,CommandMessageType{
		Command:command,
		Err:incoming,
	})
	if err != nil {
		err = errors.Wrapf(err,"could not publish error message")
		return
	}
	if err = worker.encodedConn.Flush(); err!=nil {
		err = errors.Wrapf(err,"could not flush published error message")
		return
	}
	if err = worker.encodedConn.LastError(); err!=nil {
		err = errors.Wrapf(err, "could not wire published error message")
		return
	}
	return
}





func (node *applicationNodeType) AppendWorker(a WorkerInterface) {
	if node.workers == nil {
		node.workerMux.Lock()
		if node.workers == nil {
			node.workers = make(map[string]WorkerInterface)
		}
		node.workers[a.Id()] = a
		node.workerMux.Unlock()
	} else {
		node.workerMux.Lock()
		node.workers[a.Id()] = a
		node.workerMux.Unlock()
	}
}

func (node *applicationNodeType) FindWorkerCommandSubject(id string) WorkerInterface {
	return node.FindWorkerById(node.commandSubject(id));
}

func (node *applicationNodeType) FindWorkerById(id string) WorkerInterface {
	if node.workers == nil {
		return nil
	}
	node.workerMux.RLock()
	{
		for k, v := range node.workers {
			fmt.Print(k)
			for _,s := range v.Subscriptions() {
				fmt.Print(", ",s.Subject)
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

func (node *applicationNodeType) RemoveWorkerById(id string) {
	node.workerMux.Lock()
	delete(node.workers, id)
	node.workerMux.Unlock()
}

func (node *applicationNodeType) CloseRegularWorker(
	replySubject string,
	message *CommandMessageType,
	replyCommand CommandType) (err error) {

	id := message.ParamString(workerSubjectParam, "")
	if id == "" {
		err = errors.Errorf("Parameter %v is empty", workerSubjectParam)
		node.logger.Error(err)
		return err
	}

	worker := node.FindWorkerById(id)
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

