package task

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"github.com/pkg/errors"
	"sync"
	"github.com/ovlad32/wax/hearth/appnode/natsu"
	"github.com/sirupsen/logrus"
)


func NewId() string {
	return fmt.Sprintf("task/%v", nuid.Next())
}



type Task struct {
	*natsu.Communication
	Id string
	Name string
	Subscription       *nats.Subscription
	//taskCancelContext  context.Context
	//taskCancelFunc     context.CancelFunc
	Logger func() (*logrus.Logger)
}




func (task *Task) Unsubscribe() (err error) {
	if task.Subscription != nil {
		name := task.Subscription.Subject
		err = task.Subscription.Unsubscribe()
		if err != nil {
			err = errors.Wrapf(err, "could not unsubscribe worker subject %v", name)
			if task.Logger() != nil {
				task.Logger().Error(err)
			}
			return err
		}
		task.Subscription = nil
	}
	return
}

func (task Task) Key() string {
	return task.Id
}
/*
func (task Basic) Terminate() {
	if basic.taskCancelFunc != nil {
		basic.taskCancelFunc()
	}
}

func (basic Basic) JSONMap() map[string]interface{} {
	result := make(map[string]interface{})
	t := reflect.TypeOf(basic)
	result["worker"] = t.String()
	if basic.subscription != nil {
		result["subscription"] = basic.subscription.Subject
	}
	return result
}
*/

/*

func (worker *basicWorkerType) CounterpartSubject() (subject SubjectType) {
	return worker.counterpartSubject
}


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

/*
type Owner interface {
	Append(Worker)
	Find(string) (Worker,bool)
	Remove(Worker)
	Close(Worker) (error)
	Done(string, err error)
}
*/
type Storage struct {
	storage map[string]interface{}
	mux     sync.RWMutex
}



type FindRegister interface{
	Find(string) (interface{},bool)
	Register(key string, task interface{})
}

func (s *Storage) Register(key string, task interface{}) {
	if s.storage == nil {
		s.mux.Lock()
		if s.storage == nil {
			s.storage = make(map[string]interface{})
		}
		s.storage[key] = task
		s.mux.Unlock()
	} else {
		s.mux.Lock()
		s.storage[key] = task
		s.mux.Unlock()
	}
}

func (s *Storage) Find(key string) (result interface{}, found bool) {
	if s.storage == nil || len(s.storage) == 0{
		return
	}
	s.mux.RLock()
	result, found = s.storage[key]
	s.mux.RUnlock()
	return
}

func (s *Storage) Remove(key string) {
	s.mux.Lock()
	delete(s.storage, key)
	s.mux.Unlock()
}
/*
func (s *Storage) TerminateAll() (bool){
	var err error
	if s.storage == nil {
		return true
	}

	terminated := make([]string,0,len(s.storage))

	for _,w := range s.storage {
		err = w.Terminate()
		if err != nil {
			continue
		}
		terminated = append(terminated,w)
	}

	s.mux.Lock()
	for _,w := range terminated{
		delete(s.storage, w.Id())
	}
	s.mux.Unlock()

	return len(s.storage) == 0
}*/