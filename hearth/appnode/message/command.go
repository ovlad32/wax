package message

import (
	"fmt"
	"sync"
)


type Command string
type ParamKey string

type Trigger func(
	subject string,
	replySubject string,
	incomingMessage *Body,
) (err error)

type TriggerMap map[Command]Trigger

type Body struct {
	Command Command
	Request interface{}
	Response interface{}
	Params  map[ParamKey]interface{}
	Error   error
}

type Storage struct {
	mux sync.RWMutex
	storage TriggerMap
}

type Register interface {
	Register(Command,Trigger)
}

func (s *Storage) Register(c Command,t Trigger) {
	s.mux.Lock()
	if s.storage == nil {
		s.storage = make(TriggerMap)
	}
	s.storage[c] = t
	s.mux.Unlock()
}


func New(command Command) (b *Body) {
	b = &Body{
		Command: command,
		Params:  make(map[ParamKey]interface{}),
	}
	return
}

func (b *Body) PutRequest(value interface{}) (*Body) {
	b.Request = value
	return b
}

func (b *Body) PutResponse(value interface{}) (*Body) {
	b.Response = value
	return b
}

func (b *Body) PutError(err error) (*Body) {
	b.Error = err
	return b
}


func (b *Body) Append(key ParamKey, value interface{}) (*Body) {
	if eValue, ok := value.(error); ok {
		b.Error = eValue
	} else {
		b.Params[key] = value
	}
	return b
}

func (b Body) Int64(name ParamKey) (result int64, found bool) {
	val, found := b.Params[name]
	if found {
		result, found  = val.(int64)
		if !found {
			panic(fmt.Sprintf("could not get INT64 value from parameter named %v at command %v", name, b.Command))
		}
	}
	return
}



func (b Body) String(name ParamKey) (result string,found bool) {
	val, found := b.Params[name]
	if found {
		result, found  = val.(string)
		if !found {
			panic(fmt.Sprintf("could not get STRING value from parameter named %v at command %v", name, b.Command))
		}
	}
	return
}

func (b Body) Bool(name ParamKey) (result bool, found bool) {
	val, found := b.Params[name]
	if found {
		result, found  = val.(bool)
		if !found {
			panic(fmt.Sprintf("could not get BOOL value from parameter named %v at command %v", name, b.Command))
		}
	}
	return
}
