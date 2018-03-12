package command

import (
	"fmt"
	"github.com/ovlad32/wax/hearth/appnode"
	"github.com/ovlad32/wax/hearth/appnode/worker"
)


type Command string
type Param string

type Func func(
	subject string,
	replySubject string,
	incomingMessage *Message,
) (err error)



type FuncMap map[Command]Func

type ParamMap map[Param]interface{}

type Message struct {
	Command Command
	Err     error
	TaskId  uint64
	Params  ParamMap
	//mux sync.Mutex
}

type ParamEntry struct {
	Key Param
	Value interface{}
}
type ParamEntries []*ParamEntry






func (s Param) String() string {
	return string(s)
}
func (s Param) IsEmpty() bool {
	return s == ""
}

func (c ParamEntries) Append(paramType Param, value interface{}) ParamEntries{
	c = append(c,&ParamEntry{
		Key:paramType,
		Value:value,
	})
	return c
}

func NewParams(n int) ParamEntries{
	return make(ParamEntries,0,n)
}



func (c Command) String() string {
	return string(c)
}
func (c Command) IsEmpty() bool {
	return c == ""
}


func (m Message) ParamInt64(name Param, defaultValue int64) int64 {
	val, found := m.Params[name]
	if !found {
		return defaultValue
	}

	result, ok := val.(int64)
	if !ok {
		panic(fmt.Sprintf("could not get INT64 value from parameter named %v at command %v", name, m.Command))
	}
	return result
}

func (m Message) ParamString(name Param, defaultValue string) string {
	val, found := m.Params[name]
	if !found {
		return defaultValue
	}

	result, ok := val.(string)
	if !ok {
		panic(fmt.Sprintf("could not get STRING value from parameter named %v at command %v", name, m.Command))
	}
	return result
}


func (m Message) ParamBool(name Param, defaultValue bool) bool {
	val, found := m.Params[name]
	if !found {
		return defaultValue
	}

	result, ok := val.(bool)
	if !ok {
		panic(fmt.Sprintf("could not get BOOL value from parameter named %v at command %v", name, m.Command))
	}
	return result
}



func (m Message) ParamSubject(name Param) (result appnode.Subject) {
	val, found := m.Params[name]
	if !found {
		return appnode.Subject("")
	}

	result, ok := val.(appnode.Subject)
	if !ok {
		panic(fmt.Sprintf("could not get appnode.Subject value from parameter named %v at command %v", name, m.Command))
	}
	return result
}


func (m Message) ParamNodeId(name Param) (result appnode.Id) {
	val, found := m.Params[name]
	if !found {
		return appnode.Id("")
	}

	result, ok := val.(appnode.Id)
	if !ok {
		panic(fmt.Sprintf("could not get appnode.Id value from parameter named %v at command %v", name, m.Command))
	}
	return result
}

func (m Message) ParamWorkerId(name Param) (result worker.Id) {
	val, found := m.Params[name]
	if !found {
		return worker.Id("")
	}

	result, ok := val.(worker.Id)
	if !ok {
		panic(fmt.Sprintf("could not get worker.Id value from parameter named %v at command %v", name, m.Command))
	}
	return result
}


