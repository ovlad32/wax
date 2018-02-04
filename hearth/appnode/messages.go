package appnode

import (
	"fmt"
	"sync"
)

type ParishRequestType struct {
	SlaveNodeName NodeNameType
	CommandSubject string
}

type ParishResponseType struct {
	ReConnect bool
	Err error
}

type CommandMessageType struct {
	Command CommandType
	Err error
	Parm map[string]interface{}
	mux sync.Mutex
}


func (m CommandMessageType) ParmInt64(name string, defaultValue int64) int64 {
	val,found := m.Parm[name]
	if !found {
		return defaultValue
	}

	result,ok := val.(int64)
	if !ok {
		panic(fmt.Sprintf("could not get int64 value from parameter named %v at command %v",name,m.Command))
	}
	return result
}

func (m CommandMessageType) ParmString(name string, defaultValue string) string {
	val,found := m.Parm[name]
	if !found {
		return defaultValue
	}

	result,ok := val.(string)
	if !ok {
		panic(fmt.Sprintf("could not get string value from parameter named %v at command %v",name,m.Command))
	}
	return result
}
/*
func (m *CommandMessageType) Parm(name string, value interface{}) {
	if m.parm == nil {
		m.mux.Lock()
		if m.parm == nil {
			m.parm = make(map[string]interface{})
		}
		m.mux.Unlock()
	}
	m.parm[name] = value
}

*/