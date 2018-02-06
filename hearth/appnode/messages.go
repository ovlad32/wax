package appnode

import (
	"fmt"
)

type ParishRequestType struct {
	SlaveNodeName NodeNameType
	CommandSubject string
}

type ParishResponseType struct {
	ReConnect bool
	Err error
}

type CommandMessageParamType string
func (c CommandMessageParamType) String() string {
	return string(c)
}



type CommandMessageType struct {
	Command CommandType
	///Err error
	Params map[CommandMessageParamType]interface{}
	//mux sync.Mutex
}



func (m CommandMessageType) ParamInt64(name CommandMessageParamType, defaultValue int64) int64 {
	val,found := m.Params[name]
	if !found {
		return defaultValue
	}

	result,ok := val.(int64)
	if !ok {
		panic(fmt.Sprintf("could not get INT64 value from parameter named %v at command %v",name,m.Command))
	}
	return result
}

func (m CommandMessageType) ParamString(name CommandMessageParamType, defaultValue string) string {
	val,found := m.Params[name]
	if !found {
		return defaultValue
	}

	result,ok := val.(string)
	if !ok {
		panic(fmt.Sprintf("could not get STRING value from parameter named %v at command %v",name,m.Command))
	}
	return result
}


func (m CommandMessageType) ParamBool(name CommandMessageParamType, defaultValue bool) bool {
	val,found := m.Params[name]
	if !found {
		return defaultValue
	}

	result,ok := val.(bool)
	if !ok {
		panic(fmt.Sprintf("could not get BOOL value from parameter named %v at command %v",name,m.Command))
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