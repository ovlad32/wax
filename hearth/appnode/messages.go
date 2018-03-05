package appnode

import (
	"fmt"
)

type ParishRequestType struct {
	SlaveNodeId NodeIdType
	CommandSubject string
}

type ParishResponseType struct {
	ReConnect bool
	Err       error
}

type CommandMessageParamMap map[CommandMessageParamType]interface{}

type CommandMessageType struct {
	Command CommandType
	Err     error
	Params  CommandMessageParamMap
	//mux sync.Mutex
}

func (m CommandMessageType) ParamInt64(name CommandMessageParamType, defaultValue int64) int64 {
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

func (m CommandMessageType) ParamString(name CommandMessageParamType, defaultValue string) string {
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


func (m CommandMessageType) ParamBool(name CommandMessageParamType, defaultValue bool) bool {
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



func (m CommandMessageType) ParamSubject(name CommandMessageParamType) (result SubjectType) {
	val, found := m.Params[name]
	if !found {
		return SubjectType("")
	}

	result, ok := val.(SubjectType)
	if !ok {
		panic(fmt.Sprintf("could not get SubjectType value from parameter named %v at command %v", name, m.Command))
	}
	return result
}


func (m CommandMessageType) ParamNodeId(name CommandMessageParamType) (result NodeIdType) {
	val, found := m.Params[name]
	if !found {
		return NodeIdType("")
	}

	result, ok := val.(NodeIdType)
	if !ok {
		panic(fmt.Sprintf("could not get NodeIdType value from parameter named %v at command %v", name, m.Command))
	}
	return result
}

func (m CommandMessageType) ParamWorkerId(name CommandMessageParamType) (result WorkerIdType) {
	val, found := m.Params[name]
	if !found {
		return WorkerIdType("")
	}

	result, ok := val.(WorkerIdType)
	if !ok {
		panic(fmt.Sprintf("could not get WorkerIdType value from parameter named %v at command %v", name, m.Command))
	}
	return result
}


