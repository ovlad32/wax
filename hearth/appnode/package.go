package appnode

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/pkg/errors"
	"runtime"
	"sync"
	"strings"
	"github.com/nats-io/nuid"
)

var currentInstance *applicationNodeType
var currentMaster *masterApplicationNodeType
var currentSlave *slaveApplicationNodeType

var ciMux sync.Mutex

type SubjectType string
type CommandType string
type CommandMessageParamType string

type CommandMessageParamEntryType struct {
	Key CommandMessageParamType
	Value interface{}
}
type CommandMessageParamEntryArrayType []*CommandMessageParamEntryType

const errorParam CommandMessageParamType = "error"




type ApplicationNodeConfigType struct {
	AstraConfig    handling.AstraConfigType
	NATSEndpoint   string
	IsMaster       bool
	NodeId         NodeIdType
	MasterRestPort int
}


type commandProcessorFuncType func(replySubject string,incomingMessage *CommandMessageType) (err error)



type commandProcessorsMapType map[CommandType]commandProcessorFuncType


/*
var interceptors commandInterceptionInterface
	var interceptorsFound bool
	if interceptors, interceptorsFound = node.commandInterceptorsMap[command]; interceptorsFound {
		if interceptors.request != nil {
			err = interceptors.request(outgoingMessage)
			if err != nil {
				err = errors.Wrapf(err, "could not process request message '%v' via interception handler", command)
				node.logger.Error(err)
				return err
			}
		}
	}
if interceptorsFound && interceptors.response != nil {
		err = interceptors.response(incomingMessage)
		if err != nil {
			err = errors.Wrapf(err, "could not process response message '%v' with interception of handler", command)
			node.logger.Error(err)
			return err
		}
	}
 */

func NewApplicationNode(cfg *ApplicationNodeConfigType) (err error) {
	logger := cfg.AstraConfig.Logger
	if currentInstance != nil {
		err = errors.New("current application node has already been initialized")
		logger.Fatal(err)
		return
	}

	instance := &applicationNodeType{
		config: *cfg,
		commandProcessorsMap:make(commandProcessorsMapType),
	}
	instance.logger = logger

	instance.ctx, instance.ctxCancelFunc = context.WithCancel(context.Background())

	var master *masterApplicationNodeType
	var slave *slaveApplicationNodeType
	//result.hostName, err = os.Hostname()
	if err != nil {
		err = fmt.Errorf("could not get local host name: %v", err)
		return
	}

	if cfg.IsMaster {
		master = &masterApplicationNodeType{
			applicationNodeType: instance,
		}
		master.config.NodeId = MasterNodeId
	} else {
		if cfg.NodeId == "" {
			logger.Fatal("Provide node id!")
		}
		slave = &slaveApplicationNodeType{
			applicationNodeType: instance,
		}
		slave.payloadSizeAdjustments = make(map[CommandType]int64)
		slave.workers = newWorkersMap()
	}

	ciMux.Lock()
	if currentInstance != nil {
		ciMux.Unlock()
		err = errors.New("current application node has already been initialized")
		logger.Fatal(err)
	} else {
		if cfg.IsMaster {
			currentMaster = master
			currentInstance = master.applicationNodeType
			currentMaster.slaveCommandSubjects = make(map[NodeIdType]SubjectType)
			ciMux.Unlock()
		} else {
			currentSlave = slave
			currentInstance = slave.applicationNodeType
			ciMux.Unlock()
		}
	}

	if currentSlave != nil {
		err = currentSlave.startServices()
	} else if currentMaster != nil {
		err = currentMaster.startServices()
	}
	if err != nil {
		logger.Fatal(err)
	}
	currentInstance.logger.Info("AppNode instance started...")

	runtime.Goexit()

	return
}






func (c CommandType) String() string {
	return string(c)
}
func (c CommandType) IsEmpty() bool {
	return c == ""
}


func (s SubjectType) String() string {
	return string(s)
}
func (s SubjectType) IsEmpty() bool {
	return s == ""
}
func newPrefixSubject(prefix string) SubjectType {
	prefix = strings.TrimSpace(prefix)
	if prefix != "" {
		prefix = prefix+"/"
	}
	return SubjectType(fmt.Sprintf("%v%v",prefix,nuid.Next()))
}



func (s CommandMessageParamType) String() string {
	return string(s)
}
func (s CommandMessageParamType) IsEmpty() bool {
	return s == ""
}

func (c CommandMessageParamEntryArrayType) Append(paramType CommandMessageParamType, value interface{}) CommandMessageParamEntryArrayType{
	c = append(c,&CommandMessageParamEntryType{
		Key:paramType,
		Value:value,
	})
	return c
}

func NewCommandMessageParams(n int) CommandMessageParamEntryArrayType{
	return make(CommandMessageParamEntryArrayType,0,n)
}