package appnode

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/pkg/errors"
	"runtime"
	"sync"
	"github.com/ovlad32/wax/hearth/appnode/command"
	"github.com/ovlad32/wax/hearth/appnode/worker"
)

var currentInstance *applicationNodeType
var currentMaster *masterApplicationNodeType
var currentSlave *SlaveNode

var ciMux sync.Mutex





type ApplicationNodeConfigType struct {
	AstraConfig    handling.AstraConfigType
	NATSEndpoint   string
	IsMaster       bool
	NodeId         Id
	RestAPIPort int
	//MonitorPort int
}



type ParishRequestType struct {
	SlaveNodeId Id
	CommandSubject string
}

type ParishResponseType struct {
	ReConnect bool
	Err       error
}

/*
var interceptors commandInterceptionInterface
	var interceptorsFound bool
	if interceptors, interceptorsFound = node.commandInterceptorsMap[command]; interceptorsFound {
		if interceptors.request != nil {
			err = interceptors.request(outgoingMessage)
			if err != nil {
				err = errors.Wrapf(err, "could not process request command '%v' via interception handler", command)
				node.logger.Error(err)
				return err
			}
		}
	}
if interceptorsFound && interceptors.response != nil {
		err = interceptors.response(incomingMessage)
		if err != nil {
			err = errors.Wrapf(err, "could not process response command '%v' with interception of handler", command)
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
		commandFuncMap:make(command.FuncMap),
	}
	instance.logger = logger

	instance.ctx, instance.ctxCancelFunc = context.WithCancel(context.Background())

	var master *masterApplicationNodeType
	var slave *SlaveNode
	//result.hostName, err = os.Hostname()
	if err != nil {
		err = fmt.Errorf("could not get local host name: %v", err)
		return
	}

	if cfg.IsMaster {
		master = &masterApplicationNodeType{
			applicationNodeType: instance,
		}
		master.config.NodeId = masterNodeId
	} else {
		if cfg.NodeId == "" {
			logger.Fatal("Provide node id!")
		}
		slave = &SlaveNode{
			applicationNodeType: instance,
		}
		slave.payloadSizeAdjustments = make(map[command.Command]int64)
		slave.workers = worker.NewMap()
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
			currentMaster.slaveCommandSubjects = make(map[Id]Subject)
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








