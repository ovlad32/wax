package appnode

import (
	"context"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"runtime"
	"sync"
)

var currentInstance *applicationNodeType
var currentMaster *masterApplicationNodeType
var currentSlave *slaveApplicationNodeType

var ciMux sync.Mutex
type stringDerivedType string

type NodeIdType stringDerivedType
type CommandType stringDerivedType
type SubjectType stringDerivedType

type CommandMessageParamType stringDerivedType


const (
	parishOpen   CommandType = "PARISH.OPEN"
	parishOpened CommandType = "PARISH.OPENED"
	parishClose  CommandType = "PARISH.CLOSE"
	parishClosed CommandType = "PARISH.CLOSED"
)




type ApplicationNodeConfigType struct {
	AstraConfig    handling.AstraConfigType
	NATSEndpoint   string
	IsMaster       bool
	NodeId         NodeIdType
	MasterRestPort int
}

type applicationNodeType struct {
	config        ApplicationNodeConfigType
	ctx           context.Context
	ctxCancelFunc context.CancelFunc

	encodedConn         *nats.EncodedConn
	commandSubscription *nats.Subscription
	//
	//
	logger *logrus.Logger
}

type slaveApplicationNodeType struct {
	*applicationNodeType
	//
	payloadSizeAdjustments map[CommandType]int64
	workerMux              sync.RWMutex
	workers                map[SubjectType]WorkerInterface
}


type masterApplicationNodeType struct {
	*applicationNodeType
	slaveCommandMux sync.RWMutex
	slaveCommandSubjects map[NodeIdType]SubjectType
	//TODO: CancelFunc!!!
}

func NewApplicationNode(cfg *ApplicationNodeConfigType) (err error) {
	logger := cfg.AstraConfig.Logger
	if currentInstance != nil {
		err = errors.New("current application node has already been initialized")
		logger.Fatal(err)
		return
	}

	instance := &applicationNodeType{
		config: *cfg,
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
		master.config.NodeId = "MASTER"
	} else {
		if cfg.NodeId == "" {
			logger.Fatal("Provide node id!")
		}
		slave = &slaveApplicationNodeType{
			applicationNodeType: instance,
		}
		slave.payloadSizeAdjustments = make(map[CommandType]int64)
		slave.workers = make(map[SubjectType]WorkerInterface)
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



func (node *applicationNodeType) NodeId() NodeIdType {
	return node.config.NodeId
}

func (node applicationNodeType) commandSubject(id NodeIdType) SubjectType {
	return SubjectType(fmt.Sprintf("COMMAND/%v", id))
}

func (node *applicationNodeType) connectToNATS() (err error) {
	conn, err := nats.Connect(
		node.config.NATSEndpoint,
		nats.Name(string(node.config.NodeId)),
	)
	if err != nil {
		err = errors.Wrapf(err, "could not connect to NATS at '%v'", node.config.NATSEndpoint)
		node.logger.Error(err)
		return
	}
	node.encodedConn, err = nats.NewEncodedConn(conn, nats.GOB_ENCODER)
	if err != nil {
		err = errors.Wrap(err, "could not create encoder for NATS")
		node.logger.Error(err)
		return
	}
	node.logger.Info("Node has been connected to NATS")
	return
}


func (c stringDerivedType) String() string {
	return string(c)
}


func (s stringDerivedType) IsEmpty() bool {
	return s == ""

}


