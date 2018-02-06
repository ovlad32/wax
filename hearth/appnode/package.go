package appnode

import (
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sync"
	"fmt"
	"runtime"
	"github.com/ovlad32/wax/hearth/handling"
	"context"
)

var currentInstance *applicationNodeType
var currentMaster *masterApplicationNodeType
var currentSlave *slaveApplicationNodeType

var ciMux sync.Mutex

type NodeNameType string
type CommandType string

const (
	PARISH_OPEN  CommandType = "PARISH.OPEN"
	PARISH_OPENED  CommandType = "PARISH.OPENED"
	PARISH_CLOSE CommandType = "PARISH.CLOSE"
	PARISH_CLOSED CommandType = "PARISH.CLOSED"

	CATEGORY_SPLIT_OPEN   CommandType = "SPLIT.CREATE"
	CATEGORY_SPLIT_OPENED CommandType = "SPLIT.CREATED"
	CATEGORY_SPLIT_CLOSE  CommandType = "SPLIT.CLOSE"
	CATEGORY_SPLIT_CLOSED CommandType = "SPLIT.CLOSED"
)

func (c CommandType) String() string {
	return string(c)
}


type ApplicationNodeConfigType struct {
	AstraConfig handling.AstraConfigType
	NATSEndpoint  string
	IsMaster   bool
	NodeName NodeNameType
	MasterRestPort int
}



type applicationNodeType struct {
	config   ApplicationNodeConfigType
	ctx context.Context
	ctxCancelFunc context.CancelFunc

	encodedConn *nats.EncodedConn
	commandSubscription  *nats.Subscription
	//
	workerMux            sync.RWMutex
	workers              map[string]WorkerInterface
	//
	logger *logrus.Logger
}

type slaveApplicationNodeType struct {
	*applicationNodeType
}

type masterApplicationNodeType struct{
	*applicationNodeType
	slaveCommandSubjects []string
}

/*
err = errors.Wrapf(err,"could not reply of closing  %v worker",id )
node.logger.Error(err)
return err
}
err = errors.Wrapf(err, "could not flush published reply of closing  %v worker", id)
reply of closing  %v worker", id)


func (node *ApplicationNodeType) publishReplay(subj string, msg *CommandMessageType) (err error) {
	err = node.encodedConn.Publish(subj, msg)
	if err != nil {
		err = errors.Wrap(err, "could not publish reply")
		node.logger.Error(err)
		return err
	}
	return
}


err = node.encodedConn.Flush()
	if err != nil {
		err = errors.Wrap(err, "could not flush published reply")
		node.logger.Error(err)
		return err
	}
	if err = node.encodedConn.LastError(); err != nil {
		err = errors.Wrap(err, "error while wiring flushed replay")
		node.logger.Error(err)
		return err
	}
	return*/

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
		err = fmt.Errorf("could not get local host name: %v",err)
		return
	}


	if cfg.IsMaster {
		master = &masterApplicationNodeType{
			applicationNodeType:instance ,
		}
		master.config.NodeName = "MASTER"
	} else {
		if cfg.NodeName == "" {
			logger.Fatal("Provide node name!")
		}
		slave = &slaveApplicationNodeType{
			applicationNodeType:instance ,
		}
	}


	ciMux.Lock()
	if currentInstance != nil {
		ciMux.Unlock()
		err = errors.New("current application node has already been initialized")
		logger.Fatal(err)
	} else {
		if cfg.IsMaster{
			currentMaster = master
			currentInstance = master.applicationNodeType
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
	if  err != nil {
		logger.Fatal(err)
	}
	currentInstance.logger.Info("AppNode instance started...")

	runtime.Goexit()

	return
}


func (node *applicationNodeType) NodeId() string {
	return string(node.config.NodeName)
}

func (node *applicationNodeType) connectToNATS() (err error) {
	conn, err := nats.Connect(
		node.config.NATSEndpoint,
		nats.Name(string(node.config.NodeName)),
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






/*
func (node ApplicationNodeType) OpenChannel(tableId, splitId int64) {
	num := int(math.Remainder(float64(splitId), float64(len(node.slaves))))
	index := 0
	for _, v := range node.slaves {
		if index == num {
			resp := new(CommandMessageType)
			err := node.encodedConn.Request(
				v.CommandSubject,
				CommandMessageType{
					Command: CATEGORY_SPLIT_OPEN,
					Params: map[CommandMessageParamType]interface{}{
						"tableInfoId": tableId,
						"splitId":     splitId,
					},
				},
				resp,
				nats.DefaultTimeout,
			)

			err = node.encodedConn.Flush()

			if err = node.encodedConn.LastError(); err != nil {

			}
			subj := resp.ParamString("workerSubject","")
			_= subj
			break
		}
		index++
	}

}
*/

/*
func (node *ApplicationNodeType) Finish() (err error) {
	if node.closers != nil {
		for _, c := range node.closers {
			err = c.Close()
			if err != nil {
				return err
			}
		}
	}
	return
}
*/
