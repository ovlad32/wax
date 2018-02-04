package appnode

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"math"
	"sync"
)

const (
	parishSubjectName = "parish"
	masterNodeName    = "WAX-Master"
)

type NodeNameType string
type CommandType string

const (
	PARISH_OPEN  CommandType = "PARISH.CREATE"
	PARISH_CLOSE CommandType = "PARISH.CLOSE"

	CATEGORY_SPLIT_OPEN   CommandType = "SPLIT.CREATE"
	CATEGORY_SPLIT_OPENED CommandType = "SPLIT.CREATED"
	CATEGORY_SPLIT_CLOSE  CommandType = "SPLIT.CLOSE"
	CATEGORY_SPLIT_CLOSED CommandType = "SPLIT.CLOSED"
)

func (c CommandType) String() string {
	return string(c)
}

type SlaveNodeInfoType struct {
	CommandSubject            string
	MasterCommandSubscription *nats.Subscription
}

type ApplicationNodeConfigType struct {
	RestPort int
	Logger   *logrus.Logger
	//
	NatsUrl  string
	Master   bool
	NodeName NodeNameType
}



type ApplicationNodeType struct {
	config   ApplicationNodeConfigType
	RestPort int

	enc *nats.EncodedConn

	slavesMux           sync.RWMutex
	slaves              map[NodeNameType]*SlaveNodeInfoType
	commandSubscription *nats.Subscription
	commandSessions     map[string]interface{}
	agents              map[string]WorkerInterface
}

func NewApplicationNode(cfg *ApplicationNodeConfigType) (result *ApplicationNodeType, err error) {

	result = &ApplicationNodeType{
		config: *cfg,
	}
	if result.config.Master {
		result.config.NodeName = masterNodeName
	} else {
		if result.config.NodeName == "" {
			panic("provide node name!")
		}
	}

	/*	result.hostName, err = os.Hostname()
		if err != nil {
			err = fmt.Errorf("could not get local host name: %v",err)
			return nil,err
		}*/

	return
}


func (node ApplicationNodeType) NodeCommandSubject() string {
	return fmt.Sprintf("%v/command", node.config.NodeName)
}

func (node *ApplicationNodeType) Config() (result ApplicationNodeConfigType) {
	return node.config
}

func (node *ApplicationNodeType) ConnectToNats() (err error) {
	conn, err := nats.Connect(
		node.config.NatsUrl,
		nats.Name(string(node.config.NodeName)),
	)
	if err != nil {
		err = errors.Wrapf(err, "could not connect to NATS at %v", node.config.NatsUrl)
		return err
	}
	node.enc, err = nats.NewEncodedConn(conn, nats.GOB_ENCODER)
	if err != nil {
		err = errors.Wrapf(err, "could not create encoder for NATS")
		logrus.Fatal(err)
		return err
	}
	if node.config.Master {
		node.config.Logger.Info(
			"Master node connected to NATS",
		)
	} else {
		node.config.Logger.Info(
			"Slave %v connected to NATS", node.config.NodeName,
		)
	}

	return
}


func (node ApplicationNodeType) OpenChannel(tableId, splitId int64) {
	num := int(math.Remainder(float64(splitId), float64(len(node.slaves))))
	index := 0
	for _, v := range node.slaves {
		if index == num {
			resp := new(CommandMessageType)
			err := node.enc.Request(
				v.CommandSubject,
				CommandMessageType{
					Command: CATEGORY_SPLIT_OPEN,
					Parm: map[string]interface{}{
						"tableInfoId": tableId,
						"splitId":     splitId,
					},
				},
				resp,
				nats.DefaultTimeout,
			)

			err = node.enc.Flush()

			if err = node.enc.LastError(); err != nil {

			}
			subj := resp.ParmString("workerSubject","")
			_= subj
			break
		}
		index++
	}

}


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
