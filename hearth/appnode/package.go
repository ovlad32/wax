package appnode

import (
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"sync"
)


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

	encodedConn *nats.EncodedConn

	workerMux            sync.RWMutex
	slaveCommandSubjects []string
	commandSubscription  *nats.Subscription
	workers              map[string]WorkerInterface
	//
	logger *logrus.Logger
}

/*
err = errors.Wrapf(err,"could not reply of closing  %v worker",id )
node.logger.Error(err)
return err
}
err = errors.Wrapf(err, "could not flush published reply of closing  %v worker", id)
reply of closing  %v worker", id)

*/
func (node *ApplicationNodeType) publishReplay(subj string, msg *CommandMessageType) (err error) {
	err = node.encodedConn.Publish(subj, msg)
	if err != nil {
		err = errors.Wrap(err, "could not publish reply")
		node.logger.Error(err)
		return err
	}
	return
}

/*
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
func NewApplicationNode(cfg *ApplicationNodeConfigType) (result *ApplicationNodeType, err error) {

	result = &ApplicationNodeType{
		config: *cfg,
	}
	if result.config.Master {
		result.config.NodeName = "MASTER"
	} else {
		if result.config.NodeName == "" {
			panic("provide node name!")
		}
	}
	result.logger = cfg.Logger

	/*	result.hostName, err = os.Hostname()
		if err != nil {
			err = fmt.Errorf("could not get local host name: %v",err)
			return nil,err
		}*/

	return
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
	node.encodedConn, err = nats.NewEncodedConn(conn, nats.GOB_ENCODER)
	if err != nil {
		err = errors.Wrapf(err, "could not create encoder for NATS")
		logrus.Fatal(err)
		return err
	}
	if node.config.Master {
		node.logger.Info(
			"Master node connected to NATS",
		)
	} else {
		node.logger.Info(
			"Slave %v connected to NATS", node.config.NodeName,
		)
	}

	return
}


func (node *ApplicationNodeType) Config() (result ApplicationNodeConfigType) {
	return node.config
}

func (node *ApplicationNodeType) NodeId() string {
	return string(node.config.NodeName)
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
