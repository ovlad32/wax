package appnode

import (
	"github.com/sirupsen/logrus"
	"fmt"
	"sync"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
)



const (
	parishSubjectName = "parish"
	masterNodeName = "WAX-Master"
)

type NodeNameType string

type SlaveNodeInfoType struct {
	CommandSubject string
	MasterCommandSubscription *nats.Subscription
}


type ApplicationNodeConfigType struct {
	RestPort int
	Logger *logrus.Logger
	//
	NatsUrl string
	Master bool
	NodeName NodeNameType
}

func (node ApplicationNodeType) NodeCommandSubject() string {
	return fmt.Sprintf("%v/command",node.config.NodeName)
}


type ApplicationNodeType struct {
	config ApplicationNodeConfigType
	RestPort int

	enc *nats.EncodedConn

	slavesMux sync.Mutex
	slaves map[NodeNameType]*SlaveNodeInfoType
	commandSubscription *nats.Subscription
}






func NewApplicationNode(cfg *ApplicationNodeConfigType) (result *ApplicationNodeType,err error) {

	result = &ApplicationNodeType{
		config:*cfg,
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




func (node *ApplicationNodeType) Config() (result ApplicationNodeConfigType) {
	return node.config
}

func(node *ApplicationNodeType) ConnectToNats() (err error) {
	conn, err := nats.Connect(
		node.config.NatsUrl,
		nats.Name(string(node.config.NodeName)),
	)
	if err != nil {
		err = errors.Wrapf(err,"could not connect to NATS at %v",node.config.NatsUrl)
		return err
	}
	node.enc, err = nats.NewEncodedConn(conn,nats.GOB_ENCODER)
	if err != nil {
		err = errors.Wrapf(err,"could not create encoder for NATS")
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
func (node *ApplicationNodeType) MakeMasterCommandSubscription() (err error){

	if !node.config.Master {
		panic(fmt.Sprintf("AppNode is NOT Master",))
	}

	node.commandSubscription, err = node.enc.Subscribe(
		parishSubjectName,
		node.MasterCommandSubscriptionFunc(),
	)

	err= node.enc.Flush();
	if err != nil {
		err = errors.Wrapf(err,"Error while subscription being flushed")
		node.config.Logger.Error(err)
	}

	if err = node.enc.LastError(); err!=nil {
		err = errors.Wrap(err,"error given via NATS while making Master command subscription")
		node.config.Logger.Error(err)
	}
	node.config.Logger.Info(
		"Master command subscription has been created",
	)
	return
}

func (node *ApplicationNodeType) MasterCommandSubscriptionFunc() (func(string, string, *ParishRequestType)) {
	return func(subj, reply string, msg *ParishRequestType) {
			resp := ParishResponseType{}
			node.slavesMux.Lock()
			if node.slaves == nil {
				node.slaves = make(map[NodeNameType]*SlaveNodeInfoType)
			}
			if prevInfo,found := node.slaves[msg.SlaveNodeName]; !found {
				_ = prevInfo
				resp.ReConnect = true
				node.config.Logger.Infof(
					"Slave %v had been registered before. Registered again",
					msg.SlaveNodeName,
					msg.CommandSubject,
					)
			} else {
				node.slaves[msg.SlaveNodeName] = &SlaveNodeInfoType{
					CommandSubject:msg.CommandSubject,
				}
				node.config.Logger.Infof(
					"Slave %v has been registered with command subject %v",
					msg.SlaveNodeName,
					msg.CommandSubject,
					)
			}
			node.slavesMux.Unlock()
	}
}


func (node *ApplicationNodeType) MakeSlaveCommandSubscription() (err error){
	resp := &ParishResponseType{}
	err = node.enc.Request(
		parishSubjectName,
		ParishRequestType{
			SlaveNodeName:node.config.NodeName,
			CommandSubject:node.NodeCommandSubject(),
		},
		resp,
		nats.DefaultTimeout,
	)
	if err != nil {
		err = errors.Wrapf(err,"could not register slave %v command subject")
		node.config.Logger.Error(err)
		return
	}
	err = node.enc.Flush()
	if err != nil {
		err = errors.Wrapf(err, "could not flush registration request")
		node.config.Logger.Error(err)
		return
	}

	if err = node.enc.LastError();err != nil {
		err = errors.Wrap(err,"error given via NATS while making slave registration command subscription")
		node.config.Logger.Error(err)
		return
	}

	node.config.Logger.Info("Slave %v registration has been done")


	if resp.ReConnect {
		//Todo: clean
	}


	node.commandSubscription, err = node.enc.Subscribe(
		node.NodeCommandSubject(),
		node.SlaveCommandSubscriptionFunc(),
	)
	var nodeName string = string(node.config.NodeName)
	if err != nil {
		err = errors.Wrapf(err,"could not subscribe slave %v command subject",nodeName)
		node.config.Logger.Error(err)
		return
	}

	err = node.enc.Flush()
	if err != nil {
		err = errors.Wrapf(err, "could not flush slave %v command subscription",nodeName )
		node.config.Logger.Error(err)
		return
	}

	if err = node.enc.LastError();err != nil {
		err = errors.Wrapf(err,"error given via NATS while slave %v command subscribing",node)
		node.config.Logger.Error(err)
		return
	}
	node.config.Logger.Info("Slave %v command subscription has been done")
	return
}


func (node *ApplicationNodeType) SlaveCommandSubscriptionFunc() (func(string, string, interface{})) {
	return func(subj, reply string, msg interface{}) {
		panic("not implemented yet")
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