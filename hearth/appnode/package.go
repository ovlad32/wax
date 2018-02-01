package appnode

import (
	"github.com/sirupsen/logrus"
	"fmt"
	"sync"
	"github.com/nats-io/go-nats"
	"io"
)



const (
	parishSubjectName = "parish"
	masterNodeName = "WAX/Master"

)

type ApplicationNodeConfigType struct {
	GrpcPort int
	RestPort int
	SlaveHeartBeatSeconds int
	Logger *logrus.Logger
	//
	NatsUrl string
	Master bool
	NodeName string
}

type ApplicationNodeType struct {
	config ApplicationNodeConfigType
	slavesMux sync.Mutex
	slaves map[string]string
	closers []io.Closer
}






func NewApplicationNode(cfg *ApplicationNodeConfigType) (result *ApplicationNodeType,err error) {

	result = &ApplicationNodeType{
		config:*cfg,
	}
	result.closers = make([]io.Closer,0,3)
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

type parishHandlerType struct {
	subs *nats.Subscription
	imc chan ParishRequestType
}

func (t *parishHandlerType) Close() (err error) {
	err = t.subs.Unsubscribe()
	if err != nil {
	}
	if t.imc != nil {
		close(t.imc)
	}
	return
}


func (node *ApplicationNodeType) Start() (err error) {
	fmt.Println(node.config.NodeName)
	conn, err := nats.Connect(
		node.config.NatsUrl,
		nats.Name(node.config.NodeName),
		)
	if err != nil {
		//TODO:
		logrus.Fatal(err)
		return err
	}
	enc, err  := nats.NewEncodedConn(conn,nats.GOB_ENCODER)
	if err != nil {
		//TODO:
		logrus.Fatal(err)
		return err
	}


	if node.config.Master {
		//c := make(chan(ParishRequestType))

		s, err := enc.Subscribe(
			parishSubjectName,
			func(subj, reply string, msg *ParishRequestType) {
				resp := ParishResponseType{}
				node.slavesMux.Lock()
				if node.slaves == nil {
					node.slaves = make(map[string]string)
				}
				if oldsubj,found := node.slaves[msg.SlaveNodeName]; found {
					_ = oldsubj
					resp.Err = fmt.Errorf("already exists")
				} else {
					node.slaves[msg.SlaveNodeName] = msg.CommandSubjet
					resp.Registered = true
					logrus.Infof("%v %v added ",msg.SlaveNodeName,msg.CommandSubjet)
				}
				node.slavesMux.Unlock()
				err = enc.Publish(reply,resp)
				if err != nil {
					logrus.Fatal(err)
				}
			},
		)

		if err != nil {
			//TODO:
			return err
		}
		enc.Flush()
		h := &parishHandlerType{
			//imc:c,
			subs:s,
		}
		node.closers = append(node.closers,h)

		/*go func() {
			for {
				select {
					case msg,opened := <-c:
						if !opened {
							return
						}
						node.slavesMux.Lock()
						if node.slaves == nil {
							node.slaves = make(map[string]string)
						}
						if oldsubj,found := node.slaves[msg.SlaveNodeName]; found {
								_ = oldsubj
						} else {
							node.slaves[msg.SlaveNodeName] = msg.CommandSubjet
							logrus.Infof("%v %v added ",msg.SlaveNodeName,msg.CommandSubjet)
						}
						node.slavesMux.Unlock()
				}
			}
		} ()*/
	} else {
		commandSubject := fmt.Sprintf("%v/command")
		resp := &ParishResponseType{}
		err = enc.Request(
			parishSubjectName,
			ParishRequestType{
				SlaveNodeName:node.config.NodeName,
				CommandSubjet:commandSubject,
			},
			resp,
			nats.DefaultTimeout,
		)
		fmt.Println(resp.Registered,resp.Err)
		if !resp.Registered {
			if resp.Err != nil {
				logrus.Fatal(resp.Err)
			} else {
				logrus.Fatal(fmt.Errorf("No response for %v",node.config.NodeName))
			}
		}
		//fmt.Println(resp.err)

	}
	err = enc.Flush()
	if err != nil {
		//TODO:
		logrus.Fatal(err)
		return err
	}

	if err = enc.LastError(); err!=nil {
		//TODO:
		logrus.Fatal(err)
		return err
	}

	return
}

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