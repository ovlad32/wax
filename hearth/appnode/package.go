package appnode

import (
	"github.com/sirupsen/logrus"
	"os"
	"fmt"
	"sync"
)

type ApplicationNodeConfigType struct {
	GrpcPort int
	RestPort int
	SlaveHeartBeatSeconds int
	Logger *logrus.Logger
}
type ApplicationNodeType struct {
	config ApplicationNodeConfigType
	hostName string
	wg sync.WaitGroup
}


func NewApplicationNode(cfg *ApplicationNodeConfigType) (result *ApplicationNodeType,err error) {

	result = &ApplicationNodeType{
		config:*cfg,
	}

	result.hostName, err = os.Hostname()
	if err != nil {
		err = fmt.Errorf("could not get local host name: %v",err)
		return nil,err
	}

	return
}

func (node *ApplicationNodeType) Config() (result ApplicationNodeConfigType) {
	return node.config
}
