package appnode

import (
	"github.com/sirupsen/logrus"
	"os"
	"fmt"
)

type ApplicationNodeConfigType struct {
	Port int
	Log logrus.FieldLogger
}
type ApplicationNodeType struct {
	config ApplicationNodeConfigType
	hostName string
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
