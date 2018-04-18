package di

import (
	"github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"github.com/ovlad32/wax/hearth/appnode/natsu"
)

type LogKeeper interface {
	Logger() (*logrus.Logger)
}

type NatsEncodedConner interface {
	Enc() *nats.EncodedConn
}
type NatsCommunicable interface {
	SetCommunication(*natsu.Communication)
}
type NodeIdInjectable interface {
	SetNodeId(string)
}


type Runner interface {
	Run() error
}
