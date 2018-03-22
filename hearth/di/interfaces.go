package di

import (
	"github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
)

type LogKeeper interface {
	Logger() (*logrus.Logger)
}

type NatsEncodedConner interface {
	Enc() *nats.EncodedConn
}
