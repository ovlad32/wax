package appnode

import (
	"github.com/pkg/errors"
	"os"
	"os/signal"
)

func (node *slaveApplicationNodeType) startServices() (err error) {

	err = node.initNatsService()

	if err != nil {
		err = errors.Wrapf(err, "could not start NATS service")
		return err
	}

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt, os.Kill)
	go func() {
		_ = <-osSignal

		//err = node.closeAllCommandSubscription()
		if err != nil {
			err = errors.Wrapf(err, "could not close command subscriptions")
			node.logger.Error(err)
		}
		node.logger.Warnf("Slave '%v' shut down", node.NodeId())
		os.Exit(0)
	}()
	return
}

func (node *slaveApplicationNodeType) registerMaxPayloadSize(maxLoadedMsg *CommandMessageType) (adjustment int64, err error) {
	subject := "a subject length does not matter now, so make it arbitrary size but long enough"
	encoded, err := node.encodedConn.Enc.Encode(subject, maxLoadedMsg)
	if err != nil {
		err = errors.Wrapf(err, "could not encode command message %v", maxLoadedMsg.Command)
		return
	}
	adjustment = node.encodedConn.Conn.MaxPayload() - int64(len(encoded))
	node.payloadSizeAdjustments[maxLoadedMsg.Command] = adjustment
	return
}
