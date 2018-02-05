package appnode

import (
	"github.com/pkg/errors"
	"os"
	"os/signal"
	"context"
)


const MASTER_COMMAND_SUBJECT  = "COMMAND.MASTER"

func (node *masterApplicationNodeType) startServices() (err error) {


	err = node.initNatsService()

	if err != nil {
		err = errors.Wrapf(err,"could not start NATS service")
		return err
	}


	server,err := node.initRestApiRouting()



	osSignal := make(chan os.Signal,1)
	signal.Notify(osSignal, os.Interrupt,os.Kill)
	go func() {
			_ = <-osSignal

			if err = server.Shutdown(context.TODO()); err !=nil{
				err = errors.Wrapf(err,"could not shutdown REST server")
				node.logger.Error(err)
			}
			err = node.closeAllCommandSubscription()
			if err !=nil {
				err = errors.Wrapf(err,"could not close command subscriptions")
				node.logger.Error(err)
			}
			node.logger.Warn("Master node shut down")
			os.Exit(0)
	}()
	return
}

