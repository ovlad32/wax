package appnode

import (
	"context"
	"github.com/ovlad32/wax/hearth"
	"github.com/ovlad32/wax/hearth/repository"
	"github.com/pkg/errors"
	"os"
	"os/signal"
	"sync"
)

const masterCommandSubject = "COMMAND.MASTER"


type masterApplicationNodeType struct {
	*applicationNodeType
	slaveCommandMux sync.RWMutex
	slaveCommandSubjects map[NodeIdType]SubjectType
}



func (node *masterApplicationNodeType) startServices() (err error) {

	_, err = repository.Init(hearth.AdaptRepositoryConfig(&node.config.AstraConfig))
	if err != nil {
		return err
	}


	err = node.registerCommandProcessors()

	if err != nil {
		err = errors.Wrapf(err, "could register command processors")
		return err
	}


	err = node.initNATSService()

	if err != nil {
		return err
	}

	server, err := node.initRestApiRouting()
	if err != nil {
		return err
	}

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt, os.Kill)
	go func() {
		_ = <-osSignal
		if node.ctxCancelFunc != nil {
			node.ctxCancelFunc()
		}

		if err = server.Shutdown(context.Background()); err != nil {
			err = errors.Wrapf(err, "could not shutdown REST server")
			node.logger.Error(err)
		}

		err = node.closeAllCommandSubscription()
		if err != nil {
			err = errors.Wrapf(err, "could not close command subscriptions")
			node.logger.Error(err)
		}

		repository.Close()

		node.logger.Warn("Master node shut down")
		os.Exit(0)
	}()
	return
}


func (node *masterApplicationNodeType) registerCommandProcessors() (err error) {
	node.commandProcessorsMap[parishOpen] = node.parishOpenFunc()
	return
}

