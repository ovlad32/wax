package appnode




/*
func (n *node) startServices() (err error) {

	if n.config.IsMaster {
		_, err = repository.Init(hearth.AdaptRepositoryConfig(&n.config.AstraConfig))
		if err != nil {
			return err
		}
	}


	err = node.registerCommandProcessors()
	if err != nil {
		err = errors.Wrapf(err, "could register command processors")
		return err
	}


	err = node.ConnectToNATS(node.config.NATSEndpoint, node.config.NodeId)
	if err != nil {
		return
	}
	node.Logger().Info("Node has been connected to NATS...")

	node.commandSubscription, err = node.SubscribeMessageTrigger(
		MasterCommandSubject(),
		node.commandTriggers(),
	)

	if err != nil {
		err = errors.Wrapf(err, "could not create Master subscription")
		return
	}
	node.Logger().Info("Master command subscription has been created"	)

	server, err := node.initRestApiRouting()
	if err != nil {
		return err
	}

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt, os.Kill)

	go func() {
		_ = <-osSignal

		if err = server.Shutdown(context.Background()); err != nil {
			err = errors.Wrapf(err, "could not shutdown REST server")
			node.Logger().Error(err)
		}

		//err = node.closeAllCommandSubscription()
		if err != nil {
			err = errors.Wrapf(err, "could not close command subscriptions")
			node.Logger().Error(err)
		}

		repository.Close()

		node.Logger().Warn("Master node shut down")
		os.Exit(0)
	}()
	return
}


func (node *MasterNode) registerCommandProcessors() (err error) {
	node.commandFuncMap[parishRegister] = node.onParishRegister()

	return
}
*/
