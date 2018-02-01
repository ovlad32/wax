package appnode

func (node *ApplicationNodeType) StartMasterNode() (err error) {
	//err = node.initAppNodeService()

	if err != nil {
		return err
	}


	node.initRestApiRouting()

	/*
		osSignal := make(chan os.Signal,1)
		signal.Notify(osSignal,os.Interrupt)
		func() {
			for {
				select {
					_ = <-osSignal

				}
			}
		}
	*/
	return
}





