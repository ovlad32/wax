package appnode

func (node *ApplicationNodeType) StartMasterNode() (err error) {
	node.wg.Add(1)
	err = node.initAppNodeService()

	if err != nil {
		return err
	}


	node.initRestApiRouting()
	node.wg.Wait()

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

