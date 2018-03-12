package appnode



func (node *SlaveNode) initNATSService() (err error) {

	err = node.connectToNATS()
	if err != nil {
		return err
	}
	err = node.makeCommandSubscription()
	if err != nil {
		return err
	}
	return
}
