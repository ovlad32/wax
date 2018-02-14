package appnode

const (
	slaveCommandSubjectParam CommandMessageParamType = "slaveCommandSubject"
	slaveIdParam             CommandMessageParamType = "slaveId"
	ResubscribedParam        CommandMessageParamType = "resubscribed"
)

func (node *slaveApplicationNodeType) initNATSService() (err error) {

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
