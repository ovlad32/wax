package appnode

import (
	"os"
	"github.com/pkg/errors"
)

const (
	slaveCommandSubjectParam CommandMessageParamType = "slaveCommandSubject"
	slaveIdParam             CommandMessageParamType = "slaveId"
	slaveResubscribedParam        CommandMessageParamType = "slaveResubscribed"

	parishOpen   CommandType = "PARISH.OPEN"
	//parishOpened CommandType = "PARISH.OPENED.S"
	//parishClose  CommandType = "PARISH.CLOSE"
	//parishClosed CommandType = "PARISH.CLOSED.S"
	parishStopWorker CommandType = "PARISH.CANCEL.JOB"
)



func (node masterApplicationNodeType) parishOpenFunc() commandProcessorFuncType {
	return func(subject,replySubject string, msg *CommandMessageType) (err error) {
		slaveCommandSubject := msg.ParamSubject(slaveCommandSubjectParam)
		if slaveCommandSubject.IsEmpty() {
			node.logger.Warn("gotten slave command subject is empty!")
			return
		}
		slaveId := msg.ParamNodeId(slaveIdParam)
		if slaveId.IsEmpty() {
			node.logger.Warn("gotten slaveId is empty!")
			return
		}

		node.logger.Infof("Start registering new Slave '%v' with command subject '%v'", slaveId, slaveCommandSubject)


		params := NewCommandMessageParams(3)
		node.slaveCommandMux.RLock()
		if prev, found := node.slaveCommandSubjects[slaveId]; found {
			node.slaveCommandMux.RUnlock()
			params.Append(slaveResubscribedParam,true)
			node.logger.Infof(
				"New Slave '%v' with command subject '%v' had been registered previously with '%v'",
				slaveId,
				slaveCommandSubject,
				prev,
			)
		} else {
			node.slaveCommandMux.RUnlock()
		}
		err = node.PublishCommandResponse(replySubject, msg.Command, params...)
		if err != nil {
			err = errors.Wrapf(err, "could not reply of opening a new slave")
			return
		}
		node.slaveCommandMux.Lock()
		node.slaveCommandSubjects[slaveId] = slaveCommandSubject
		node.slaveCommandMux.Unlock()
		node.logger.Infof("A new node with command subject '%v' has been successfully registered", slaveCommandSubject)
		return
	}

}




func (node slaveApplicationNodeType) parishStopWorkerFunc() commandProcessorFuncType {
	return func(subject,replySubject string, msg *CommandMessageType) (err error) {
		node.logger.Warnf("Slave '%v': Shutdown signal received", node.NodeId())

		//of stopping worker on

		workerId := msg.ParamWorkerId(workerIdParam);
		worker := node.FindWorker(workerId)
		if worker != nil {
			worker.TaskCanceled();
		}

		if node.commandSubscription != nil {
			err := node.commandSubscription.Unsubscribe()
			if err != nil {
				node.logger.Error(err)
			}
		}
		/*node.encodedConn.Publish(
			replySubject,
			&CommandMessageType{
				Command: parishClosed,
			})
		err := node.CloseRegularWorker(
			reply,
			msg,
			parishClosed,
		)
		if err != nil {
			panic(err.Error())
		}*/
		node.encodedConn.Close()
		os.Exit(0)
		return
	}
}
