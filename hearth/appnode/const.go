package appnode

import "github.com/ovlad32/wax/hearth/appnode/command"

const (
	masterNodeId Id = "MASTER"

	slaveIdParam             command.Param = "slaveId"
	slaveCommandSubjectParam command.Param = "slaveCommandSubject"
	slaveResubscribedParam   command.Param = "slaveResubscribed"
	distributedTaskIdParam   command.Param = "distTaskId"
	workerSubjectParam       command.Param = "workerSubject"
	workerIdParam            command.Param = "workerId"
	errorParam               command.Param = "error"
)
