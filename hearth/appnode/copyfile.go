package appnode

import (
	"bufio"
	"context"
	"github.com/nats-io/go-nats"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/repository"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
)

type copyFileWorkerType struct {
	basicWorkerType
	file              io.ReadWriteCloser
	pathToFile        string
	distributedTaskId int64
}
type copyFileReader struct {
	copyFileWorkerType
	fileSize int64
}
type copyFileWriter struct {
	bytesWritten   int
	pathToTempFile string
	copyFileWorkerType
}

const filePathParam CommandMessageParamType = "filePathParam"
const fileSizeParam CommandMessageParamType = "fileSizeParam"
const fileNotExistsParam CommandMessageParamType = "fileNotExistsParam"

const fileStats CommandType = "FILE.STATS"

const copyFileOpen CommandType = "CMD.COPYFILE.OPEN"
const copyFileCreate CommandType = "CMD.COPYFILE.CREATE"
const copyFileData CommandType = "WRK.COPYFILE.DATA"
const copyFileError CommandType = "CMD.COPYFILE.ERROR"
const copyFileLaunch CommandType = "CMD.COPYFILE.LAUNCH"
const copyFileTerminate  CommandType = "CMD.COPYFILE.TERMINATE"

const (
	copyFileDataParam CommandMessageParamType = "data"
	copyFileEOFParam  CommandMessageParamType = "EOF"
)

func (node masterApplicationNodeType) copyFile(
	srcNodeId,
	srcFile,
	tgtNodeId,
	tgtFile string,
) (subject string, err error) {
	var distTask *dto.DistTaskType
	sourceNodeId := NodeIdType(srcNodeId)
	if sourceNodeId.IsEmpty() {
		//TODO:
	}

	targetNodeId := NodeIdType(tgtNodeId)
	if sourceNodeId.IsEmpty() {
		//TODO:
	}
	ctx := context.Background()
	var sourceResp, targetResp *CommandMessageType
	var targetDataSubject SubjectType
	var sourceWorkerId, targetWorkerId WorkerIdType
	var sourceFileSize int64

	// file statistics
	if sourceResp, err = node.RequestCommandBySubject(
		sourceNodeId.CommandSubject(),
		fileStats,
		NewCommandMessageParams(1).
			Append(filePathParam, srcFile)...,
	); err != nil {
		//TODO:
		err = errors.Wrapf(err, "couldn't make a request to source node")
		node.logger.Error(err)
		return
	}

	if err = func() (err error) {
		fileNotExists := sourceResp.ParamBool(fileNotExistsParam, false)
		if fileNotExists {
			err = errors.Errorf("%v: source file does not exist: %v", sourceResp.Command, srcFile)
			return
		}
		sourceFileSize = sourceResp.ParamInt64(fileSizeParam, -1)
		if sourceFileSize == -1 {
			err = errors.Errorf("%v: fileSizeParam parameter is absent", sourceResp.Command)
			return
		} else if sourceFileSize == 0 {
			err = errors.Errorf("%v: source file is empty: %v", sourceResp.Command, srcFile)
			return
		}
		return
	}(); err != nil {
		//TODO:
		node.logger.Error(err)
		return
	}

	// create file
	if targetResp, err = node.RequestCommandBySubject(
		targetNodeId.CommandSubject(),
		copyFileCreate,
		NewCommandMessageParams(2).
			Append(filePathParam, tgtFile).
			Append(fileSizeParam, sourceFileSize)...,
	); err != nil {
		//TODO:
		err = errors.Wrapf(err, "couldn't make a request to target node")
		node.logger.Error(err)
		return
	}

	if err = func() (err error) {
		targetDataSubject = targetResp.ParamSubject(workerSubjectParam)
		if targetDataSubject.IsEmpty() {
			err = errors.Errorf("%v: target writer subject is empty", targetResp.Command)
			return
		}

		targetWorkerId := targetResp.ParamWorkerId(workerIdParam)
		if targetWorkerId.IsEmpty() {
			err = errors.Errorf("%v: target writer worker id is empty", targetResp.Command)
			return
		}
		return
	}(); err != nil {
		//TODO:
		node.logger.Error(err)
		return
	}

	if sourceResp, err = node.RequestCommandBySubject(
		sourceNodeId.CommandSubject(),
		copyFileOpen,
		NewCommandMessageParams(3).
			Append(workerIdParam, targetWorkerId).
			Append(workerSubjectParam, targetDataSubject).
			Append(filePathParam, srcFile)...,
	); err != nil {
		//TODO KILL target worker
		//TODO:
		err = errors.Wrapf(err, "couldn't make a request to source node")
		node.logger.Error(err)
		return
	}

	if err = func() (err error) {
		sourceWorkerId = sourceResp.ParamWorkerId(workerIdParam)
		if sourceWorkerId.IsEmpty() {
			err = errors.Errorf("%v: source reader worker id is empty", sourceResp.Command)
			return
		}
		return
	}(); err != nil {
		//TODO KILL target worker
		//TODO:
		node.logger.Error(err)
		return
	}

	if err = func() (err error) {
		distTask = &dto.DistTaskType{
			Task:   "copyfile",
			Status: "init",
		}
		err = repository.PutDistTask(
			ctx,
			distTask,
		)
		if err != nil {
			return
		}
		err = repository.PutDistTaskNode(ctx,
			&dto.DistTaskNodeType{
				DistTaskId:  distTask.Id,
				DataSubject: targetDataSubject.String(),
				WorkerId:    targetNodeId.String(),
				NodeId:      tgtNodeId,
				Role:        "target",
			})
		if err != nil {
			return
		}
		err = repository.PutDistTaskNode(ctx,
			&dto.DistTaskNodeType{
				DistTaskId: distTask.Id,
				WorkerId:   sourceWorkerId.String(),
				NodeId:     sourceNodeId.String(),
				Role:       "source",
			})
		if err != nil {
			return
		}
		return
	}(); err != nil {
		//TODO KILL target&source workers
		//TODO:
		node.logger.Error(err)
		return
	}

	if err = node.PublishCommand(
		sourceNodeId.CommandSubject(),
		copyFileLaunch,
		NewCommandMessageParams(2).
			Append(workerIdParam, sourceWorkerId).
			Append(distributedTaskIdParam, distTask.Id.Value())...,
	); err != nil {
		//TODO KILL target&source workers
		//TODO:
		node.logger.Error(err)
		return
	}

	go func() {
		distTask.Status = "start"
		if err = repository.PutDistTask(ctx, distTask); err != nil {
			node.logger.Error(err)
		}
		return
	}()

	return string(distTask.Id.Value()), nil
}

func (node slaveApplicationNodeType) fileStatsCommandFunc() commandFuncType {
	return func(
		subject,
		replySubject string,
		incomingMessage *CommandMessageType,
	) (err error) {
		replyParams := NewCommandMessageParams(3)
		reply := func() (err error) {
			err = node.PublishCommandResponse(
				replySubject,
				fileStats,
				replyParams...,
			)
			if err != nil {
				err = errors.Wrapf(err, "could not publish %v response", fileStats)
				return
			}
			return
		}

		filePath := incomingMessage.ParamString(filePathParam, "")
		stats, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				replyParams = replyParams.Append(fileNotExistsParam, true)
			} else {
				err = errors.Wrapf(err, "could not get file statistics for %v", filePath)
				node.logger.Error(err)
				replyParams = replyParams.Append(errorParam, err)
			}
		} else {
			replyParams = replyParams.Append(fileSizeParam, stats.Size())
		}
		err = reply()
		return
	}
}

func (node *slaveApplicationNodeType) copyFileCreateCommandFunc() commandFuncType {
	return func(
		subject,
		replySubject string,
		incomingMessage *CommandMessageType,
	) (err error) {
		replyParams := NewCommandMessageParams(3)

		reply := func() (err error) {
			err = node.PublishCommandResponse(
				replySubject,
				copyFileCreate,
				replyParams...,
			)
			if err != nil {
				err = errors.Wrapf(err, "could not publish subscription %v response: ", copyFileCreate)
				return
			}
			return
		}

		logger := node.logger
		writer := &copyFileWriter{
			copyFileWorkerType: copyFileWorkerType{
				basicWorkerType: basicWorkerType{
					id:   newWorkerId(),
					node: node,
				},
			},
		}

		dataSubject := newPrefixSubject("CopyFileData")

		writer.pathToFile = incomingMessage.ParamString(filePathParam, "")
		fileSize := incomingMessage.ParamInt64(fileSizeParam, -1)

		if writer.pathToFile == "" {
			err = errors.New("path to target file is empty")
			logger.Error(err)
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}

		dir, _ := path.Split(writer.pathToFile)
		//TODO:FILESIZE!
		_ = fileSize

		var tempFile *os.File
		tempFile, err = ioutil.TempFile(dir, "tmp_")
		if err != nil {
			err = errors.Wrapf(err, "could not create temp file at %v:", dir)
			logger.Error(err)
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}

		writer.pathToTempFile = tempFile.Name()
		writer.file = tempFile

		writer.subscription, err = node.Subscribe(
			dataSubject,
			writer.subscriptionFunc(),
		)
		if err != nil {
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}

		replyParams = replyParams.Append(workerSubjectParam, dataSubject)
		replyParams = replyParams.Append(workerIdParam, writer.Id())
		err = reply()
		if err != nil {
			writer.Unsubscribe()
		} else {
			writer.taskCancelContext, writer.taskCancelFunc = context.WithCancel(context.Background())
			node.AppendWorker(writer)
		}
		return
	}

}

func (node *slaveApplicationNodeType) copyFileOpenCommandFunc() commandFuncType {
	return func(
		subject,
		replySubject string,
		incomingMessage *CommandMessageType,
	) (err error) {
		logger := node.logger

		replyParams := NewCommandMessageParams(3)

		reply := func() (err error) {
			err = node.PublishCommandResponse(
				replySubject,
				copyFileOpen,
				replyParams...,
			)
			if err != nil {
				err = errors.Wrapf(err, "could not publish subscription %v response: ", copyFileOpen)
				logger.Error(err)
				return
			}
			return
		}

		reader := &copyFileReader{
			copyFileWorkerType: copyFileWorkerType{
				basicWorkerType: basicWorkerType{
					id:   newWorkerId(),
					node: node,
				},
			},
		}

		reader.pathToFile = incomingMessage.ParamString(filePathParam, "")

		if reader.pathToFile == "" {
			err = errors.New("path to source file is empty")
			logger.Error(err)
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}

		reader.counterpartSubject = incomingMessage.ParamSubject(workerSubjectParam)
		if reader.counterpartSubject.IsEmpty() {
			err = errors.New("write-worker data subject is empty")
			logger.Error(err)
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}

		fileStats, err := os.Stat(reader.pathToFile)
		if err != nil {
			err = errors.Wrapf(err, "could not get source file information: %v", reader.pathToFile)
			node.logger.Error(err)
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}
		reader.fileSize = fileStats.Size()

		if reader.fileSize == 0 {
			err = errors.Errorf("Source file is empty: %v", reader.pathToFile)
			node.logger.Error(err)
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}
		reader.file, err = os.Open(reader.pathToFile)
		if err != nil {
			err = errors.Wrapf(err, "could not open source file: %v", reader.pathToFile)
			node.logger.Error(err)
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}

		replyParams = replyParams.Append(workerIdParam, reader.Id())
		err = reply()
		if err != nil {
		} else {
			reader.taskCancelContext, reader.taskCancelFunc = context.WithCancel(context.Background())
			node.AppendWorker(reader)
		}
		return
	}
}

func (node *slaveApplicationNodeType) copyFileLaunchCommandFunc() commandFuncType {
	return func(
		subject,
		replySubject string,
		incomingMessage *CommandMessageType,
	) (err error) {
		logger := node.logger

		pubParams := NewCommandMessageParams(1)

		publishError := func() (err error) {
			err = node.PublishCommand(
				MasterCommandSubject(),
				copyFileError,
				pubParams...,
			)
			if err != nil {
				err = errors.Wrapf(err, "could not publish subscription %v response: ", copyFileLaunch)
				return
			}
			return
		}
		distributedTaskId := incomingMessage.ParamInt64(distributedTaskIdParam, -1)
		if distributedTaskId == -1 {
			err = errors.New("read-worker didn't receive distributed task id")
			logger.Fatal(err)
			return
		}

		readerId := incomingMessage.ParamWorkerId(workerIdParam)
		if readerId.IsEmpty() {
			err = errors.New("read-worker id is empty")
			logger.Error(err)
			pubParams = pubParams.Append(errorParam, err)
			publishError()
			return
		}

		untyped := node.FindWorker(readerId)
		if untyped == nil {
			err = errors.Errorf("could not find registered worker by id: %v", readerId)
			logger.Error(err)
			pubParams = pubParams.Append(errorParam, err)
			publishError()
			return
		}

		reader, ok := untyped.(*copyFileReader)
		if !ok {
			err = errors.Errorf("could not cast found worker to copyFileReader")
			logger.Error(err)
			pubParams = pubParams.Append(errorParam, err)
			publishError()
			return
		}


		if err = func() (err error) {
			buf := bufio.NewReader(reader.file)
			var readBuffer []byte
			var dataSizeAdjusted = false
			var maxPayloadSize = node.encodedConn.Conn.MaxPayload()

			//* as long as message broker has litation on its payload size,
			//*  assume that we don't need any size adjustment if our file is a half of MaxPayload
			if dataSizeAdjusted = reader.fileSize < maxPayloadSize/2; dataSizeAdjusted {
				node.logger.Warnf("readBuffer allocated to the size of the sourceFile %v", reader.fileSize)
				readBuffer = make([]byte, reader.fileSize)
			} else {
				var adjustment int64
				node.payLoadSizeMux.RLock()
				adjustment, dataSizeAdjusted = node.payloadSizeAdjustments[copyFileData]
				node.payLoadSizeMux.RUnlock()
				readBuffer = make([]byte, maxPayloadSize - adjustment)
			}

			for {
				var eof bool
				var readCount int
				select {
				case _ = <-reader.taskCancelContext.Done():
					return
				default:
				readCount, err = buf.Read(readBuffer)
				if err != nil {
					if err != io.EOF {
						err = errors.Wrapf(err, "could not read source file %v", reader.pathToFile)
						node.logger.Error(err)
						return
					} else {
						eof = true
					}
				}

				replica := make([]byte, readCount)
				copy(replica, readBuffer[:readCount])

				message := &CommandMessageType{
					Command: copyFileData,
					Params: CommandMessageParamMap{
						copyFileDataParam:      replica,
						copyFileEOFParam:       eof,
						workerIdParam:          reader.id,
						distributedTaskIdParam: reader.distributedTaskId,
					},
				}
				if !dataSizeAdjusted {
					var adjustment int64
					adjustment, err = node.registerMaxPayloadSize(message)
					if err != nil {
						return
					}
					newSize := maxPayloadSize + adjustment
					node.logger.Infof("copyFile message adjustment is %v", adjustment)
					cutMessage := &CommandMessageType{
						Command: copyFileData,
						Params: CommandMessageParamMap{
							copyFileDataParam:      replica[0:newSize],
							copyFileEOFParam:       eof,
							workerIdParam:          reader.id,
							distributedTaskIdParam: reader.distributedTaskId,
						},
					}
					if err = node.encodedConn.Publish(reader.counterpartSubject.String(), cutMessage); err != nil {
						err = errors.Wrapf(err, "could not publish cut copyFile message")
						node.logger.Info(err)
						return
					}
					replica = replica[newSize:]
					readBuffer = readBuffer[0:newSize]
					node.logger.Warnf("readBuffer has been cut to %v bytes", newSize)
					dataSizeAdjusted = true
					message = &CommandMessageType{
						Command: copyFileData,
						Params: CommandMessageParamMap{
							copyFileDataParam:      replica,
							copyFileEOFParam:       eof,
							workerIdParam:          reader.id,
							distributedTaskIdParam: reader.distributedTaskId,
						},
					}
				}

				if err = node.encodedConn.Publish(reader.counterpartSubject.String(), message); err != nil {
					err = errors.Wrapf(err, "could not publish copyFileData message")
					node.logger.Error(err)
					return
				}

				if eof {
					if err = node.encodedConn.Flush(); err != nil {
						err = errors.Wrapf(err, "could not flush copyFileData message")
						node.logger.Error(err)
						return
					}

					if err = node.encodedConn.LastError(); err != nil {
						err = errors.Wrapf(err, "error while wiring copyFileData message")
						node.logger.Error(err)
						return
					}
					break;
				}
			}
			return
			}
		}(); err != nil {
			errc := node.PublishCommand(
				reader.counterpartSubject,
				copyFileError,
			)
			if errc != nil {
				errc = errors.Wrapf(errc, "could not send copyFileData failure command")
				reader.logger().Error(errc)
			}
		}
		reader.CloseFile()
		reader.Unsubscribe()
		node.RemoveWorker(reader)

		return
	}
}


func (node *slaveApplicationNodeType) copyFileTerminateCommandFunc() commandFuncType {
	return func(
		subject,
		replySubject string,
		incomingMessage *CommandMessageType,
	) (err error) {
		logger := node.logger

		workerId := incomingMessage.ParamWorkerId(workerIdParam)
		if workerId.IsEmpty() {
			err = errors.New("gotten worker id is empty")
			logger.Error(err)
			return
		}

		worker := node.FindWorker(workerId)
		if worker == nil {
			err = errors.Errorf("could not find worker by id %v ", workerId)
			logger.Error(err)
			return
		}

		worker.Terminate()

		node.RemoveWorker(worker)

		return
	}

}

func (worker *copyFileWriter) Terminate() {
	logger := worker.logger()
	if worker.taskCancelFunc != nil {
		worker.taskCancelFunc()
	}
	logger.Warnf("Slave '%v': Cancel current task called", worker.node.NodeId())
	worker.CloseFile()
	worker.RemoveTempFile()
	worker.RemoveTargetFile()
	worker.Unsubscribe()
}


func (worker *copyFileWriter) TaskDone() {
	worker.node.RemoveWorker(worker)

}
func (worker *copyFileWorkerType) enc() *nats.EncodedConn {
	return worker.node.encodedConn
}
func (worker *copyFileWorkerType) logger() *logrus.Logger {
	return worker.node.logger
}

func (worker *copyFileWriter) subscriptionFunc() commandFuncType {
	return func(subject, replySubject string, incomingMessage *CommandMessageType) (err error) {
		worker.logger().Infof("Slave %v got command message: %v", worker.node.NodeId(), incomingMessage.Command)
		switch incomingMessage.Command {
		case copyFileData:
			if err = func() (err error) {
				if worker.bytesWritten == 0 {
					worker.counterpartSubject = incomingMessage.ParamSubject(workerSubjectParam)
					if worker.counterpartSubject.IsEmpty() {
						err = errors.New("Error subject has not been gotten")
					}
					worker.distributedTaskId = incomingMessage.ParamInt64(distributedTaskIdParam, -1)
				}
				untyped, ok := incomingMessage.Params[copyFileDataParam]
				if !ok {
					err = errors.Errorf("parameter % is expected ", copyFileDataParam)
					return
				}
				var data []byte
				if data, ok = untyped.([]byte); !ok {
					err = errors.Errorf("parameter % is expected as []byte", copyFileDataParam)
					return
				}
				if len(data) > 0 {
					var written int
					written, err = worker.file.Write(data)
					if err != nil {
						err = errors.Wrapf(err, "could not write output file %v", worker.pathToFile)
						return
					}
					worker.bytesWritten += written
				}
				eof := incomingMessage.ParamBool(copyFileEOFParam, false)
				if eof {
					worker.CloseFile()
					worker.Unsubscribe()
					err = os.Rename(worker.pathToTempFile, worker.pathToFile)
					if err != nil {
						err = errors.Wrapf(err, "could not rename received file to %v", worker.pathToFile)
					}
					worker.node.RemoveWorker(worker)
					return
				}
				return
			}(); err != nil {
				worker.logger().Error(err)
				err2 := worker.node.PublishCommandResponse(
					worker.counterpartSubject.String(),
					copyFileData,
					&CommandMessageParamEntryType{
						errorParam, err,
					},
				)
				if err2 != nil {
					worker.logger().Error(err2)
				}
				worker.CloseFile()
				worker.RemoveTempFile()
				worker.Unsubscribe()
				worker.TaskDone()
				return
			}

		case copyFileError:
			worker.TaskCanceled()
			return
		default:
			err = errors.Errorf("unexpected command ", incomingMessage.Command)
			worker.logger().Error(err)
		}
		return
	}
}

func (worker *copyFileReader) CancelCurrentJob() {
	worker.logger().Warnf("Slave '%v': Cancel current job called")
	worker.CloseFile()
	worker.Unsubscribe()
}

func (worker *copyFileWorkerType) CloseFile() (err error) {
	if worker.file != nil {
		err = worker.file.Close()
		if err != nil && err != os.ErrClosed {
			err = errors.Wrapf(err, "could not close file %v", worker.pathToFile)
			worker.logger().Error(err)
		}
		worker.file = nil
	}
	return
}

func (worker *copyFileWriter) RemoveTargetFile() (err error) {
	if len(worker.pathToFile) != 0 {
		err = os.Remove(worker.pathToFile)
		if err != nil && !os.IsNotExist(err) {
			err = errors.Wrapf(err, "could not remove target file %v", worker.pathToFile)
		} else {
			worker.pathToFile = ""
		}
	}
	return
}

func (worker *copyFileWriter) RemoveTempFile() (err error) {
	if len(worker.pathToTempFile) != 0 {
		err = os.Remove(worker.pathToFile)
		if err != nil && !os.IsNotExist(err) {
			err = errors.Wrapf(err, "could not remove temp file %v", worker.pathToTempFile)
		} else {
			worker.pathToFile = ""
		}
	}
	return
}

func (worker copyFileWorkerType) JSONMap() map[string]interface{} {
	result := worker.basicWorkerType.JSONMap()
	result["worker"] = reflect.TypeOf(worker).String()
	result["pathToFile"] = worker.pathToFile
	return result
}

func (worker copyFileWriter) JSONMap() map[string]interface{} {
	result := worker.copyFileWorkerType.JSONMap()
	result["worker"] = reflect.TypeOf(worker).String()
	result["pathToTempFile"] = worker.pathToTempFile
	result["bytesWritten"] = worker.bytesWritten
	return result
}

func (worker *copyFileReader) TaskDone() {
	worker.node.RemoveWorker(worker)
}
