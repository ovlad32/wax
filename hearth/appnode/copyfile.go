package appnode

import (
	"bufio"
	"context"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"github.com/nats-io/gnatsd/logger"
)

type copyFileWorkerType struct {
	basicWorkerType
	file       io.ReadWriteCloser
	pathToFile string
}
type copyFileReader struct {
	copyFileWorkerType
}
type copyFileWriter struct {
	bytesWritten   int
	pathToTempFile string
	copyFileWorkerType
}

const filePathParam CommandMessageParamType = "filePathParam"
const fileSizeParam CommandMessageParamType = "fileSizeParam"
const fileReadingParam CommandMessageParamType = "fileReadingParam"
const fileNotExistsParam CommandMessageParamType = "fileNotExistsParam"

const fileStats CommandType = "FILE.STATS"

const copyFileOpen CommandType = "CMD.COPYFILE.OPEN"
const copyFileData CommandType = "WRK.COPYFILE.DATA"
const copyFileError CommandType = "CMD.COPYFILE.ERROR"
const copyFileLaunch CommandType = "CMD.COPYFILE.LAUNCH"

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

	sourceNodeId := NodeIdType(srcNodeId)
	if sourceNodeId.IsEmpty() {
		//TODO:
	}

	targetNodeId := NodeIdType(tgtNodeId)
	if sourceNodeId.IsEmpty() {
		//TODO:
	}
	var sourceResp, targetResp *CommandMessageType
	var targetDataSubject SubjectType
	var sourceWorkerId,targetWorkerId WorkerIdType
	var sourceFileSize int64

	SourceRequest := func (command CommandType, params CommandMessageParamEntryArrayType) (resp *CommandMessageType, err error) {
		resp, err = node.RequestCommandBySubject(
			sourceNodeId.CommandSubject(),
			command,
			params...
		)
		if resp == nil {
			err = errors.Errorf("%v response from source node is not initialized", command)
			return
		}
		if resp.Command != command {
			err = errors.Wrapf(err, "%v: response from source node is not recognized: %v", command, resp.Command)
			return
		}
		if resp.Err != nil {
			err = errors.Errorf("%v: response from source node with error: %v", command, resp.Err)
			return
		}
		return
	}

	TargetRequest := func (command CommandType, params CommandMessageParamEntryArrayType) (resp *CommandMessageType, err error) {
		targetResp, err = node.RequestCommandBySubject(
			targetNodeId.CommandSubject(),
			command,
			params...
		)

		if targetResp == nil {
			err = errors.Errorf("%v: response from target node with error: %v", command,targetResp.Err)
			return
		}
		if targetResp.Command != command {
			err = errors.Wrapf(err, "%v: response from target node is not recognized: %v", command, targetResp.Command)
			return
		}

		if targetResp.Err != nil {
			err = errors.Errorf("%v: response from target node with error: %v", command, targetResp.Err)
			return
		}
		return
	}


	if sourceResp, err = SourceRequest(
		fileStats,
		NewCommandMessageParams(1).
			Append(filePathParam, srcFile),
	); err != nil {
		//TODO:
		node.logger.Error(err)
		return
	} else if err = func ()(err error) {
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
	} else if targetResp, err = TargetRequest(
		copyFileOpen,
		NewCommandMessageParams(3).
			Append(fileReadingParam,false).
			Append(filePathParam,tgtFile).
			Append(fileSizeParam,sourceFileSize),
		); err != nil {
		//TODO:
		node.logger.Error(err)
		return
	} else if err = func() (err error) {
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
	} else if  sourceResp, err = SourceRequest(
		copyFileOpen,
		NewCommandMessageParams(4).
			Append(fileReadingParam, true).
			Append(workerIdParam, targetWorkerId).
			Append(workerSubjectParam, targetDataSubject).
			Append(filePathParam, srcFile),
	); err != nil {
		//TODO KILL target worker
		//TODO:
		node.logger.Error(err)
		return
	} else if err = func() (err error) {
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
	} else {
		
	}


	return



	err = node.PublishCommand(
		sourceNodeId.CommandSubject(),
		copyFileLaunch,
		NewCommandMessageParams(1).
			Append(filePathParam,srcFile).
			Append(workerSubjectParam,dataSubject)...
	)

	return dataSubject.String(),nil
}


func (node slaveApplicationNodeType) fileStatsProcessorFunc() commandProcessorFuncType {
	return func(subject, replySubject string, incomingMessage *CommandMessageType) (err error) {
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

func (node *slaveApplicationNodeType) copyFileOpenProcessorFunc() commandProcessorFuncType {

	return func(subject, replySubject string, incomingMessage *CommandMessageType) (err error) {
		replyParams := NewCommandMessageParams(3)

		reply := func() (err error) {
			err = node.PublishCommandResponse(
				replySubject,
				copyFileOpen,
				replyParams...,
			)
			if err != nil {
				err = errors.Wrapf(err, "could not publish subscription %v response: ", copyFileOpen)
				return
			}
			return
		}
		createWriter := func () (err error) {
			logger := node.logger;

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

			if writer.pathToFile == "" {
				err = errors.New("path to target file is empty")
				logger.Error(err)
				replyParams = replyParams.Append(errorParam, err)
				reply()
				return
			}

			dir, _ := path.Split(writer.pathToFile)
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

		createReader := func() (err error) {
			logger := node.logger;
			reader := &copyFileReader{
				copyFileWorkerType: copyFileWorkerType{
					basicWorkerType: basicWorkerType{
						id:   newWorkerId(),
						node: node,
					},
				},
			}
			reader.taskCancelContext, reader.taskCancelFunc= context.WithCancel(context.Background())


			reader.pathToFile = incomingMessage.ParamString(filePathParam, "")

			if reader.pathToFile == "" {
				err = errors.New("path to source file is empty")
				logger.Error(err)
				return
			}

			reader.counterpartSubject = incomingMessage.ParamSubject(workerSubjectParam)
			if reader.counterpartSubject.IsEmpty() {
				err = errors.New("write-worker data subject is empty")
				logger.Error(err)
				return
			}

			fileStats, err := os.Stat(reader.pathToFile)
			if err != nil {
				err = errors.Wrapf(err, "could not get source file information: %v", reader.pathToFile)
				node.logger.Error(err)
				return
			}
			if fileStats.Size() == 0 {
				err = errors.Errorf("Source file is empty: %v", reader.pathToFile)
				node.logger.Error(err)
				return
			}
			return
		}



		reading := incomingMessage.ParamBool(fileReadingParam,true)
		if !reading {
			err = createReader()
		} else {
			err = createWriter()
		}

		return err
	}

}

func (node *slaveApplicationNodeType) copyFileLaunchProcessorFunc() commandProcessorFuncType {
	return func(subject, replySubject string, incomingMessage *CommandMessageType) (err error) {

		/*replyParams := NewCommandMessageParams(3)

		reply := func () (err error){
			err = node.PublishCommandResponse(
				replySubject,
				copyFileDataSubscribe,
				replyParams...
			)
			if err!=nil {
				err = errors.Wrapf(err, "could not publish subscription %v response: ", copyFileDataSubscribe)
				return
			}
			return
		}*/



		worker.file, err = os.Open(worker.pathToFile)
		if err != nil {
			node.logger.Error(err)
			return
		}

		//node.logger.Info(worker.counterpartSubject)

		go func() {
			if err = func() (err error) {
				select {
				case _ = <-worker.taskCancelContext.Done():
					worker.CloseFile()
					worker.Unsubscribe()
					return
				default:
					buf := bufio.NewReader(worker.file)
					var readBuffer []byte
					var dataSizeAdjusted = false
					var maxPayloadSize = node.encodedConn.Conn.MaxPayload()

					if dataSizeAdjusted = fileStats.Size() < maxPayloadSize/2; dataSizeAdjusted {
						node.logger.Warnf("readBuffer allocated to the size of the sourceFile %v", fileStats.Size())
						readBuffer = make([]byte, fileStats.Size())
					} else {
						var adjustment int64
						adjustment, dataSizeAdjusted = node.payloadSizeAdjustments[copyFileData]
						readBuffer = make([]byte, maxPayloadSize-adjustment)
					}

					for {
						var eof bool
						var readBytes int
						readBytes, err = buf.Read(readBuffer)
						if err != nil {
							if err != io.EOF {
								err = errors.Wrapf(err, "could not read source file %v", worker.pathToFile)
								node.logger.Error(err)
								return
							} else {
								eof = true
							}
						}

						replica := make([]byte, readBytes)
						copy(replica, readBuffer[:readBytes])

						message := &CommandMessageType{
							Command: copyFileData,
							Params: CommandMessageParamMap{
								copyFileDataParam: replica,
								copyFileEOFParam:  eof,
								workerIdParam: worker.id,
							},
						}
						if !dataSizeAdjusted {
							var adjustment int64
							adjustment, err = node.registerMaxPayloadSize(message)
							if err != nil {
								return
							}
							newSize := maxPayloadSize + adjustment
							node.logger.Infof("message adjustment %v",adjustment)
							cutMessage := &CommandMessageType{
								Command: copyFileData,
								Params: CommandMessageParamMap{
									copyFileDataParam:  replica[0:newSize],
									copyFileEOFParam:   eof,
									workerIdParam: worker.id,
								},
							}
							if err = node.encodedConn.Publish(worker.counterpartSubject.String(), cutMessage); err != nil {
								err = errors.Wrapf(err, "could not publish cut copyfile message")
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
									copyFileDataParam:  replica,
									copyFileEOFParam:   eof,
									workerIdParam: worker.id,
								},
							}
						}

						if err = node.encodedConn.Publish(worker.counterpartSubject.String(), message); err != nil {
							err = errors.Wrapf(err, "could not publish copyfile message")
							node.logger.Error(err)
							return
						}

						if eof {
							if err = node.encodedConn.Flush(); err != nil {
								err = errors.Wrapf(err, "could not flush copyfile message")
								node.logger.Error(err)
								return
							}

							if err = node.encodedConn.LastError(); err != nil {
								err = errors.Wrapf(err, "error while wiring copyfile message")
								node.logger.Error(err)
								return
							}
							worker.CloseFile()
							worker.Unsubscribe()
							node.RemoveWorker(worker)
							return
						}
					}
				}
			}(); err != nil {
				worker.CloseFile()
				err = node.PublishCommand(
					worker.counterpartSubject,
					copyFileError,
				)
				if err != nil {
					err = errors.Wrapf(err, "could not send copyfile failure command")
					worker.logger().Error(err)
				}
				node.RemoveWorker(worker)
				return
			}
		}()

		return
	}
}

func (worker *copyFileWriter) TaskCanceled() {
	if worker.taskCancelFunc != nil {
		worker.taskCancelFunc()
	}
	worker.logger().Warnf("Slave '%v': Cancel current task called",worker.node.NodeId())
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

func (worker *copyFileWriter) subscriptionFunc() commandProcessorFuncType {
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


func(worker copyFileWorkerType ) JSONMap() map[string]interface{} {
	result:= worker.basicWorkerType.JSONMap()
	result["worker"] = reflect.TypeOf(worker).String()
	result["pathToFile"] =  worker.pathToFile
	return result
}



func(worker copyFileWriter ) JSONMap() map[string]interface{} {
	result:= worker.copyFileWorkerType.JSONMap()
	result["worker"] = reflect.TypeOf(worker).String()
	result["pathToTempFile"] =  worker.pathToTempFile
	result["bytesWritten"] = worker.bytesWritten
	return result
}

func (worker *copyFileReader) TaskDone() {
	worker.node.RemoveWorker(worker)
}
