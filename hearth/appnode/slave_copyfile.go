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
const fileNotExistsParam CommandMessageParamType = "fileNotExistsParam"

const fileStats CommandType = "FILE.STATS"

const copyFileDataSubscribe CommandType = "COPYFILE.SUBS"
const copyFileData CommandType = "COPYFILE.DATA"
const copyFileError CommandType = "COPYFILE.ERROR"
const copyFileLaunch CommandType = "COPYFILE.LAUNCH"

const (
	copyFileDataParam CommandMessageParamType = "data"
	copyFileEOFParam  CommandMessageParamType = "EOF"
)

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

func (node *slaveApplicationNodeType) copyFileDataSubscriptionProcessorFunc() commandProcessorFuncType {
	return func(subject, replySubject string, incomingMessage *CommandMessageType) (err error) {
		replyParams := NewCommandMessageParams(3)

		reply := func() (err error) {
			err = node.PublishCommandResponse(
				replySubject,
				copyFileDataSubscribe,
				replyParams...,
			)
			if err != nil {
				err = errors.Wrapf(err, "could not publish subscription %v response: ", copyFileDataSubscribe)
				return
			}
			return
		}

		dataSubject := newPrefixSubject("CopyFileData")

		worker := &copyFileWriter{
			copyFileWorkerType: copyFileWorkerType{
				basicWorkerType: basicWorkerType{
					id:   newWorkerId(),
					node: node,
				},
			},
		}

		logger := worker.logger()

		worker.pathToFile = incomingMessage.ParamString(filePathParam, "")

		if worker.pathToFile == "" {
			err = errors.New("path to file is empty")
			logger.Error(err)
			return
		}

		dir, _ := path.Split(worker.pathToFile)
		var tempFile *os.File
		tempFile, err = ioutil.TempFile(dir, "tmp_")
		if err != nil {
			err = errors.Wrapf(err, "could not create temp file at %v:", dir)
			logger.Error(err)
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}
		worker.pathToTempFile = tempFile.Name()
		worker.file = tempFile

		worker.jobContext, worker.jobCancelFunc = context.WithCancel(context.Background())
		worker.subscription, err = node.Subscribe(
			dataSubject,
			worker.subscriptionFunc(),
		)
		if err != nil {
			replyParams = replyParams.Append(errorParam, err)
			reply()
			return
		}

		replyParams = replyParams.Append(workerSubjectParam, dataSubject)
		err = reply()
		if err != nil {
			worker.Unsubscribe()
		} else {
			node.AppendWorker(worker)
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

		worker := &copyFileReader{
			copyFileWorkerType: copyFileWorkerType{
				basicWorkerType: basicWorkerType{
					id:   newWorkerId(),
					node: node,
				},
			},
		}
		worker.jobContext, worker.jobCancelFunc = context.WithCancel(context.Background())

		logger := worker.logger()

		worker.pathToFile = incomingMessage.ParamString(filePathParam, "")

		if worker.pathToFile == "" {
			err = errors.New("path to destination file is empty")
			logger.Error(err)
			return
		}

		worker.counterpartSubject = incomingMessage.ParamSubject(workerSubjectParam)
		if worker.counterpartSubject.IsEmpty() {
			err = errors.New("counterpart data subject is empty")
			logger.Error(err)
			return
		}

		fileStats, err := os.Stat(worker.pathToFile)
		if err != nil {
			err = errors.Wrapf(err, "could not get source file information: %v", worker.pathToFile)
			node.logger.Error(err)
			return
		}
		if fileStats.Size() == 0 {
			err = errors.Errorf("Source file is empty: %v", worker.pathToFile)
			node.logger.Error(err)
			return
		}

		worker.file, err = os.Open(worker.pathToFile)
		if err != nil {
			node.logger.Error(err)
			return
		}

		//node.logger.Info(worker.counterpartSubject)

		go func() {
			if err = func() (err error) {
				select {
				case _ = <-worker.jobContext.Done():
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
							node.RemoveWorker(worker.Id())
							return
						}
					}
				}
			}(); err != nil {
				worker.CloseFile()
				err = node.PublishCommand(worker.counterpartSubject,
					copyFileError,
				)
				if err != nil {
					err = errors.Wrapf(err, "could not send copyfile failure command")
					worker.logger().Error(err)
				}
				node.RemoveWorker(worker.Id())
				return
			}
		}()

		return
	}
}

func (worker *copyFileWriter) CancelCurrentJob() {
	if worker.jobCancelFunc != nil {
		worker.jobCancelFunc()
	}
	worker.logger().Warnf("Slave '%v': Cancel current job called")
	worker.CloseFile()
	worker.RemoveTempFile()
	worker.RemoveFile()
	worker.Unsubscribe()
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
				return
			}

		case copyFileError:
			worker.CancelCurrentJob()
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

func (worker *copyFileWriter) RemoveFile() (err error) {
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

