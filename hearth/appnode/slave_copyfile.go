package appnode

import (
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"context"
	"io/ioutil"
	"path"
	"bufio"
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
	return func(replySubject string, incomingMessage *CommandMessageType) (err error) {
		replyParams := NewCommandMessageParams(3)
		reply := func () (err error){
			err = node.PublishCommandResponse(
				replySubject,
				fileStats,
				replyParams...
			)
			if err!=nil {
				err = errors.Wrapf(err, "could not publish %v response", fileStats)
				return
			}
			return
		}

		filePath := incomingMessage.ParamString(filePathParam, "")
		stats, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				replyParams.Append(fileNotExistsParam, true)
			} else {
				err  = errors.Wrapf(err,"could not get file statistics for %v",filePath)
				node.logger.Error(err)
				replyParams.Append(errorParam, err)
			}
		} else {
			replyParams.Append(fileSizeParam,stats.Size())
		}
		err = reply()
		return
	}
}

func (node *slaveApplicationNodeType) copyFileDataSubscriptionProcessorFunc() commandProcessorFuncType {
	return func(replySubject string, incomingMessage *CommandMessageType) (err error) {
		replyParams := NewCommandMessageParams(3)

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
		}


		dataSubject := newPrefixSubject("COPYFILEDATA")

		worker := &copyFileWriter{
			copyFileWorkerType:copyFileWorkerType{
				basicWorkerType:basicWorkerType{
					id:newWorkerId(),
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
		worker.file, err = ioutil.TempFile(dir,"tmp_");

		if err != nil {
			err = errors.Wrapf(err,"could not create temp file at %v:",dir)
			logger.Error(err)
			replyParams.Append(errorParam,err)
			reply()
			return
		}

		worker.jobContext,worker.jobCancelFunc = context.WithCancel(context.Background())
		worker.subscription, err = node.Subscribe(
			dataSubject,
			worker.subscriptionFunc(),
		)
		if err != nil {
			replyParams.Append(errorParam,err)
			reply()
			return
		}

		replyParams.Append(workerSubjectParam,dataSubject)
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

	return func(replySubject string, incomingMessage *CommandMessageType) (err error) {

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
			copyFileWorkerType:copyFileWorkerType{
				basicWorkerType:basicWorkerType{
					node: node,
				},
			},
		}

		logger := worker.logger()

		worker.pathToFile = incomingMessage.ParamString(filePathParam, "")

		if worker.pathToFile == "" {
			err = errors.New("path to destination file is empty")
			logger.Error(err)
			return
		}

		worker.subject = incomingMessage.ParamSubject(workerSubjectParam)
		if worker.subject.IsEmpty() {
			err = errors.New("counterpart data subject is empty")
			logger.Error(err)
			return
		}


		fileStats,err := os.Stat(worker.pathToFile)
		if err != nil {
			err = errors.Wrapf(err,"could not get source file information: %v",worker.pathToFile)
			node.logger.Error(err)
			return
		}
		if fileStats.Size() == 0 {
			err = errors.Errorf("Source file is empty: %v",worker.pathToFile)
			node.logger.Error(err)
			return
		}


		worker.file,err  = os.Open(worker.pathToFile)
		if err != nil {
			node.logger.Error(err)
			return
		}



		node.logger.Info(worker.subject)

		go func () {

			buf := bufio.NewReader(worker.file)

			var readBuffer []byte
			var dataSizeAdjusted = false
			var maxPayloadSize = node.encodedConn.Conn.MaxPayload()

			if dataSizeAdjusted = fileStats.Size() < maxPayloadSize/2; dataSizeAdjusted {
				node.logger.Warnf("readBuffer allocated to the size of the sourceFile %v",fileStats.Size())
				readBuffer = make([]byte, fileStats.Size())
			} else {
				var adjustment int64
				adjustment, dataSizeAdjusted = node.payloadSizeAdjustments[copyFileData]
				readBuffer = make([]byte, maxPayloadSize - adjustment)
			}

			for {
				var eof bool
				readBytes, err := buf.Read(readBuffer);
				if err != nil {
					if err != io.EOF {
						//todo:
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
					},
				}
				if !dataSizeAdjusted {
					var adjustment int64
					adjustment,err  = node.registerMaxPayloadSize(message)
					if err != nil {
						//todo:
					}
					newSize := maxPayloadSize + adjustment;
					cutMessage := &CommandMessageType{
						Command: copyFileData,
						Params: CommandMessageParamMap{
							copyFileDataParam: replica[0:newSize],
							copyFileEOFParam:  eof,
						},
					}
					if err = node.encodedConn.Publish(worker.subject.String(), cutMessage); err != nil {
						err = errors.Wrapf(err, "could not publish copyfile cutMessage")
						node.logger.Error(err)

					}
					replica = replica[newSize:]
					readBuffer = readBuffer[0:newSize]
					node.logger.Warnf("readBuffer has been cut to %v bytes",newSize)

					dataSizeAdjusted = true
					message = &CommandMessageType{
						Command: copyFileData,
						Params: CommandMessageParamMap{
							copyFileDataParam: replica,
							copyFileEOFParam:  eof,
						},
					}
				}

				if err = node.encodedConn.Publish(worker.subject.String(), message); err != nil {
					err = errors.Wrapf(err, "could not publish copyfile message")
					node.logger.Error(err)

				}

				if eof {
					if err = node.encodedConn.Flush(); err != nil {
						err = errors.Wrapf(err,"could not flush copyfile message")
						node.logger.Error(err)
						return
					}

					if err = node.encodedConn.LastError(); err != nil {
						err = errors.Wrapf(err,"error while wiring copyfile message")
						node.logger.Error(err)
						return
					}
					worker.file.Close()
					return
				}
			}
		} ()

	return
	}
}

func (worker *copyFileWorkerType) enc() *nats.EncodedConn {
	return worker.node.encodedConn
}
func (worker *copyFileWorkerType) logger() *logrus.Logger {
	return worker.node.logger
}


func (worker *copyFileWorkerType) subscriptionFunc() commandProcessorFuncType {
	return func(replySubject string, incomingMessage *CommandMessageType) (err error) {
		worker.logger().Infof("Slave %v got command message: %v", worker.node.NodeId(), incomingMessage.Command)
		switch incomingMessage.Command {
		case copyFileData:
			if worker.file == nil {
				worker.file, err = os.Create(worker.pathToFile)
				if err != nil {
					err = errors.Wrapf(err, "could not open file %v", worker.pathToFile)
					worker.logger.Error(err)
					worker.node.reportError(
						copyFileError,
						err,
						&CommandMessageParamEntryType{
							Name:workerSubjectParam,
							Value:worker.subject,
						},
						)
					worker.unsubscribe()
					return
				}
			}
			untyped, ok := msg.Params[copyFileDataParam]
			if !ok {
				err = errors.Errorf("parameter % is expected ", copyFileDataParam)
				worker.logger.Error(err)
				worker.reportError(copyFileError, err)
				worker.closeFile()
				worker.removeFile()
				worker.unsubscribe()
				return
			}
			if data, ok := untyped.([]byte); !ok {

			} else if len(data) > 0 {
				_, err = worker.file.Write(data)
				if err != nil {
					err = errors.Wrapf(err, "could not write output file %v", worker.pathToFile)
					worker.logger.Error(err)
					worker.reportError(copyFileError, err)
					worker.closeFile()
					worker.removeFile()
					worker.unsubscribe()
					return
				}
			}
			eof := msg.ParamBool(copyFileEOFParam, false)
			if eof {
				worker.file.Close()
				worker.unsubscribe()
			}
		case copyFileError:
			worker.closeFile()
			if err := worker.removeFile(); err != nil {
				worker.reportError(copyFileError, err)
			}
			worker.unsubscribe()
			return
		default:
			err := errors.Errorf("unexpected command ", msg.Command)
			worker.logger.Error(err)
		}
	}
}
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



func (worker *copyFileWriter) removeFile() (err error) {
	if len(worker.pathToFile) != 0 {
		err = os.Remove(worker.pathToFile)
		if err != nil && !os.IsNotExist(err) {
			err = errors.Wrapf(err, "could not remove target file %v", worker.pathToFile)
		}
		worker.pathToFile = ""
	}
	return
}
