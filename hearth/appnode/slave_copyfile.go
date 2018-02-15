package appnode

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"context"
	"io/ioutil"
	"path"
)

type copyFileWorkerType struct {
	basicWorkerType
	file       io.ReadWriteCloser
	pathToFile string
	counterPartCommandSubject SubjectType
}
type copyFileReader struct {
	copyFileWorkerType
}
type copyFileWriter struct {
	copyFileWorkerType
}

const errorParam CommandMessageParamType = "error"
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
		replyParams := make(CommandMessageParamEntryArrayType,0,3)
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
				replyParams.append(fileNotExistsParam, true)
			} else {
				err  = errors.Wrapf(err,"could not get file statistics for %v",filePath)
				node.logger.Error(err)
				replyParams.append(errorParam, err)
			}
		} else {
			replyParams.append(fileSizeParam,stats.Size())
		}
		err = reply()
		return
	}
}

func (node *slaveApplicationNodeType) copyFileDataSubscriptionProcessorFunc() commandProcessorFuncType {
	return func(replySubject string, incomingMessage *CommandMessageType) (err error) {
		replyParams := make(CommandMessageParamEntryArrayType,0,3)

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


		subjectString := fmt.Sprintf("COPYFILEDATA/%v", nuid.Next())
		worker := &copyFileWriter{
			copyFileWorkerType:copyFileWorkerType{
				basicWorkerType:basicWorkerType{
					node: node,
				},
			},
		}

		logger := worker.logger()

		err = worker.readMessageParameters(incomingMessage)
		if err != nil {
			return
		}

		dir, _ := path.Split(worker.pathToFile)
		worker.file, err = ioutil.TempFile(dir,"tmp_");

		if err != nil {
			err = errors.Wrapf(err,"could not create temp file at %v:",dir)
			logger.Error(err)
			replyParams.append(errorParam,err)
			reply()
			return
		}




		worker.jobContext,worker.jobCancelFunc = context.WithCancel(context.Background())
		worker.subject = SubjectType(subjectString)
		worker.subscription, err = node.Subscribe(
			worker.subject,
			worker.subscriptionFunc(),
		)
		if err != nil {
			replyParams.append(errorParam,err)
			reply()
			return
		}


		replyParams.append(workerSubjectParam,worker.subject)
		if err = reply(); err!= nil {
			worker.UnsubscribeCommandSubject()
		} else {
				node.AppendWorker(worker)
		}
		return err
	}

}

func (worker *copyFileWorkerType) enc() *nats.EncodedConn {
	return worker.node.encodedConn
}
func (worker *copyFileWorkerType) logger() *logrus.Logger {
	return worker.node.logger
}

func (worker *copyFileWorkerType) readMessageParameters(message *CommandMessageType) (err error){
	logger := worker.logger();
	worker.pathToFile = message.ParamString(filePathParam, "")

	if worker.pathToFile == "" {
		err = errors.New("path to file is empty")
		logger.Error(err)
		return
	}
	worker.counterPartCommandSubject = message.ParamSubject(slaveCommandSubjectParam)
	if worker.counterPartCommandSubject.IsEmpty() {
		err = errors.New("counterpart command subject is empty")
		logger.Error(err)
		return
	}
	return
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
