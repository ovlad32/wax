package appnode

import (
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/nats-io/nuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

type copyFileWorker struct {
	basicWorker
	file       io.WriteCloser
	pathToFile string
	logger     *logrus.Logger
}

const outputFilePathParm CommandMessageParamType = "outputFilePath"
const copyFileOpen CommandType = "COPYFILE.OPEN"
const copyFileOpened CommandType = "COPYFILE.OPENED"
const copyFileData CommandType = "COPYFILE.DATA"
const copyFileError CommandType = "COPYFILE.ERROR"

const (
	copyFileDataParam CommandMessageParamType = "data"
	copyFileEOFParam  CommandMessageParamType = "EOF"
)



func (node slaveApplicationNodeType) copyFileOpenFunc() commandProcessorFuncType {
	return func(replySubject string, incomingMessage *CommandMessageType) error {
		worker, err := newCopyFileWorker(
			node.encodedConn,
			replySubject, incomingMessage,
			node.logger,
		)
		if err != nil {
			panic(err.Error())
		}
		node.AppendWorker(worker)
		return err
	}
}



func newCopyFileWorker(
	enc *nats.EncodedConn,
	replySubject string,
	message *CommandMessageType,
	logger *logrus.Logger,
) (result *copyFileWorker, err error) {

	worker := &copyFileWorker{
		basicWorker: basicWorker{
			encodedConn: enc,
		},
		logger: logger,
	}

	worker.pathToFile = message.ParamString(outputFilePathParm, "")

	if worker.pathToFile == "" {
		err = errors.Errorf("output path is empty")
		return
	}

	subjectName := fmt.Sprintf("COPYFILE/%v", nuid.Next())

	worker.subscription, err = enc.Subscribe(
		subjectName,
		worker.subscriptionFunc(),
	)
	if err != nil {
		logger.Error(err)
		return
	}

	if func() (err error) {
		err = enc.Flush()
		if err != nil {
			logger.Error(err)
			return
		}

		if err = enc.LastError(); err != nil {
			logger.Error(err)
			return
		}

		err = enc.Publish(replySubject,
			&CommandMessageType{
				Command: copyFileOpened,
				Params: map[CommandMessageParamType]interface{}{
					workerSubjectParam: subjectName,
				},
			})
		if err != nil {
			err = errors.Errorf("could not reply %v: %v ", message.Command, err)
			logger.Error(err)
			return
		}
		return
	}() != nil {
		worker.subscription.Unsubscribe()
		worker.subscription = nil
	}
	worker.subject = SubjectType(subjectName)
	result = worker
	return
}

func (worker *copyFileWorker) subscriptionFunc() func(msg *CommandMessageType) {
	return func(msg *CommandMessageType) {
		var err error
		worker.logger.Infof("Slave %v got command message: %v", worker.Subject(), msg.Command)
		switch msg.Command {
		case copyFileData:
			if worker.file == nil {
				worker.file, err = os.Create(worker.pathToFile)
				if err != nil {
					err = errors.Wrapf(err, "could not open file %v", worker.pathToFile)
					worker.logger.Error(err)
					worker.reportError(copyFileError, err)
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

func (worker *copyFileWorker) unsubscribe() {
	if worker.subscription != nil {
		err := worker.subscription.Unsubscribe()
		err = errors.Wrapf(err, "could not unsubscribe from subject %v ", worker.subscription.Subject)
		worker.logger.Error(err)
	}
}

func (worker *copyFileWorker) closeFile() (err error) {
	if worker.file != nil {
		err = worker.file.Close()
		if err != nil && err != os.ErrClosed {
			err = errors.Wrapf(err, "could not close target file %v", worker.pathToFile)
			worker.logger.Error(err)
		}
		worker.file = nil
	}
	return
}


func (worker *copyFileWorker) removeFile() (err error) {
	if len(worker.pathToFile) != 0 {
		err = os.Remove(worker.pathToFile)
		if err != nil && !os.IsNotExist(err) {
			err = errors.Wrapf(err, "could not remove target file %v", worker.pathToFile)
		}
		worker.pathToFile = ""
	}
	return
}
