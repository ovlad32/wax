package appnode

import (
	"github.com/sirupsen/logrus"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/nats-io/nuid"
	"os"
	"io"
)

type copyFileWorker struct {
	basicWorker
	file io.WriteCloser
	pathToFile string
	logger *logrus.Logger
}

const outputFilePathParm CommandMessageParamType = "outputFilePath";
const copyFileOpen CommandType = "COPYFILE.OPEN"
const copyFileOpened CommandType = "COPYFILE.OPENED"
const copyFileData CommandType = "COPYFILE.DATA"
const copyFileError CommandType = "COPYFILE.ERROR"

func newCopyFileWorker(
	enc *nats.EncodedConn,
	replySubject string,
	message *CommandMessageType,
	logger *logrus.Logger,
) (result *copyFileWorker, err error){


	worker := &copyFileWorker{
		basicWorker:basicWorker {
			encodedConn:enc,
		},
		logger:logger,
	}


	worker.pathToFile =  message.ParamString(outputFilePathParm,"")

	if worker.pathToFile == "" {
		err = errors.Errorf("output path is empty")
		return
	}

	subjectName := fmt.Sprintf("COPY/%v",nuid.Next())
	worker.subscriptions = make([]*nats.Subscription,1)

	worker.subscriptions[0], err = enc.Subscribe(
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
	} != nil {
		worker.subscriptions[0].Unsubscribe()
		worker.subscriptions = nil
	}
	worker.id = subjectName
	result = worker
	return
}
const (
	copyFileDataParam CommandMessageParamType = "data"
	copyFileEOFParam CommandMessageParamType = "EOF"
)

func (worker *copyFileWorker) subscriptionFunc() func(msg CommandMessageType) {
	return func (msg CommandMessageType) {
		if msg.Command == copyFileData {
			var err  error
			if worker.file == nil {
				worker.file, err = os.Create(worker.pathToFile)
					if err != nil {
						err = errors.Wrapf(err,"could not open file %v",worker.pathToFile)
						worker.logger.Error(err)
						worker.reportError(copyFileError,err)
						return
					}
			}
			 untyped,ok  :=  msg.Params[copyFileDataParam]
			 if !ok {
			 	 err = errors.Errorf("parameter % is expected ",copyFileDataParam)
				 worker.logger.Error(err)
				 worker.reportError(copyFileError,err)
				 return
			 }
			 if data,ok  := untyped.([]byte); !ok {

			 } else if len(data)> 0 {
			 	_, err := worker.file.Write(data)
			 	if err != nil {
					err = errors.Wrapf(err,"could not write output file %v",worker.pathToFile)
					worker.logger.Error(err)
					worker.reportError(copyFileError,err)
					return
				 }
			 }
			 eof := msg.ParamBool(copyFileEOFParam,false)
			 if eof {
			 	worker.file.Close()
			 }
		} else {
			err := errors.Errorf("unexpected command ",msg.Command)
			worker.logger.Error(err)
			worker.reportError(copyFileError,err)
			return
		}
	}
}


