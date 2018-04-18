package appnode

import (
	"github.com/ovlad32/wax/hearth/appnode/message"
	"fmt"
	"github.com/pkg/errors"
	"github.com/nats-io/go-nats"
	"github.com/ovlad32/wax/hearth/appnode/natsu"
	"github.com/nats-io/nuid"
	"context"
	"os"
	"bufio"
	"io"
	"github.com/ovlad32/wax/glog"
)

const (
	fileCopy            message.Command = "FILE.COPY"
)


type MxFileCopy struct {
	SrcPathToFile string
	SrcNodeId     string
	DstPathToFile string
	DstNodeId     string
	DataSubject   string
}

type MxFileCopyAgent struct {
	Stage         string
	params *MxFileCopy
	maxPayloadSize uint
	file *os.File
	interruptingContext context.Context
	eofContext context.Context
	comm *natsu.Communication
}


func (agent *MxFileCopyAgent) readFile () (err error) {
	agent.file, err  = os.Open(agent.params.SrcPathToFile)
	if err != nil {
		err = errors.Wrapf(err, "could not open source file: %v", agent.params.SrcPathToFile)
		return
	}


	reader := bufio.NewReader(agent.file)
	buffer := make([]byte, agent.maxPayloadSize)

	for {
		select {
		case _ = <-agent.interruptingContext.Done():
			if agent.file != nil {
				err = agent.file.Close()
			}
			return
		default:
			_, err = reader.Read(buffer)
			if err != nil {
				if err != io.EOF {
					err = errors.Wrapf(err, "could not read source file %v", agent.params.SrcPathToFile)
					agent.file.Close()
					return
				} else {
					if err = agent.comm.Flush(); err != nil {
						err = errors.Wrapf(err, "could not flush published file raw data")
						return
					}
				}
				return nil
			}
			if err = agent.comm.Enc().Publish(agent.params.DataSubject, buffer); err != nil {
				err = errors.Wrapf(err, "could not publish file raw data")
				return
			}
		}
	}
}

func (instance *MxFileCopy) SetCommunication(comm *natsu.Communication) {
	instance.comm = comm
}


func (instance *MxFileCopy) init()  (err error){
	if instance.SrcPathToFile == "" {
		err = errors.New("Path to source file is empty")
		return
	}

	if instance.DstPathToFile == "" {
		err = errors.New("Path to destination file is empty")
		return
	}

	if instance.SrcNodeId == "" {
		err = errors.New("Source Node Id is empty")
		return
	}
	if instance.DstNodeId == "" {
		err = errors.New("Destination Node Id is empty")
		return
	}
	return
}


func (instance *MxFileCopy) openRawDataSubscription()  (err error) {
	dataReceiver := func(msg *nats.Msg) {
		for {
			select {
				case _ = <-instance.dataTransfer.Done():
					instance.Close()
					return
			default:
				instance.Write(msg.([]byte))
			}
		}


		return
	}

	instance.DataSubject = fmt.Sprintf("%v/%v",instance.DstNodeId,nuid.Next())

	instance.comm.SubscribeBareFunc(instance.DataSubject,dataReceiver)

	return
}




