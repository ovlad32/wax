package appnode

import (
	"fmt"
	"github.com/pkg/errors"
)

func (node *ApplicationNodeType) StartMasterNode() (err error) {
	//err = node.initAppNodeService()

	if err != nil {
		return err
	}


	node.initRestApiRouting()

	/*
		osSignal := make(chan os.Signal,1)
		signal.Notify(osSignal,os.Interrupt)
		func() {
			for {
				select {
					_ = <-osSignal

				}
			}
		}
	*/
	return
}



func (node *ApplicationNodeType) MakeMasterCommandSubscription() (err error) {

	if !node.config.Master {
		panic(fmt.Sprintf("AppNode is NOT Master"))
	}

	node.commandSubscription, err = node.enc.Subscribe(
		parishSubjectName,
		node.MasterCommandSubscriptionFunc(),
	)

	err = node.enc.Flush()
	if err != nil {
		err = errors.Wrapf(err, "Error while subscription being flushed")
		node.config.Logger.Error(err)
	}

	if err = node.enc.LastError(); err != nil {
		err = errors.Wrap(err, "error given via NATS while making Master command subscription")
		node.config.Logger.Error(err)
	}
	node.config.Logger.Info(
		"Master command subscription has been created",
	)
	return
}

func (node *ApplicationNodeType) MasterCommandSubscriptionFunc() func(string, string, *ParishRequestType) {
	return func(subj, reply string, msg *ParishRequestType) {
		resp := ParishResponseType{}
		node.slavesMux.Lock()
		if node.slaves == nil {
			node.slaves = make(map[NodeNameType]*SlaveNodeInfoType)
		}
		if prevInfo, found := node.slaves[msg.SlaveNodeName]; !found {
			_ = prevInfo
			resp.ReConnect = true
			node.config.Logger.Infof(
				"Slave %v had been registered before. Registered again",
				msg.SlaveNodeName,
				msg.CommandSubject,
			)
		} else {
			node.slaves[msg.SlaveNodeName] = &SlaveNodeInfoType{
				CommandSubject: msg.CommandSubject,
			}
			node.config.Logger.Infof(
				"Slave %v has been registered with command subject %v",
				msg.SlaveNodeName,
				msg.CommandSubject,
			)
		}
		node.slavesMux.Unlock()
	}
}

