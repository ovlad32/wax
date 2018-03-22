package appnode

import (
	"context"
	"fmt"
	"github.com/nats-io/go-nats"
	"github.com/ovlad32/wax/hearth"
	"github.com/ovlad32/wax/hearth/appnode/message"
	"github.com/ovlad32/wax/hearth/appnode/natsu"
	"github.com/ovlad32/wax/hearth/appnode/task"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/ovlad32/wax/hearth/repository"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"
)

const(
	dispatcherNodeId string = "Dispatcher"
	dispatcherCommandSubject = "dispatcher.cs"

	slaveIdParam             message.ParamKey = "slaveId"
	slaveCommandSubjectParam message.ParamKey = "slaveCommandSubject"
	slaveResubscribedParam   message.ParamKey = "slaveResubscribed"
	distributedTaskIdParam   message.ParamKey = "distTaskId"
	workerSubjectParam       message.ParamKey = "workerSubject"
	workerIdParam            message.ParamKey = "workerId"
)

var instanceOnce sync.Once



type ApplicationNodeConfigType struct {
	AstraConfig  handling.AstraConfigType
	NATSEndpoint string
	IsDispatcher bool
	NodeId       string
	RestAPIPort  int
}

type node struct {
	natsu.Communication
	config              ApplicationNodeConfigType
	commandSubscription *nats.Subscription
	commandTriggers     message.TriggerMap
	Tasks               task.Storage
}

func DispatcherCommandSubject() string {
	return dispatcherCommandSubject
}

func AgentCommandSubject(id string) string {
	return fmt.Sprintf("COMMAND/%v", id)
}

func CommandSubject(nodeId string) string {
	if nodeId == "" {
		panic("NodeId is empty!")
	}
	if nodeId == dispatcherNodeId {
		return DispatcherCommandSubject()
	} else {
		return AgentCommandSubject(nodeId)
	}
}

func LaunchNode(cfg *ApplicationNodeConfigType) {
	logger := cfg.AstraConfig.Logger

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt, os.Kill)

	var instance *node
	var httpServer *http.Server

	if cfg.IsDispatcher {
		cfg.NodeId = dispatcherNodeId
	} else if cfg.NodeId == "" {
		logger.Fatal("Provide Node Id!")
	}

	subscribeOnOsSignal := func() (err error) {
		_ = <-osSignal
		if httpServer != nil {
			if err = httpServer.Shutdown(context.Background()); err != nil {
				err = errors.Wrapf(err, "could not shutdown REST server")
				return
			}
		}
		//TODO: closeAllCommandSubscription
		//err = node.closeAllCommandSubscription()
		if err != nil {
			err = errors.Wrapf(err, "could not close command subscriptions")
			return
		}

		if cfg.IsDispatcher {
			repository.Close()
			logger.Warn("Dispatcher shut down")
		} else {
			logger.Warn("Node shut down")
		}
		os.Exit(0)
		return
	}

	configureHttpServer := func(handler http.Handler) {
		address := fmt.Sprintf(":%d", cfg.RestAPIPort)
		httpServer = &http.Server{
			Handler: handler,
			Addr:    address,
			// Good practice: enforce timeouts for servers you create!
			WriteTimeout: 15 * time.Second,
			ReadTimeout:  15 * time.Second,
		}
		logger.Infof("REST API server has started at %v....", address)

	}

	runHttpServer := func() {
		err := httpServer.ListenAndServe()
		if err != nil {
			httpServer = nil
			if err != http.ErrServerClosed {
				err = errors.Wrap(err, "REST API server broke")
				logger.Error(err)
			}
			osSignal <- os.Interrupt
		}
	}

	launch := func() (err error) {

		if cfg.IsDispatcher {
			_, err = repository.Init(hearth.AdaptRepositoryConfig(&cfg.AstraConfig))
			if err != nil {
				return
			}
		}
		instance = &node{
			config: *cfg,
			commandTriggers: make(message.TriggerMap),
		}

		if cfg.IsDispatcher {
			registerDispatcherTriggers(instance)
		} else {
			registerAgentTriggers(instance)
		}
		gobRegisterNewTypes()
		err = instance.ConnectToNATS(cfg.NATSEndpoint, cfg.NodeId)
		if err != nil {
			return
		}
		logger.Info("Connection to NATS has been established...")

		commandSubject := CommandSubject(instance.Id())
		instance.commandSubscription, err = instance.SubscribeMessageTrigger(
			commandSubject,
			instance.applyCommandTriggers(),
		)

		if err != nil {
			err = errors.Wrapf(err, "could not create command message subscription")
			return
		}
		logger.Info("Command message subscription has been created...")

		configureHttpServer(
			instance.RestApiHandlers(),
		)

		if !cfg.IsDispatcher {
			var respMessage *message.Body
			respMessage, err = instance.Request(
				DispatcherCommandSubject(),
				message.New(agentRegister).
					PutRequest(
						&AgentMessage{
							CommandSubject: commandSubject,
							NodeId:         instance.Id(),
						},
					),
			)

			if err != nil {
				err = errors.Wrapf(err,
					"could not inform Dispatcher about '%v' command subscription",
					instance.Id(),
				)
				return
			}
			if response, ok := respMessage.Response.(*AgentMessage); !ok {
				err = errors.New("could not cast gotten response")
				return
			} else {
				if response.RegisteredBefore {
					logger.Warnf(
						"Agent '%v' command subscription had been registered before...",
						instance.Id(),
					)
				}
			}
			logger.Infof("Agent '%v' has been registered", instance.Id())
		}

		go func() {
			if err := subscribeOnOsSignal(); err != nil {
				logger.Fatal(err)
			}
		}()

		go runHttpServer()

		runtime.Goexit()
		return
	}

	instanceOnce.Do(
		func() {
			if err := launch(); err != nil {
				logger.Fatal(err)
			}
		},
	)
	return
}

/*
func (node applicationNodeType) CallCommandByNodeId(
	nodeId NodeIdType,
	command CommandType,
	entries ...*CommandMessageParamEntryType,
) (response *CommandMessageType, err error) {
	return node.RequestCommandBySubject(
		SlaveCommandSubject(nodeId),
		command,
		entries...
	)
}*/

func (n *node) Logger() *logrus.Logger {
	return n.config.AstraConfig.Logger
}

func (n *node) Id() string {
	return n.config.NodeId
}
/*
func (n *node) registerCommandTriggers() (err error) {
	n.commandTriggers = make(message.TriggerMap)
	if n.config.IsDispatcher {
		dispatcherTriggerDefinition(n)

	} else {
		n.commandTriggers[agentTerminate]=nil
	}

	//node.commandProcessorsMap[parishClose] = node.parishCloseFunc()
	//node.commandFuncMap[parish.TerminateWorker] = node.parishTerminateWorkerFunc()

		//node.commandFuncMap[fileStats] = node.fileStatsCommandFunc()
		//
		//node.commandFuncMap[copyFileOpen] = node.copyFileOpenCommandFunc()
		//node.commandFuncMap[copyFileCreate] = node.copyFileCreateCommandFunc()
		//node.commandFuncMap[copyFileLaunch] = node.copyFileLaunchCommandFunc()
		//node.commandFuncMap[copyFileTerminate] = node.copyFileTerminateCommandFunc()

	//node.commandProcessorsMap[categorySplitOpen] = node.categorySplitOpenFunc()
	//node.commandProcessorsMap[categorySplitClose] = node.categorySplitCloseFunc()
	return
}
*/
func (n *node) applyCommandTriggers() message.Trigger {
	return func(subject,replySubject string, incomingMessage *message.Body) (err error) {
		n.Logger().Infof("Master node command command: %v", incomingMessage.Command)
		if processor, found := n.commandTriggers[incomingMessage.Command];found {
			err = processor(subject,replySubject,incomingMessage)
			if err != nil {
				err = errors.Wrapf(err,
					"could not trigger '%v'",
					incomingMessage.Command,
				)
				n.Logger().Error(err)
			}
		} else {
			n.Logger().Fatalf(
				"%v: cannot recognize incoming command '%v' ",
				n.config.NodeId,
				incomingMessage.Command,
			)
		}
		return
	}
}