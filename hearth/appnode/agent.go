package appnode

import "github.com/ovlad32/wax/hearth/appnode/message"

const (
	agentRegister            message.Command = "AGENT.REGISTER"
	agentUnregister message.Command = "AGENT.UNREGISTER"
	agentTerminate message.Command = "AGENT.TERMINATE"
)

type MxAgent struct {
	CommandSubject string
	NodeId string
	RegisteredBefore bool
}

const (
	fileStats            message.Command = "FILE.STATS"
	fileCopy            message.Command = "FILE.COPY"
)

type MxFileStats struct {
	PathToFile string
	FileExists bool
	FileSize   int64
}

type MxFileCopy struct {
	SrcPathToFile string
	SrcNodeId     string
	DstPathToFile string
	DstNodeId     string
	Stage string
	DataSubject string
}

/*
func (node *SlaveNode) startServices() (err error) {

	err = node.registerCommandProcessors()

	if err != nil {
		err = errors.Wrapf(err, "could register command processors")
		return err
	}

	err = node.ConnectToNATS(node.config.NATSEndpoint,node.config.NodeId)
	if err != nil {
		return err
	}
	err = node.createCommandSubscription()
	if err != nil {
		return err
	}

	server, err := node.initRestApiRouting()
	if err != nil {
		return err
	}

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt, os.Kill)
	go func() {
		_ = <-osSignal

		if err = server.Shutdown(context.Background()); err != nil {
			err = errors.Wrapf(err, "could not shutdown REST server")
			node.Logger().Error(err)
		}

		//err = node.closeAllCommandSubscription()
		if err != nil {
			err = errors.Wrapf(err, "could not close command subscriptions")
			node.Logger().Error(err)
		}

		node.Logger().Warnf("Slave '%v' shut down", node.Id())
		os.Exit(0)
	}()
	return
}
*/
/*

func (node *SlaveNode) registerMaxPayloadSize(maxLoadedMsg *message.Message) (adjustment int64, err error) {
	subject := "a subject length does not matter now, so make it arbitrary size but long enough"
	encoded, err := node.encodedConn.Enc.Encode(subject, maxLoadedMsg)
	if err != nil {
		err = errors.Wrapf(err, "could not encode command command %v", maxLoadedMsg.Command)
		return
	}
	adjustment = node.encodedConn.Conn.MaxPayload() - int64(len(encoded))
	node.payLoadSizeMux.Lock()
	node.payloadSizeAdjustments[maxLoadedMsg.Command] = adjustment
	node.payLoadSizeMux.Unlock()

	return
}*/
/*
func (node *SlaveNode) workersHandlerFunc() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		workerList := make([]map[string]interface{}, 0, len(node.Workers))
		for _, instance := range node.Workers {
			workerList = append(workerList, instance.JSONMap())
		}
		je := json.NewEncoder(w)
		err := je.Encode(workerList)
		if err != nil {
			http.Error(w, err.Error(), http.StatusOK)
			return
		}
		return
	}
}

func (node *SlaveNode) Close(w task.Worker) (err error){

	return
}


func (node *SlaveNode) Done(String,ierr error) {

	return
}*/

/*

func (node *SlaveNode) initRestApiRouting() (srv *http.Server, err error) {

	r := mux.NewRouter()


	//defer node.wg.Done()
	address := fmt.Sprintf(":%d", node.config.RestAPIPort)
	srv = &http.Server{
		Handler: r,
		Addr:    address,
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}
	node.Logger().Infof("REST API server has started at %v....", address)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			if err == http.ErrServerClosed {
				node.Logger().Warn("REST API server closed")
				return
			}
			err = errors.Wrapf(err, "REST API server broke at %v: %v", address)
			node.Logger().Fatal(err)
		}
	}()

	return
}*/


/*
func (node slaveApplicationNodeType) reportError(
		command CommandType,
		incomingErr error,
		entries... *CommandMessageParamEntryType,
) (err error) {

	errMsg := &CommandMessageType{
		Command: command,
		Err:     incomingErr,
	}

	errMsg.Params = CommandMessageParamMap{
		slaveCommandSubjectParam:SubjectType(node.commandSubscription.Subject),
	}

	for _,e := range entries {
		errMsg.Params[e.Name] = e.Value
	}

	err = node.encodedConn.Publish(masterCommandSubject, errMsg)
	if err != nil {
		err = errors.Wrapf(err, "could not publish error command")
		return
	}
	if err = node.encodedConn.Flush(); err != nil {
		err = errors.Wrapf(err, "could not flush published error command")
		return
	}
	if err = node.encodedConn.LastError(); err != nil {
		err = errors.Wrapf(err, "could not wire published error command")
		return
	}
	return
}

*/
