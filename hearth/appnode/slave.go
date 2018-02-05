package appnode

import (
	"os"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"fmt"
)


func (node ApplicationNodeType) SlaveCommandSubject() string {
	return fmt.Sprintf("COMMAND/%v", node.config.NodeName)
}
const (
	slaveCommandSubjectParam  CommandMessageParamType = "slaveCommandSubject"
	ResubscribedParam CommandMessageParamType = "resubscribed"
)



func (node *ApplicationNodeType) MakeSlaveCommandSubscription() (err error) {

	slaveSubject := node.SlaveCommandSubject()
	node.commandSubscription, err = node.encodedConn.Subscribe(
		slaveSubject,
		node.SlaveCommandSubscriptionFunc(),
	)
	if err != nil {
		err = errors.Wrapf(err,"could not create slave command subject subscription for node id  %v",node.NodeId())
		node.logger.Error(err)
		return
	}



	if func() (err error) {

		err = node.encodedConn.Flush()
		if err != nil {
			err = errors.Wrapf(err, "could not flush slave command subject subscription for node id", node.NodeId())
			node.logger.Error(err)
			return
		}

		if err = node.encodedConn.LastError(); err != nil {
			err = errors.Wrapf(err, "error given via NATS while propagating slave command subject subscription for node id", node)
			node.logger.Error(err)
			return
		}



		response := new(CommandMessageType)
		err = node.encodedConn.Request(
			MASTER_COMMAND_SUBJECT,
			&CommandMessageType{
				Command: PARISH_OPEN,
				Params: map[CommandMessageParamType]interface{}{
					slaveCommandSubjectParam: node.commandSubscription.Subject,
				},
			},
			response,
			nats.DefaultTimeout,
		)

		if err != nil {
			err = errors.Wrapf(err, "could not inform master about slave command subject creation of NodeID %v ", node.NodeId())
			node.logger.Error(err)
			return
		}

		err = node.encodedConn.Flush()
		if err != nil {
			err = errors.Wrapf(err, "could not flush registration request")
			node.logger.Error(err)
			return
		}

		if err = node.encodedConn.LastError(); err != nil {
			err = errors.Wrap(err, "error given via NATS while making slave registration command subscription")
			node.logger.Error(err)
			return
		}

		node.logger.Info("Slave %v registration has been done")
		//if response.Command == PARISH_OPENED
		//TODO:  PARISH_OPENED
		if response.ParamBool(ResubscribedParam,false)  {
			node.logger.Warn("slave node command subscription had been created before...")
			//Todo: clean
		}

		return
	} () != nil {
		node.commandSubscription.Unsubscribe()
		node.commandSubscription = nil
	}
	node.logger.Info("Slave %v command subscription has been done")
	return
}


func (node *ApplicationNodeType) SlaveCommandSubscriptionFunc() func(string, string, *CommandMessageType) {
	return func(subj, reply string, msg *CommandMessageType) {
			switch msg.Command {
			case PARISH_CLOSE:
				/*err := node.CloseRegularWorker(
					reply,
					msg,
					PARISH_CLOSED,
				)
				if err != nil {
					panic(err.Error())
				}*/
				node.encodedConn.Close()
				os.Exit(0)
			case CATEGORY_SPLIT_OPEN:
				worker, err := newCategorySplitWorker(
					node.encodedConn,
					reply, msg,
					node.logger,
				)
				if err != nil {
					panic(err.Error())
				}
				node.AppendWorker(worker)
			case CATEGORY_SPLIT_CLOSE:
				err := node.CloseRegularWorker(reply,msg,CATEGORY_SPLIT_CLOSED)
				if err != nil {
					panic(err.Error())
				}
			default:
				panic(msg.Command)
			}
	}
}




/*
func (node *ApplicationNodeType) StartSlaveNode(masterHost, masterPort, nodeIDDir string) (err error) {
	xContext := context.Background()
	logger := node.logger
	var storedNodeId string
	err = os.MkdirAll(nodeIDDir, 771)
	if err != nil {
		err = fmt.Errorf("could not create NodeId directory %v: %v", nodeIDDir, err)
		logger.Fatal(err)
		return
	}
	nodeIdFile := path.Join(nodeIDDir, "nodeId")

	{
		var nodeIdBytes []byte
		nodeIdBytes, err = ioutil.ReadFile(nodeIdFile)
		if err != nil {
			if !os.IsNotExist(err) {
				err = fmt.Errorf("could not read registered Node Id from %v: %v", nodeIdFile, err)
				logger.Fatal(err)
				return
			}
		}
		if nodeIdBytes != nil && len(nodeIdBytes) > 0 {
			storedNodeId = string(nodeIdBytes)
		}
	}

	backend := fmt.Sprintf("%v:%v", masterHost, masterPort)
	var conn net.Conn

	gconn, err := grpc.Dial(
		backend,
		grpc.WithDialer(func(s string, duration time.Duration) (net.Conn, error) {
			conn, err = net.DialTimeout("tcp", backend, duration)
			if err != nil {
				err = fmt.Errorf("could not connect to master at %v: %v", backend, err)
				logger.Fatal(err)
				return nil, err
			}
			return conn, err
		}),
		grpc.WithInsecure(),
	)

	if err != nil {
		logger.Fatal(err)
		return err
	}
	//TODO: Really close need?

	//defer gconn.Close()
	client := pb.NewAppNodeManagerClient(gconn)

	_, err = client.AppNodeHeartBeat(
		xContext,
		&pb.HeartBeatRequest{
			Status: "Init",
		},
	)

	if err != nil {
		logger.Fatal(err)
		return err
	}

	request := &pb.AppNodeRegisterRequest{
		NodeId:       storedNodeId,
		LocalAddress: fmt.Sprintf("%v", conn.LocalAddr()),
	}
	request.HostName, err = os.Hostname()
	if err != nil {
		logger.Fatal(err)
		return err
	}

	response, err := client.AppNodeRegister(xContext, request)

	if err != nil {
		err = fmt.Errorf("could not call grpc:RegisterNode : %v", err)
		logger.Fatal(err)
		return err
	}
	if response.ErrorMessage != "" {
		err = fmt.Errorf("message given from grpc:RegisterNode:%v", response.ErrorMessage)
		logger.Fatal(err)
		return err
	}
	if response.NodeId == "" {
		err = fmt.Errorf("grpc:RegisterNode returned empty NodeId")
		logger.Fatal(err)
		return err
	}
	err = ioutil.WriteFile(nodeIdFile, []byte(response.NodeId), 0644)
	if err != nil {
		err = fmt.Errorf("could not save the given node id to %v: %v ", nodeIdFile, err)
		logger.Fatal(err)
		return err
	}
	if storedNodeId == "" {
		logger.Infof("Master Node registered this Node with NodeId #%v",response.NodeId)
	} else {
		logger.Infof("Master Node accepted this NodeId #%v",response.NodeId)
	}
	heartBeatTicker := time.NewTicker(time.Duration(node.config.SlaveHeartBeatSeconds) * time.Second)
	go func(
		hbTicker *time.Ticker,
		client pb.AppNodeManagerClient,
		logger *logrus.Logger,
		nodeId string,
	) {
		for {
			select {
			case _, opened := <-hbTicker.C:
				if opened {
					backgroundHeartBeatRoutine(client, logger, response.NodeId, "ALIVE")
				}
				if !opened {
					return
				}
			}
		}
	}(heartBeatTicker, client, logger, response.NodeId)

	address := ":9101"

	listener, err := net.Listen("tcp", address)
	if err != nil {
		err = fmt.Errorf("could not start listener at %v: %v ",address,err)
	}
	srv := grpc.NewServer()

	pb.RegisterDataManagerServer(srv, &dataManagerServiceType{
		logger:            node.logger,
		categorySlicePath: ".",
		//TODO:
	})

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt,os.Kill)
	//signal.Reset(osSignal, os.Interrupt,os.Kill)
	go func(hbTicker *time.Ticker, client pb.AppNodeManagerClient, server *grpc.Server, logger *logrus.Logger, NodeId string) {
		for {
			select {
			case _ = <-osSignal:
				hbTicker.Stop()
				srv.GracefulStop()
				backgroundHeartBeatRoutine(
					client,
					logger,
					NodeId,
					"INTERRUPTED",
				)
			}
		}
	}(
		heartBeatTicker,
		client,
		srv,
		logger,
		response.NodeId,
	)
	//	var wg sync.WaitGroup
	//wg.Add(1)
	//fmt.Errorf()
	err = srv.Serve(listener)

	return
}

func backgroundHeartBeatRoutine(
	client pb.AppNodeManagerClient,
	logger *logrus.Logger,
	nodeId string,
	status string,
) {
	response, err := client.AppNodeHeartBeat(
		context.Background(),
		&pb.HeartBeatRequest{
			NodeId: nodeId,
			Status: status,
		},
	)
	if err != nil {
		err = fmt.Errorf("could not call grpc:HeartBeat method: %v", err)
		if logger != nil {
			logger.Error(err)
		}
	}
	if response.ErrorMessage != "" {
		err = fmt.Errorf("grpc:HeartBeat method responce: %v", response.ErrorMessage)
		if logger != nil {
			logger.Error(err)
		}
	}
	if status == "ALIVE" {
		logger.Infof("NodeId #%v HeartBeat has been called", nodeId)
	} else if status == "INTERRUPTED" {
		logger.Warnf("NodeId #%v HeartBeat: Interruption occurred ", nodeId)
	}

}

type dataManagerServiceType struct {
	logger            *logrus.Logger
	categorySlicePath string
}

func (s dataManagerServiceType) CategorySplitCollect(
	ctx context.Context,
	request *pb.CategorySplitRequest) (
	response *pb.CategorySplitResponse,
	err error,
) {
	response = new(pb.CategorySplitResponse)
	request.


	fileName := path.Join(s.categorySlicePath, request.RelativeStoringFile)
	fout, err := os.Create(fileName)
	if err != nil {
		err = fmt.Errorf("could not create the specified file  %v: %v", fileName, err)
		s.logger.Error(err)
		response.ErrorMessage = fmt.Sprintf("%v", err)
		return
	}
	defer fout.Close()
	written, err := fout.Write(request.Data)
	if err != nil {
		err = fmt.Errorf("could not write data to the specified file %v: %v", fileName, err)
		s.logger.Error(err)
		response.ErrorMessage = fmt.Sprintf("%v", err)
		return
	}
	if written < 0 {
		err = fmt.Errorf("number of written bytes is less than 0")
		s.logger.Error(err)
		response.ErrorMessage = fmt.Sprintf("%v", err)
		return
	}
	response.SizeWritten = uint64(written)
	return
}
*/