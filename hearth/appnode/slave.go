package appnode

import (
	"fmt"
	pb "github.com/ovlad32/wax/hearth/grpcservice"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"path"
	"time"
)

func (node *ApplicationNodeType) StartSlaveNode(masterHost, masterPort, nodeIDDir string) (err error) {
	xContext := context.Background()
	logger := node.config.Logger
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
		logger:            node.config.Logger,
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
