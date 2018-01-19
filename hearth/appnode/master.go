package appnode

import (
	"fmt"
	"net"
	"google.golang.org/grpc"
	pb "github.com/ovlad32/wax/hearth/grpcservice"
	"golang.org/x/net/context"
	"time"
	"github.com/sirupsen/logrus"
	"github.com/ovlad32/wax/hearth/dto"
)

func (node *ApplicationNodeType) StartMasterNode() (err error){
	backend := fmt.Sprintf(":%d",node.config.Port)
	listener, err := net.Listen(
		"tcp",
		backend,
		)
	if err!= nil {
		node.config.Log.Fatal("Could not start master at %v: %v",backend,err)
		return err
	}
	defer listener.Close()

	gServer := grpc.NewServer()
	pb.RegisterNodeManagerServer(gServer,masterEndPointType{})

	node.config.Log.Info("master server started at %v....",backend)

	err = gServer.Serve(listener)

	return
}
type slaveNodeInfoType struct {
	dto.AppNodeType
	lastHeartBeat time.Time
	counter uint64
}

type masterEndPointType struct {
	hostName string
	slaves map[string]*slaveNodeInfoType
	log logrus.Logger
}

func (s masterEndPointType) RegisterNode(ctx context.Context,request *pb.RegisterNodeRequest) (result *pb.RegisterNodeResponse,err error) {
	result = &pb.RegisterNodeResponse{

	}
	if request.NodeId == "" {
		if request.HostName == s.hostName {
			if request.StandaloneId =="" {
				result.ErrorMessage = "provide standalone id for standalone cluster"
				logrus.Errorf(result.ErrorMessage)
				return
			}
		} else {

		}
		slave := &slaveNodeInfoType{
			lastHeartBeat:time.Now(),
			}
		slave.Hostname = 
	}

	if slave, found := s.slaves[request.NodeId]; found {
		slave.lastHeartBeat = time.Now()
		slave.counter++
	}


	fmt.Println(pb.RegisterNodeRequest{})


	return
}

func (s *masterEndPointType) HeartBeatNode(ctx context.Context,request *pb.HeartBeatRequest) (result *pb.HeartBeatResponse,err error) {
	result = &pb.HeartBeatResponse{LastHeartBeat:"*"}
	if request.Status == "HB" {
		if request.NodeId == ""  {
			logrus.Errorf("HeartBeat NodeId is empty")
		}
		if slave, found := s.slaves[request.NodeId]; found {
			slave.lastHeartBeat = time.Now()
			slave.counter++
		}
	}
	return
}

