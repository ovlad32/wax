package appnode

import (
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	pb "github.com/ovlad32/wax/hearth/grpcservice"
	"github.com/ovlad32/wax/hearth/repository"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"strconv"
	"time"
)

func (node *ApplicationNodeType) StartMasterNode() (err error) {
	backend := fmt.Sprintf(":%d", node.config.Port)
	listener, err := net.Listen(
		"tcp",
		backend,
	)
	if err != nil {
		node.config.Logger.Fatal("Could not start master at %v: %v", backend, err)
		return err
	}
	defer listener.Close()

	gServer := grpc.NewServer()
	pb.RegisterAppNodeManagerServer(gServer, &registerAppNodeServiceType{
		logger: node.config.Logger,
		slaves:  make(map[string]*slaveNodeInfoType),
	},
	)

	node.config.Logger.Info("grpc masterNode server started at %v....", backend)
	go func() {
		err = gServer.Serve(listener)
		node.config.Logger.Warn("NodeRegiSat %v....", backend)
	}()

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

func startMasterNodeRoutine(node *ApplicationNodeType) {
}

type slaveNodeInfoType struct {
	*dto.AppNodeType
	lastHeartBeat time.Time
	counter       uint64
}

func (s *slaveNodeInfoType) UpdateLastHeartbeatTime() {
	s.lastHeartBeat = time.Now()
	s.AppNodeType.LastHeartbeat = fmt.Sprintf("%v", s.lastHeartBeat)
}

type registerAppNodeServiceType struct {
	slaves map[string]*slaveNodeInfoType
	logger *logrus.Logger
}

func (s *registerAppNodeServiceType) AppNodeRegister(ctx context.Context, request *pb.AppNodeRegisterRequest) (result *pb.AppNodeRegisterResponse, err error) {
	result = new(pb.AppNodeRegisterResponse)

	if request.NodeId == "" {
		slave := &slaveNodeInfoType{
			lastHeartBeat: time.Now(),
		}
		slave.UpdateLastHeartbeatTime()
		slave.Hostname = request.HostName
		slave.Address = request.LocalAddress
		slave.State = "A"
		slave.Role = "SLAVE"
		err = repository.PutAppNode(slave.AppNodeType)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("%v", err)
			s.logger.Error(err)
			return result, nil
		}
		result.NodeId = strconv.FormatInt(slave.Id.Value(), 10)
		s.slaves[result.NodeId] = slave
	} else {
		if slave, found := s.slaves[request.NodeId]; found {
			slave.UpdateLastHeartbeatTime()
		} else {
			var id int64
			id, err = strconv.ParseInt(request.NodeId, 10, 64)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("%v", err)
				s.logger.Error(err)
				return
			}

			slave.AppNodeType, err = repository.AppNameById(context.Background(), id)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("%v", err)
				s.logger.Error(err)
				return
			}
			if slave.AppNodeType == nil {
				result.ErrorMessage = fmt.Sprintf("Your NodeId {%v} is not recoginzed", request.NodeId)
				s.logger.Error(err)
				return
			}
			slave.UpdateLastHeartbeatTime()
			slave.Hostname = request.HostName
			slave.Address = request.LocalAddress
			slave.State = "A"
			slave.Role = "SLAVE"
			err = repository.PutAppNode(slave.AppNodeType)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("%v", err)
				s.logger.Error(err)
				return result, nil
			}
			result.NodeId = strconv.FormatInt(slave.Id.Value(), 10)
			s.slaves[request.NodeId] = slave
		}
	}
	return
}

func (s *registerAppNodeServiceType) AppNodeHeartBeat(ctx context.Context, request *pb.HeartBeatRequest) (result *pb.HeartBeatResponse, err error) {
	result = &pb.HeartBeatResponse{LastHeartBeat: "*"}
	if request.Status == "HB" {
		if request.NodeId == "" {
			logrus.Errorf("HeartBeat NodeId is empty")
		}
		if slave, found := s.slaves[request.NodeId]; found {
			slave.lastHeartBeat = time.Now()
			slave.counter++
		}
	}
	return
}
