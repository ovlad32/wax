package appnode

import (
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	pb "github.com/ovlad32/wax/hearth/grpcservice"
	"github.com/ovlad32/wax/hearth/repository"
	"github.com/sirupsen/logrus"
	xcontext "golang.org/x/net/context"
	"context"
	"google.golang.org/grpc"
	"net"
	"strconv"
	"time"
)

func (node *ApplicationNodeType) initAppNodeService() (err error) {

	gServer := grpc.NewServer()

	pb.RegisterAppNodeManagerServer(
		gServer,
		&appNodeServiceType{
			logger: node.config.Logger,
			slaves: make(map[string]*slaveNodeInfoType),
		},
	)

	go func() {
		defer node.wg.Done()
		address := fmt.Sprintf(":%d", node.config.GrpcPort)
		listener, err := net.Listen(
			"tcp",
			address,
		)

		if err != nil {
			node.config.Logger.Fatalf("could not start grpc appNodeService listener at %v: %v", address, err)
			return
		}

		node.config.Logger.Infof("grpc appNodeService has started at %v....", address)
		err = gServer.Serve(listener)
		if err != nil {
			node.config.Logger.Errorf("grpc appNodeService at %v broke: %v", address, err)
		}
	}()
	return
}

type slaveNodeInfoType struct {
	*dto.AppNodeType
	lastHeartBeat time.Time
	counter       uint64
}

func (s *slaveNodeInfoType) UpdateLastHeartbeatTime() {
	s.lastHeartBeat = time.Now()
	s.AppNodeType.LastHeartbeat = fmt.Sprintf("%v-%v-%v %v:%v:%v.%v ",
		s.lastHeartBeat.Year(),
		s.lastHeartBeat.Month(),
		s.lastHeartBeat.Day(),
		s.lastHeartBeat.Hour(),
		s.lastHeartBeat.Minute(),
		s.lastHeartBeat.Second(),
		s.lastHeartBeat.Nanosecond(),
	)

}

type appNodeServiceType struct {
	slaves map[string]*slaveNodeInfoType
	logger *logrus.Logger
}

func (s slaveNodeInfoType) NodeId() string {
	if s.Id.Valid() {
		return strconv.FormatInt(s.Id.Value(), 10)
	} else {
		return ""
	}
}
func (s *appNodeServiceType) AppNodeRegister(
	ctx xcontext.Context,
	request *pb.AppNodeRegisterRequest,
) (result *pb.AppNodeRegisterResponse,
	err error,
) {
	result = new(pb.AppNodeRegisterResponse)

	if request.NodeId == "" {
		slave := new(slaveNodeInfoType)
		slave.AppNodeType = new(dto.AppNodeType)
		slave.UpdateLastHeartbeatTime()
		slave.Hostname = request.HostName
		slave.Address = request.LocalAddress
		slave.State = "A"
		slave.Role = "SLAVE"
		err = repository.PutAppNode(context.Background(),slave.AppNodeType)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("%v", err)
			s.logger.Error(err)
			return result, nil
		}
		result.NodeId = strconv.FormatInt(slave.Id.Value(), 10)
		s.slaves[result.NodeId] = slave
		s.logger.Infof(
			"New Application Node hosted on %v (%v) is registered with NodeID %v",
			slave.Hostname,
			slave.Address,
			result.NodeId,
			)
	} else {
		if slave, found := s.slaves[request.NodeId]; found {
			slave.UpdateLastHeartbeatTime()
			result.NodeId = slave.NodeId()
		} else {
			var id int64
			id, err = strconv.ParseInt(request.NodeId, 10, 64)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("%v", err)
				s.logger.Error(err)
				return
			}
			slave := new(slaveNodeInfoType)
			slave.AppNodeType, err = repository.AppNameById(context.Background(), id)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("%v", err)
				s.logger.Error(err)
				return
			}
			if slave.AppNodeType == nil {
				result.ErrorMessage = fmt.Sprintf(
					"Application NodeId #%v hosted on %v (%v) was never registered",
					request.NodeId,
					request.HostName,
					request.LocalAddress,
					)
				s.logger.Error(err)
				return
			}
			slave.UpdateLastHeartbeatTime()
			slave.Hostname = request.HostName
			slave.Address = request.LocalAddress
			slave.State = "A"
			slave.Role = "SLAVE"
			err = repository.PutAppNode(context.Background(),slave.AppNodeType)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("%v", err)
				s.logger.Error(err)
				return result, nil
			}
			result.NodeId = slave.NodeId()
			s.slaves[request.NodeId] = slave
		}
		s.logger.Infof(
			"Application NodeId #%v hosted on %v (%v) has been recognized",
			request.NodeId,
			request.HostName,
			request.LocalAddress,
		)
	}
	return
}

func (s *appNodeServiceType) AppNodeHeartBeat(
	ctx xcontext.Context,
	request *pb.HeartBeatRequest,
) (
	result *pb.HeartBeatResponse,
	err error,
) {
	result = &pb.HeartBeatResponse{LastHeartBeat: "*"}
	if request.Status == "HB" {
		if request.NodeId == "" {
			err = fmt.Errorf("HeartBeat NodeId is empty")
			logrus.Error(err)
		}
		if slave, found := s.slaves[request.NodeId]; found {
			slave.lastHeartBeat = time.Now()
			slave.counter++
		}
	}
	return
}



