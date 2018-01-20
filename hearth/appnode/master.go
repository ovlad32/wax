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
	"github.com/ovlad32/wax/hearth/repository"
	"strconv"
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
	pb.RegisterNodeManagerServer(gServer,&registerNodeServerType{
		log:node.config.Log,
		slaves:make(map[string]*slaveNodeInfoType),
		},
		)

	node.config.Log.Info("master server started at %v....",backend)

	err = gServer.Serve(listener)

	return
}
type slaveNodeInfoType struct {
	*dto.AppNodeType
	lastHeartBeat time.Time
	counter uint64
}
func (s *slaveNodeInfoType) UpdateLastHeartbeatTime() {
	s.lastHeartBeat = time.Now()
	s.AppNodeType.LastHeartbeat = fmt.Sprintf("%v",s.lastHeartBeat)
}

type registerNodeServerType struct {
	slaves map[string]*slaveNodeInfoType
	log *logrus.Logger
}

func (s *registerNodeServerType) RegisterNode(ctx context.Context,request *pb.RegisterNodeRequest) (result *pb.RegisterNodeResponse,err error) {
	result = new(pb.RegisterNodeResponse)

	if request.NodeId == "" {
		slave := &slaveNodeInfoType{
			lastHeartBeat:time.Now(),
			}
		slave.UpdateLastHeartbeatTime()
		slave.Hostname = request.HostName
		slave.Address = request.LocalAddress
		slave.State = "A"
		slave.Role = "SLAVE"
		err = repository.PutAppNode(slave.AppNodeType)
		if err != nil {
			result.ErrorMessage = fmt.Sprintf("%v", err)
			s.log.Error(err)
			return result, nil
		}
		result.NodeId = strconv.FormatInt(slave.Id.Value(),10)
		s.slaves[result.NodeId] = slave
	} else {
		if slave, found := s.slaves[request.NodeId]; found {
			slave.UpdateLastHeartbeatTime()
		} else {
			var id int64
			id, err = strconv.ParseInt(request.NodeId,10,64)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("%v",err)
				s.log.Error(err)
				return
			}

			slave.AppNodeType,err = repository.AppNameById(context.Background(), id)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("%v",err)
				s.log.Error(err)
				return
			}
			if slave.AppNodeType == nil {
				result.ErrorMessage = fmt.Sprintf("Your NodeId {%v} is not recoginzed", request.NodeId)
				s.log.Error(err)
				return
			}
			slave.UpdateLastHeartbeatTime()
			slave.Hostname = request.HostName
			slave.Address = request.LocalAddress
			slave.State = "A"
			slave.Role = "SLAVE"
			err = repository.PutAppNode(slave.AppNodeType)
			if err != nil {
				result.ErrorMessage = fmt.Sprintf("%v",err)
				s.log.Error(err)
				return result,nil
			}
			result.NodeId = strconv.FormatInt(slave.Id.Value(),10)
			s.slaves[request.NodeId] = slave
		}
	}
	return
}

func (s *registerNodeServerType) HeartBeatNode(ctx context.Context,request *pb.HeartBeatRequest) (result *pb.HeartBeatResponse,err error) {
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

