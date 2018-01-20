package appnode

import (
	"fmt"
	"net"
	pb "github.com/ovlad32/wax/hearth/grpcservice"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"time"
	"os"
	"io/ioutil"
	"path"
)

func (node *ApplicationNodeType) StartSlaveNode(masterHost, masterPort,nodeIDDir string) (err error) {
	xContext := context.Background()
	var storedNodeId string
	err = os.MkdirAll(nodeIDDir, 771)
	if err != nil {
		err = fmt.Errorf("could not create NodeId directory %v: %v", nodeIDDir, err)
		node.config.Log.Fatal(err)
		return
	}
	nodeIdFile := path.Join(nodeIDDir, "nodeId")

	{
		var nodeIdBytes []byte
		nodeIdBytes, err = ioutil.ReadFile(nodeIdFile)
		if !os.IsExist(err) {
			err = fmt.Errorf("could not read registered Node Id from %v: %v", err)
			node.config.Log.Fatal(err)
			return
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
				node.config.Log.Fatal("Could not connect to master at %v:%v", backend, err)
				return nil, err
			}
			fmt.Println(conn)
			return conn, err
		}),
		grpc.WithInsecure(),
	)

	if err != nil {
		node.config.Log.Fatal(err)
		return err
	}

	defer gconn.Close()
	client := pb.NewNodeManagerClient(gconn)

	_, err = client.HeartBeatNode(
		xContext,
		&pb.HeartBeatRequest{
			Status: "Init",
		},
	)
	if err != nil {
		node.config.Log.Fatal(err)
		return err
	}

	request := &pb.RegisterNodeRequest{
		NodeId:       storedNodeId,
		LocalAddress: fmt.Sprintf("%v", conn.LocalAddr()),
	}
	request.HostName, err = os.Hostname()
	if err != nil {
		node.config.Log.Fatal(err)
		return err
	}

	response, err := client.RegisterNode(xContext, request)

	if err != nil {
		err = fmt.Errorf("could not call grpc:RegisterNode : %v", err)
		node.config.Log.Fatal(err)
		return err
	}
	if response.ErrorMessage != "" {
		err = fmt.Errorf("Message given from grpc:RegisterNode:%v", response.ErrorMessage)
		node.config.Log.Fatal(err)
		return err
	}
	if response.NodeId == "" {
		err = fmt.Errorf("grpc:RegisterNode returned empty NodeId")
		node.config.Log.Fatal(err)
		return err
	}
	err = ioutil.WriteFile(nodeIdFile, []byte(response.NodeId), 0644)
	if err != nil {
		err = fmt.Errorf("could not save the given node id to %v: %v ", nodeIdFile, err)
		node.config.Log.Fatal(err)
		return err
	}
	return
}
