package appnode

import (
	"fmt"
	"net"
	pb "github.com/ovlad32/wax/hearth/grpcservice"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"time"
	"os"
)

func (node *ApplicationNodeType) StartSlaveNode(standaloneId, masterHost,masterPort string) (err error){
	backend := fmt.Sprintf("%v:%v",masterHost,masterPort)
	var conn net.Conn

	gconn, err := grpc.Dial(
		backend,
		grpc.WithDialer(func(s string, duration time.Duration) (net.Conn, error) {
			conn, err = net.DialTimeout("tcp",backend,duration)
			if err!= nil {
				node.config.Log.Fatal("Could not connect to master at %v:%v",backend,err)
				return nil, err
			}
			fmt.Println(conn)
			return conn,err
		}),
		grpc.WithInsecure(),
	)

	if err != nil {
		node.config.Log.Fatal(err)
		return err
	}
	defer gconn.Close()
	client := pb.NewNodeManagerClient(gconn)

	response1,err := client.HeartBeatNode(context.Background(), &pb.HeartBeatRequest{
		Status: "initial",
	},
	)
	if err != nil {
		node.config.Log.Fatal(err)
		return err
	}
	fmt.Println("response1:",response1)
	fmt.Println(conn)
	fmt.Println()
	fmt.Println(conn.RemoteAddr().Network())
	fmt.Println(os.Hostname())


	response2, err := client.RegisterNode(context.Background(),
		&pb.RegisterNodeRequest{
			LocalAddress:fmt.Sprintf("%v",conn.LocalAddr()),
			StandaloneId:standaloneId,
			HostName:"321",
			})

	if err != nil {
		node.config.Log.Fatal(err)
		return err
	}
	fmt.Println("response2:",response2)
	return
}
