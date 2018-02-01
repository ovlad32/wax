package categorysplit
/*
import (
	"github.com/nats-io/nuid"
	"fmt"
	nats "github.com/nats-io/go-nats"
)

type CommandName string
const createCategorySplitSubject CommandName = "CS/C"

type BusCommandRequestType struct {
	command CommandName
	subject string
	nodeName string
}


type NewDumpMessageType struct {

}

type DumpMessageType struct {

}
type i interface {
	getBusName
}

type messager struct {
	conn *nats.EncodedConn
}

func (n messager) CreateChannel(tableId, splitId int64)(c interface{},err error) {
	nc,err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return

	}
	n.conn,err  = nats.NewEncodedConn(nc,nats.GOB_ENCODER)
	in := make(chan(BusCommandRequestType),0)
	n.conn.BindRecvChan("buf",in)
	select {
		case msg := <-in:
			n.
			msg.command
	}


	if tableId == 0 {
		panic(fmt.Sprintf("tableId is zero!"))
	}
	if splitId == 0 {
		panic(fmt.Sprintf("splitId is zero!"))
	}
	temp := fmt.Sprintf("t:%v/cs:%v",tableId,splitId)

	n.conn.Request("bus",)
	n.conn.Subscribe()
}


*/

