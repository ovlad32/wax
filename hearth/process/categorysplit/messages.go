package categorysplit

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
	nats.
	conn nats.E
}

func (n messager) CreateChannel(tableId, splitId int64)(c interface{},err error) {
	ct,err  := nats.NewEncodedConn(n.conn,nats.GOB_ENCODER)

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




