package appnode

import (
	"strings"
	"github.com/nats-io/nuid"
	"fmt"
)

type Subject string
type Id string


func (i Id) String() string {
	return string(i)
}
func (i Id) IsEmpty() bool {
	return i == ""
}

func (i Id) CommandSubject() Subject {
	if i.IsEmpty() {
		panic("NodeId is empty!")
	}
	if i == masterNodeId {
		return MasterCommandSubject()
	} else {
		return SlaveCommandSubject(i)
	}
}



func (s Subject) String() string {
	return string(s)
}
func (s Subject) IsEmpty() bool {
	return s == ""
}

func NewSubject(prefix string) Subject {
	prefix = strings.TrimSpace(prefix)
	if prefix != "" {
		prefix = prefix+"/"
	}
	return Subject(fmt.Sprintf("%v%v",prefix,nuid.Next()))
}


