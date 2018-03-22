package appnode

import "encoding/gob"

func gobRegisterNewTypes() {
	gob.Register(&AgentMessage{})
}
