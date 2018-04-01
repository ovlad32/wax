package appnode

import "encoding/gob"

func gobRegisterNewTypes() {
	gob.Register(&MxAgent{})
	gob.Register(&MxFileStats{})
}
