package appnode

import (
	"github.com/pkg/errors"
)

func (node *MasterNode) initNATSService() (err error) {

	func() {
		err = node.connectToNATS()

		if err != nil {
			return
		}
		err = node.makeCommandSubscription()
		if err != nil {
			return
		}
	}()
	if err != nil {
		err = errors.Wrapf(err, "could not start NATS service")
	}
	return
}
