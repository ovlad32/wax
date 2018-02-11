package categorysplit

import (
	"bytes"
	"encoding/gob"
	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
)

type dumper struct {
	c    *nats.Conn
	w    io.Writer
	s    *nats.Subscription
	path string
	log  *logrus.Logger
}

func (d dumper) EncodeResponse(resp *dumperResponse) (data []byte, err error) {
	//TODO:
	return

}

type ChannelMessageType struct {
	Written int
	Err     error
}

type dumperResponse struct {
	written int
	err     error
}

func (resp *dumperResponse) deserialize() (data []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 10))
	enc := gob.NewEncoder(buff)
	err = enc.Encode(*resp)
	if err != nil {
		err = errors.Wrapf(err, "could not deserialize splitDump response object")
		return
	}
	return buff.Bytes(), nil
}

func (d dumper) SubscriptionHandlerFunc() func(msg *nats.Msg) {

	return func(msg *nats.Msg) {
		resp := dumperResponse{}
		var err error
		resp.written, err = d.w.Write(msg.Data)
		if err != nil {
			resp.err = errors.Wrapf(err, "could not persist received data to %v", d.path)
			d.log.Error(err)
		}
		data, err := resp.deserialize()
		if err != nil {
			d.log.Error(err)
		}
		d.c.Publish(msg.Reply, data)
	}

}

func (d *dumper) Subscribe(subj string) (err error) {
	d.s, err = d.c.Subscribe(subj, d.SubscriptionHandlerFunc())
	return
}

func (d *dumper) Unsubscribe() {
	d.s.Unsubscribe()
}
