package natsu

import (
	"encoding/gob"
	"github.com/nats-io/go-nats"
	"github.com/ovlad32/wax/hearth/appnode/message"
	errors1 "github.com/pkg/errors"
	errors2 "errors"
	"github.com/pkg/errors"
)

type Communication struct {
	//di.Logruser
	//di.NatsEncodedConner
	encodedConn *nats.EncodedConn
}

func (m Communication) Enc() *nats.EncodedConn {
	return m.encodedConn
}

func (m *Communication) ConnectToNATS(
	url string,
	nodeId string,

) (err error) {
	conn, err := nats.Connect(
		url,
		nats.Name(nodeId),
	)
	if err != nil {
		err = errors.Wrapf(err, "could not connect to NATS at '%v'", url)
		return
	}

	gob.Register(errors1.New(""))
	gob.Register(errors2.New(""))


	m.encodedConn, err = nats.NewEncodedConn(conn, nats.GOB_ENCODER)
	if err != nil {
		err = errors.Wrap(err, "could not create encoder for NATS")
		return
	}
	return
}

func (m Communication) DisconnectFromNATS() {
	m.encodedConn.Close()
}

type MessageRequester interface {
	Request(string, *message.Body) (*message.Body, error)
}

func (m Communication) Request(
	subject string,
	request *message.Body,
) (result *message.Body, err error) {
	response := new(message.Body)
	err = m.Enc().Request(
		subject,
		request,
		response,
		nats.DefaultTimeout,
	)
	if err != nil {
		err = errors.Wrapf(err,
			"could not make command request %v ",
			request.Command,
		)
		return
	}

	if err = m.Flush(); err != nil {
		err = errors.Wrapf(err,"could not flush command request %v  ", request.Command)
		return
	}

	if response == nil {
		err = errors.Errorf("%v-command response is not initialized",
			request.Command,
		)
		return
	}
	if response.Command != request.Command {
		err = errors.Wrapf(err,
			"Response command %v is not expected. Expected: %v",
			response.Command, request.Command)
		return
	}
	if response.Error != nil {
		err = errors.Errorf("%v-command response with error: %v",
			request.Command,
			response.Error,
		)
		return
	}

	result = response
	return
}

type MessageReplier interface {
	Publish(string, *message.Body) error
}

func (m Communication) Reply(
	subject string,
	response *message.Body,
) (err error) {
	err = m.Enc().Publish(subject, response)
	if err != nil {
		err = errors.Wrapf(err, "could not publish '%v' response to subject %v ", response.Command, subject)
		return
	}
	return
}

type MessagePublisher interface {
	Publish(string, *message.Body) error
}

func (m Communication) Publish(
	subject string,
	msg *message.Body,
) (err error) {

	err = m.Enc().Publish(subject, msg)
	if err != nil {
		err = errors.Wrapf(err, "could not publish '%v' response ", msg.Command)
		return
	}

	if err = m.Flush(); err != nil {
		err = errors.Wrapf(err,"could not flush published command %v ", msg.Command)
		return
	}

	return
}


func (m Communication) SubscribeMessageTrigger(
	subject string,
	processor message.Trigger,
) (result *nats.Subscription, err error) {
	return m.subscribeFunc(subject, processor)
}

func (m Communication) SubscribeBareFunc(
	subject string,
	processor func(msg *nats.Msg),
) (result *nats.Subscription, err error) {
	return m.subscribeFunc(subject, processor)
}

func (m Communication) subscribeFunc(
	subject string,
	processor interface{},
) (result *nats.Subscription, err error) {
	var subscription *nats.Subscription
	subscription, err = m.Enc().Subscribe(
		subject,
		processor,
	)

	if err != nil {
		err = errors.Wrapf(err, "could not create subscription for subject: %v ", subject)
		return
	}

	if err = m.Flush(); err != nil {
		err = errors.Wrapf(err,"could not flush created subscription for subject: %v", subject)
		return
	}
	result = subscription
	return
}

func (m Communication) Flush() (err error){
	err = m.Enc().Flush()
	if err != nil {
		return
	}

	if err = m.Enc().LastError(); err != nil {
		err = errors.Wrapf(err, "could not wire flush in NATS")
		return
	}
	return
}