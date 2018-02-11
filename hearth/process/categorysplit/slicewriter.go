package categorysplit

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
)

const (
	bufferSize      int = 4 * 1024
	bufferSizeLimit     = 1 * 1024 * 1024 * 1024
)

type outerType interface {
	batchConsumer
}

type sliceWriterType struct {
	buffer       *bytes.Buffer
	tableId      int64
	sliceId      int64
	channel      interface{}
	outer        outerType
	totalWritten int64
}

func newSliceWriter(outer outerType, tableId, sliceId int64) (result *sliceWriterType, err error) {
	result.channel, err = outer.CreateChannel(tableId, sliceId)
	if err != nil {
		return
	}

	result = &sliceWriterType{
		buffer:  bytes.NewBuffer(make([]byte, bufferSize)),
		tableId: tableId,
		sliceId: sliceId,
	}
	return
}

func (s *sliceWriterType) tryToWrite(data []byte, flush ...bool) (written int, err error) {
	written, err = s.buffer.Write(data)
	if err == nil {
		s.totalWritten += int64(written)
		return
	}
	if err == bytes.ErrTooLarge {
		if len(flush) == 0 {
			err = s.flush()
			if err != nil {
				return
			}
			return s.tryToWrite(data, false)
		}
	}
	if err != nil {
		err = fmt.Errorf("could not write data to buffer %v ", err)
		return
	}
	return
}

func (s *sliceWriterType) Write(data []byte) (written int, err error) {

	l := int64(len(data))
	if l == 0 {
		return 0, nil
	}
	if s.totalWritten+l > bufferSizeLimit {
		err = s.flush()
	}
	return s.tryToWrite(data)
}

func (s *sliceWriterType) flush() (err error) {
	err = s.outer.Transfer(s.channel, s.buffer.Bytes())
	if err != nil {
		err = errors.Wrap(err, "could not write buffered data to channel")
		return
	}
	s.buffer.Truncate(0)
	return
}

func (s *sliceWriterType) Close() (err error) {
	if s.buffer.Len() > 0 {
		err = s.flush()
		if err != nil {
			return
		}
	}
	err = s.outer.CloseChannel(s.channel)
	if err != nil {
		return
	}
	s.channel = nil
	return
}
