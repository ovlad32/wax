package filesize

import (
	"github.com/ovlad32/wax/hearth/appnode"
	"os"
)

const fileSize appnode.CommandType = "FILE.STATS"

func (node appnode.SlaveNodeType) fileStatsCommandFunc() commandFuncType {
	return func(
		subject,
		replySubject string,
		incomingMessage *CommandMessageType,
	) (err error) {
		replyParams := NewCommandMessageParams(3)
		reply := func() (err error) {
			err = node.PublishCommandResponse(
				replySubject,
				fileStats,
				replyParams...,
			)
			if err != nil {
				err = errors.Wrapf(err, "could not publish %v response", fileStats)
				return
			}
			return
		}

		filePath := incomingMessage.ParamString(filePathParam, "")
		stats, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				replyParams = replyParams.Append(fileNotExistsParam, true)
			} else {
				err = errors.Wrapf(err, "could not get file statistics for %v", filePath)
				node.logger.Error(err)
				replyParams = replyParams.Append(errorParam, err)
			}
		} else {
			replyParams = replyParams.Append(fileSizeParam, stats.Size())
		}
		err = reply()
		return
	}
}
