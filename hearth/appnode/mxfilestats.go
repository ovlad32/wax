package appnode

import (
	"github.com/ovlad32/wax/hearth/appnode/message"
	"github.com/pkg/errors"
	"os"
)

const fileStats    message.Command = "FILE.STATS"



type MxFileStats struct {
	PathToFile string
	FileExists bool
	FileSize   int64
}


func(instance *MxFileStats) checkFilePath() (err error){
	if instance.PathToFile == "" {
		err = errors.New("Path to requested file is empty")
	}
	return
}

func (instance *MxFileStats) Run() (err error){

	if err = instance.checkFilePath(); err != nil {
		return
	}

	fileInfo, err := os.Stat(instance.PathToFile)
	if err != nil {
		err = errors.Wrapf(err, "could not find requested file at %v", instance.PathToFile)
		return
	}

	if !fileInfo.IsDir() {
		err = errors.Errorf("%v is a directory!",instance.PathToFile)
		return
	}

	instance.FileExists = true
	instance.FileSize = fileInfo.Size()

	return
}

