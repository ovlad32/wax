package filesize

import (
	"github.com/pkg/errors"
	"os"
	"path"
	"strings"
)

type Sizer interface {
	Size(string) (uint64, error)
}

type Checker interface {
	CheckSpace(string, uint64) (bool, error)
}

type SizerType struct{}
type CheckerType struct{}

func (c SizerType) Size(pathToFile string) (result uint64, err error) {
	pathToFile = strings.TrimSpace(pathToFile)
	if pathToFile == "" {
		err = errors.New("path to file to read is empty")
		return
	}

	s, err := os.Stat(pathToFile)
	if err != nil {
		err = errors.Wrapf(err, "could not get statistics for %v", pathToFile)
		return
	}

	result = uint64(s.Size())
	return
}

func (c CheckerType) CheckSpace(pathToFile string, size uint64) (result bool, err error) {
	if size == 0 {
		result = true
		return
	}

	pathToFile = strings.TrimSpace(pathToFile)
	if pathToFile == "" {
		err = errors.New("path to check space is empty")
		return
	}

	dirName, fileName := path.Split(pathToFile)
	_ = fileName
	_ = dirName
	//TODO: implement!
	result = true
	return
}
