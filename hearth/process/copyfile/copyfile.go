package copyfile

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type ReaderInterface interface {
	Size(string) (uint64, error)
	Open(string) error
	io.ReadWriteCloser
}

type ReaderType struct {
	pathToFile string
	logger     *logrus.Logger
	reader     io.ReadCloser
}

func (c *ReaderType) Size(pathToFile string) (result uint64, err error) {
	if c == nil {
		err = errors.New("hearth::process::copyFile self reference is nil")
		panic(err)
	}

	pathToFile = strings.TrimSpace(pathToFile)
	if pathToFile == "" {
		err = errors.New("path to file to read is empty")
		if c.logger != nil {
			c.logger.Error(err)
		}
		return
	}

	s, err := os.Stat(pathToFile)
	if err != nil {
		err = errors.Wrapf(err, "could not get statistics of %v", pathToFile)
		if c.logger != nil {
			c.logger.Error(err)
		}
		return
	}

	result = uint64(s.Size())
	c.pathToFile = pathToFile
	return
}

func (c *ReaderType) Open(pathToFile string) (err error) {
	if c == nil {
		err = errors.New("hearth::process::copyFile self reference is nil")
		panic(err)
	}

	if pathToFile == "" {
		err = errors.New("path to file to read is empty")
		if c.logger != nil {
			c.logger.Error(err)
		}
		return
	}

	file, err := os.Open(c.pathToFile)
	if err != nil {
		err = errors.Wrapf(err, "could not open file %v", c.pathToFile)
		if c.logger != nil {
			c.logger.Error(err)
		}
		return
	}
	c.reader = file
	return nil
}

func (c *ReaderType) Read(buff []byte) (result int, err error) {
	if c == nil {
		err = errors.New("hearth::process::copyFile self reference is nil")
		panic(err)
	}

	result, err = c.reader.Read(buff)

	if err != nil {
		if err != io.EOF {
			err = errors.Wrapf(err, "could not read data from %v", c.pathToFile)
		}
	}
	return
}

func (c *ReaderType) Close() (err error) {
	if c == nil {
		err = errors.New("hearth::process::copyFile self reference is nil")
		panic(err)
	}

	if c.reader != nil {
		err = c.reader.Close()
		if err != nil {
			err = errors.Wrapf(err, "could not close %v", c.pathToFile)
			if c.logger != nil {
				c.logger.Error(err)
			}
			return err
		}
	}
	return nil
}

type copyFileWriterInterface interface {
	CheckSpace(string, uint64) (bool, error)
	Open(string) error
	io.WriteCloser
}

type copyFileWriterType struct {
	pathToFile string
	tempFile   string
	writer     io.WriteCloser
	logger     *logrus.Logger
}

func (c *copyFileWriterType) Open(pathToFile string) (err error) {
	if c == nil {
		err = errors.New("hearth::process::copyFile self reference is nil")
		panic(err)
	}

	pathToFile = strings.TrimSpace(pathToFile)
	if pathToFile == "" {
		err = errors.New("path to file to write is empty")
		return
	}
	dir, _ := path.Split(pathToFile)
	var tempFile *os.File
	tempFile, err = ioutil.TempFile(dir, "tmp_")
	if err != nil {
		err = errors.Wrapf(err, "could not create temp file at %v:", dir)
		if c.logger != nil {
			c.logger.Error(err)
		}
	}
	c.tempFile = tempFile.Name()
	c.writer = tempFile
	c.pathToFile = pathToFile
	return
}
