package dump

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"github.com/ovlad32/wax/hearth/handling"
)


type DumperStartFromLine struct {
	 Line uint64
}

type DumperStartFromByte struct {
	Position int
	FirstLine uint64
}

type DumperConfigType struct {
	GZip            bool
	ColumnSeparator byte
	LineSeparator   byte
	BufferSize      int
	StartFromLine   *DumperStartFromLine
	StartFromByte   *DumperStartFromByte
	Log             handling.Logger
}

type DumperType struct {
	config DumperConfigType
}

func NewDumpReader(cfg *DumperConfigType) (dumper *DumperType,err error){
	if cfg == nil {
		//TODO: fill me

	}

	err = validateConfig(cfg)
	if err != nil {
		return
	}
	return &DumperType{
		config: *cfg,
	},nil

}

type errorAbortedByType struct {
	error
	message string
}
func (e errorAbortedByType) Error() (string) {
	return e.message
}

type errorAbortedByRowProcessing errorAbortedByType

type errorAbortedByContext errorAbortedByType

type RowProcessingFuncType func(context.Context, uint64, uint64,[][]byte,[]byte) (error)


var (
	x0D = []byte{0x0D}
    defaultColumnSeparatorByte byte =0x1F
    defaultLineSeparatorByte byte =0x0A
	defaultBufferSize int =  4096
)

func IsErrorAbortedByRowProcessing(err error) bool {
	if err == nil {
		return false;
	}
	_, typeOf := err.(errorAbortedByRowProcessing)
	return typeOf;
}

func IsErrorByContext(err error) bool {
	if err == nil {
		return false;
	}
	_, typeOf := err.(errorAbortedByContext)
	return typeOf;
}




func validateConfig(cfg *DumperConfigType) (err error) {
	if cfg == nil {
		err = fmt.Errorf("config is not initialized")
	}

	if cfg.StartFromLine !=nil && cfg.StartFromByte != nil &&
		cfg.StartFromLine.Line > 0 && cfg.StartFromByte.Position > 0 {
		err = fmt.Errorf(
			"wrong parameters: mixture of mutually exceptional parameters: "+
				"config.MoveToLine > %v && config.MoveToByte.Position>%v",
			cfg.StartFromLine.Line,
			cfg.StartFromByte.Position,
		)
	}

	if cfg.ColumnSeparator == 0 {
		cfg.ColumnSeparator = defaultColumnSeparatorByte
	}

	if cfg.LineSeparator == 0 {
		cfg.LineSeparator = defaultLineSeparatorByte
	}

	if cfg.LineSeparator == 0 {
		cfg.BufferSize = defaultBufferSize
	}

	return
}



func (dumper *DumperType) ReadFromStream(
	ctx context.Context,
	stream io.Reader,
	rowProcessingFunc RowProcessingFuncType,
) (lineNumber uint64, err error) {
	var streamPosition uint64


	err = validateConfig(&dumper.config)
	if err != nil {
		return
	}

	if rowProcessingFunc == nil {
		err = fmt.Errorf(
			"row processing function must be defined",
		)
		return
	}



	if dumper.config.GZip {
		var zipped *gzip.Reader
		zipped,err = gzip.NewReader(stream)
		if err != nil {
			err = fmt.Errorf("couldn't create zip reader from stream: %v",err)
			return
		}
		stream = zipped
	}

	buffered := bufio.NewReaderSize(stream, dumper.config.BufferSize)
	if err != nil {
		err = fmt.Errorf("couldn't create buffer from stream: %v",err)
		return
	}


	if dumper.config.StartFromByte != nil && dumper.config.StartFromByte.Position > 0 {
		discarded, err := buffered.Discard(dumper.config.StartFromByte.Position)
		if err != nil {
			err = fmt.Errorf("could not discard stream to position %v: %v",
				dumper.config.StartFromByte.Position,
				err,
			)
			return
		}

		if discarded != dumper.config.StartFromByte.Position {
			err = fmt.Errorf("discarded position mismatch %v, expected: %v",
				dumper.config.StartFromByte.Position,
				uint64(discarded),
			)
			return
		}
		lineNumber =  dumper.config.StartFromByte.FirstLine
		streamPosition = uint64(discarded)
	}
	var columnSeparatorBytes []byte = []byte{dumper.config.ColumnSeparator}

	for {
		select {
		case <-ctx.Done():
			err = &errorAbortedByContext{}
			return
		default:
			originalLine, err := buffered.ReadSlice(dumper.config.LineSeparator)
			if err == io.EOF {
				return lineNumber, nil
			} else if err != nil {
				return lineNumber, fmt.Errorf(
					"couldn't read data from stream: %v",
					err,
				)
			}
			if dumper.config.StartFromLine == nil || lineNumber >= dumper.config.StartFromLine.Line{

				originalLineLength := len(originalLine)
				strippedLine := originalLine
				if strippedLine[originalLineLength-1] == dumper.config.LineSeparator {
					strippedLine = strippedLine[:originalLineLength-1]
				}
				if strippedLine[originalLineLength-2] == x0D[0] {
					strippedLine = strippedLine[:originalLineLength-2]
				}

				lineColumns := bytes.Split(strippedLine,columnSeparatorBytes )

				err = rowProcessingFunc(ctx, lineNumber, streamPosition, lineColumns, originalLine)

				if err != nil {
					return
				}

				lineNumber++
				streamPosition += uint64(originalLineLength)
			}
		}
	}
	return
}




func (dumper *DumperType) ReadFromFile(
	ctx context.Context,
	pathToFile string,
	rowProcessingFunc RowProcessingFuncType ,
) (lineNumber uint64, err error) {

	err = validateConfig(&dumper.config)
	if err != nil {
		return
	}

	if strings.TrimSpace(pathToFile) == "" {
		err = errors.New("pathToFile is empty")
		return
	}

	file, err := os.Open(pathToFile)
	if err != nil {
		err = fmt.Errorf("could not open file %v: %v", pathToFile,err)
		return
	}

	defer file.Close()

	return dumper.ReadFromStream(ctx,file,rowProcessingFunc)

}
