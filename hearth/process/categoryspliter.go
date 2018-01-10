package process

import (
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/goinggo/tracelog"
	"context"
	"fmt"
	"io"
	"bufio"
	"os"
	"io/ioutil"
	"bytes"
)

type  SplitConfigType struct {
	DumpReaderConfig handling.DumpReaderConfigType
    PathToSliceDirectory string

}

type categoryColumnDataType [][]byte

func (c categoryColumnDataType) Equal(data [][]byte) bool{

	if len(data) != len(c) {
		return false
	}
	for index := range c {
		if len(c[index]) != len(data[index]){
			return false
		}
		for position := range c[index]{
			if c[index][position] != data[index][position] {
				return false
			}
		}
	}
	//fmt.Printf("%v:%v\n",c,data)
	return true

//	return reflect.DeepEqual(c,data)
}

func (c categoryColumnDataType) Copy(data [][]byte){
	for index := range data {
		c[index] = make([]byte,len(data[index]))
		copy(c[index],data[index])
	}
}

func (c categoryColumnDataType) String() (result string){
	return string(bytes.Join(c,[]byte{0x1F}))
}
type outWriterType struct {
	io.Writer
	buffer *bufio.Writer
	file *os.File
}


func newOutWriter(pathToSliceDirectory string) (out *outWriterType,err error) {
	out = &outWriterType{}
	os.MkdirAll(pathToSliceDirectory,0x007)
	out.file, err = ioutil.TempFile(pathToSliceDirectory,"")
	if err != nil {
	}
	out.Writer = out.file
	out.buffer = nil

	out.buffer = bufio.NewWriter(out.file)
	out.Writer = out.buffer

	if false {
		fmt.Println(out.file.Name())
	}
	return
}


func (out *outWriterType) Close() (err error) {
	if out.buffer != nil {
		out.buffer.Flush()
	}
	return out.file.Close()
}


func Split(ctx context.Context, conf *SplitConfigType,categoryColumns dto.ColumnInfoListType) (err error){
	funcName := ""
	var targetTable *dto.TableInfoType
	if  len(categoryColumns) == 0 {

	}
	holder := make(map[string]*outWriterType)
//	var zipWriter *gzip.Writer

	targetTable = categoryColumns[0].TableInfo

	categoryPositions, err := targetTable.ColumnPositionFlags(categoryColumns,true)

	currentConfig := conf.DumpReaderConfig
	currentConfig.TableName = targetTable.String()
	currentConfig.TableColumnCount = len(targetTable.Columns)
	currentConfig.TableDumpFileName = targetTable.PathToFile.Value()

	lastCategoryRowData := make(categoryColumnDataType,len(categoryColumns))
	var currentOutWriter *outWriterType;

	categoryRowData := make(categoryColumnDataType,len(categoryColumns))

	processRowContent := func(
		ctx context.Context,
		lineNumber,
		DataPosition uint64,
		columnData [][]byte,
		original []byte,
	) (result handling.DumpReaderActionType, err error) {
		categoryNum := 0
		for columnNumber  := range targetTable.Columns {
			if categoryPositions[columnNumber] {
				categoryRowData[categoryNum] = columnData[columnNumber]
				categoryNum++
			}
		}

		if lastCategoryRowData.Equal(categoryRowData) {
			_, err = currentOutWriter.Write(original)
			if err != nil {

			}
		} else {
			found := false
			key := categoryRowData.String()
			if currentOutWriter, found = holder[key]; !found {
				currentOutWriter, err = newOutWriter(conf.PathToSliceDirectory)
				holder[key] = currentOutWriter
			} else {

			}
			/*if zipWriter == nil {
				zipWriter = gzip.NewWriter(currentOutWriter)
			} else {
				zipWriter.Flush()
				zipWriter.Reset(currentOutWriter)
			}*/
			_, err = currentOutWriter.Write(original)
			lastCategoryRowData.Copy(categoryRowData)
		}
		return
	}

		_, linesRead, err := handling.ReadAstraDump(
		ctx,
		&currentConfig,
		processRowContent,
	)

	//zipWriter.Flush()

	for _,h := range holder{
		h.Close()
	}

	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Error while reading table %v in line #%v ", targetTable, linesRead)
		return
	} else {
		tracelog.Info(packageName, funcName, "Table %v processed. %v lines have been read", targetTable, linesRead)
	}
	return
}