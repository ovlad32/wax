package categorysplit

import (
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/process/dump"
	"context"
	"fmt"
	"io"
	"bufio"
	"os"
	"io/ioutil"
	"bytes"
	"github.com/ovlad32/wax/hearth/handling"
	"strings"
	"path"
	"strconv"
)

type CategorySplitConfigType struct {
	DumpReaderConfig *dump.DumperConfigType
	PathToSliceDirectory string
	log handling.Logger
}
func validateCategorySplitConfig(cfg *CategorySplitConfigType) (err error) {
	if cfg == nil {
		err = fmt.Errorf("config is not initialized")
		return
	}
	if strings.TrimSpace(cfg.PathToSliceDirectory) == "" {
		err = fmt.Errorf(" PathToSliceDirectory is not defined!")
	}
	return
}

type CategorySplitterType struct {
	config CategorySplitConfigType
}

func NewCategorySpliter(cfg *CategorySplitConfigType) (splitter *CategorySplitterType,err error) {
	//TODO: fill me
	err = validateCategorySplitConfig(cfg)
	if err != nil {
		err = fmt.Errorf("coult not create a new category splitter: %v",err)
		return
	}

	splitter = &CategorySplitterType{
		config:*cfg,
	}

	return

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
	writer io.Writer
	buffer *bufio.Writer
	file *os.File
	fileName string
}


func newOutWriter(pathToSliceDirectory string) (out *outWriterType,err error) {
	out = &outWriterType{}
	os.MkdirAll(pathToSliceDirectory,0711)
	out.file, err = ioutil.TempFile(pathToSliceDirectory,"")
	if err != nil {
		err = fmt.Errorf("could not create a temp file at %v: %v",pathToSliceDirectory,err)
		return
	}
	out.fileName = out.file.Name()

	out.writer = out.file
	out.buffer = nil

	out.buffer = bufio.NewWriter(out.file)
	out.writer = out.buffer

	if false {
		fmt.Println(out.file.Name())
	}
	return
}
func (out *outWriterType) Reopen() (err error) {
	out.file, err = os.OpenFile(out.fileName,os.O_APPEND,0x007)
	if err != nil {
		err = fmt.Errorf("could not reopen file %v for appending: %v",out.fileName,err)
		return
	}
	out.buffer = bufio.NewWriter(out.file)
	out.writer = out.buffer
	return
}
func  (out *outWriterType) IsOpen() bool {
	return out.file != nil
}


func (out *outWriterType) Close() (err error) {
	if out.buffer != nil {
		err = out.buffer.Flush()
		if err != nil {
			err = fmt.Errorf("could not flush data to temp file %v: %v", out.fileName, err)
			return
		}
	}
	err = out.file.Close()
	if err!= nil {
		err = fmt.Errorf("could not close temp file %v: %v",out.fileName,err)
		return
	}
	out.file = nil
	out.buffer = nil
	out.writer = nil
	return
}


func (splitter CategorySplitterType) SplitFile(ctx context.Context, pathToFile string, categoryColumnListInterface dto.ColumnListInterface) (err error){
	var targetTable *dto.TableInfoType

	counter := make(map[string]uint64)



	if categoryColumnListInterface == nil {
		err = fmt.Errorf("parameter 'categoryColumnList' is not defined")
		return err
	}

	categoryColumns := categoryColumnListInterface.ColumnList();
	if len(categoryColumns) == 0 {
		err = fmt.Errorf("parameter 'categoryColumnList' is empty")
		return err
	}


	targetTable = categoryColumns[0].TableInfo

	targetTableColumns:= targetTable.ColumnList()
	targetTableColumnCount := len(targetTableColumns)

	categoryPositions, err := targetTable.ColumnPositionFlags(categoryColumns,dto.ColumnPositionOn)

	dumperConfig := splitter.config.DumpReaderConfig

	lastCategoryRowData := make(categoryColumnDataType,len(categoryColumns))
	var currentOutWriter *outWriterType;

	categoryRowData := make(categoryColumnDataType,len(categoryColumns))

	processRowContent := func(
		ctx context.Context,
		lineNumber,
		DataPosition uint64,
		rowFields [][]byte,
		original []byte,
	) (err error) {
		categoryNum := 0
		if len(rowFields) != targetTableColumnCount  {
			err = fmt.Errorf("Column count mismach given: %v, expected %v",len(rowFields),targetTableColumnCount)
			return
		}


		for columnNumber  := range targetTable.Columns {
			if categoryPositions[columnNumber] {
				categoryRowData[categoryNum] = rowFields[columnNumber]
				categoryNum++
			}
		}

		if lastCategoryRowData.Equal(categoryRowData) {
			_, err = currentOutWriter.writer.Write(original)
			if err != nil {

			}
		} else {
			if currentOutWriter != nil {
				err = currentOutWriter.Close()
				//TODO: err!
			}

			var index uint64
			key := categoryRowData.String()
			if index, found := counter[key]; !found {
				index = uint64(len(counter)+1)
				counter[key] = index
			}

			currentOutWriter, err = newOutWriter(path.Join(splitter.config.PathToSliceDirectory,strconv.FormatUint(index,36)))
			if err != nil {
				err = fmt.Errorf("could not create a temp file #%v: %v",index,err)
				return err
			}

			_, err = currentOutWriter.writer.Write(original)
			lastCategoryRowData.Copy(categoryRowData)
		}
		return
	}
	dumper, err  := dump.NewDumper(dumperConfig)
	if err != nil {
		return
	}

	linesRead, err := dumper.ReadFromFile(
		ctx,
		pathToFile,
		processRowContent,
	)

	_ = linesRead

	if 	currentOutWriter != nil {
		currentOutWriter.Close();
	}

	if err != nil {
//		tracelog.Errorf(err, packageName, funcName, "Error while reading table %v in line #%v ", targetTable, linesRead)
		return
	} else {
		//tracelog.Info(packageName, funcName, "Table %v processed. %v lines have been read", targetTable, linesRead)
	}
	return
}

