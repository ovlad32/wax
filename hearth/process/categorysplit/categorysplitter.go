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
	"github.com/ovlad32/wax/hearth/repository"
	"github.com/ovlad32/wax/hearth/handling/nullable"
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

func NewCategorySplitter(cfg *CategorySplitConfigType) (splitter *CategorySplitterType,err error) {
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

type rowCategoryData [][]byte

func newRowCategoryData(size int) (result rowCategoryData) {
	result = make(rowCategoryData,size)
	return
}

func (c rowCategoryData) Copy(data [][]byte) {
	for index := range data {
		c[index] = make([]byte,len(data[index]))
		copy(c[index],data[index])
	}
	return
}

func (c rowCategoryData) Equal(data [][]byte) bool{

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
	return true
}




func (c rowCategoryData) String() (result string){
	chunks:= make([][]byte,len(c))
	for index := range(c) {
		chunks[index]  = c[index]
	}
	return string(bytes.Join(chunks,[]byte{0x1F}))
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
	counter := make(map[string]*dto.CategorySplitRowDataType)


	if categoryColumnListInterface == nil {
		err = fmt.Errorf("parameter 'categoryColumnList' is not defined")
		return err
	}

	splitColumns := categoryColumnListInterface.ColumnList();
	if len(splitColumns ) == 0 {
		err = fmt.Errorf("parameter 'categoryColumnList' is empty")
		return err
	}

	categorySplit :=&dto.CategorySplitType{
		Table: splitColumns [0].TableInfo,
		CategorySplitColumns: make(dto.CategorySplitColumnListType,0,len(splitColumns)),
	}

	repository.PutCategorySplit(categorySplit)

	slitColumnMap := make(map[*dto.ColumnInfoType]*dto.CategorySplitColumnType);

	tableColumns := targetTable.ColumnList()

	for position,column := range (splitColumns) {
		splitColumn := &dto.CategorySplitColumnType{
			Column:        column,
			Position:      position,
			CategorySplit: categorySplit,
		}
		slitColumnMap[column] = splitColumn
		categorySplit.CategorySplitColumns = append(categorySplit.CategorySplitColumns, splitColumn)
		err = repository.PutCategorySplitColumn(splitColumn)
		if err != nil {
			return err
		}
	}



	tableColumnCount := len(tableColumns)

	splitPositions, err := targetTable.ColumnPositionFlags(splitColumns,dto.ColumnPositionOn)

	dumperConfig := splitter.config.DumpReaderConfig

	lastRowCategoryData := newRowCategoryData(len(splitColumns))
	currentRowCategoryData := newRowCategoryData(len(splitColumns))

	var currentOutWriter *outWriterType
	var currentLineNumber uint64 = 0
	processRowContent := func(
		ctx context.Context,
		lineNumber,
		DataPosition uint64,
		rowFields [][]byte,
		original []byte,
	) (err error) {
		categoryColumnCount := 0
		if len(rowFields) != tableColumnCount  {
			err = fmt.Errorf("Column count mismach given: %v, expected %v",len(rowFields),tableColumnCount)
			return
		}
		for columnNumber  := range targetTable.Columns {
			if splitPositions[columnNumber] {
				currentRowCategoryData[categoryColumnCount] = rowFields[columnNumber]
				categoryColumnCount++
			}
		}

		if lastRowCategoryData != nil && lastRowCategoryData.Equal(currentRowCategoryData) {
			_, err = currentOutWriter.writer.Write(original)
			if err != nil {
				err = fmt.Errorf("could not write a split file %v for dump %v: %v",
					currentOutWriter.file.Name(),
					pathToFile,
					err,
				)
				return err
			}
			currentLineNumber++
		} else {
			if currentOutWriter != nil {
				err = currentOutWriter.Close()
				//TODO: err!
			}

			key := currentRowCategoryData.String()
			var rowDataId int64
			if rowData, found := counter[key]; !found {
				rowData := &dto.CategorySplitRowDataType{
					CategorySplit:categorySplit,
					Data:key,
				}
				err = repository.PutCategorySplitRowDataType(rowData)
				if err!=nil {
					err =fmt.Errorf("could not persist CategorySplitRowDataType:%v",err)
					return err
				}
				rowDataId = rowData.Id.Value()
				counter[key] = rowData
			} else {
				rowDataId = rowData.Id.Value()
			}

			directoryPrefix := strconv.FormatInt(rowDataId,36)
			outDirectory := path.Join(
				splitter.config.PathToSliceDirectory,
				strconv.FormatInt(categorySplit.Table.Id.Value(),10),
				directoryPrefix,
				)
			currentOutWriter, err = newOutWriter(outDirectory)
			if err != nil {
				err = fmt.Errorf("could not create a temp file #%v for dump %v: %v",
					rowDataId,
					pathToFile,
					err,
				)
				return err
			}
			categorySplitFile := &dto.CategorySplitFileType {
				CategorySplitRowDataId:nullable.NewNullInt64(rowDataId),
				PathToFile:
			}

			directory, file := path.Split(currentOutWriter.file.Name())

			_, err = currentOutWriter.writer.Write(original)
			if err != nil {
				err = fmt.Errorf("could not write a split file %v for dump %v: %v",
					currentOutWriter.file.Name(),
					pathToFile,
					err,
				)
				return err
			}
			currentLineNumber++

			lastRowCategoryData.Copy(currentRowCategoryData)

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

