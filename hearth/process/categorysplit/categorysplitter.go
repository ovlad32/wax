package categorysplit

import (
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/process/dump"
	"context"
	"fmt"
	"os"
	"io/ioutil"
	"bytes"
	"github.com/ovlad32/wax/hearth/handling"
	"strings"
	"path"
	"strconv"
	"github.com/ovlad32/wax/hearth/repository"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"runtime"
	"time"
	"github.com/ovlad32/wax/hearth/misc"
)

type CategorySplitConfigType struct {
	DumpReaderConfig *dump.DumperConfigType
	PathToSliceDirectory string
	MaxRowCountPerFile int64
	log handling.Logger
}

const (
	dataSeparatorByte byte = 0x1F;
	initialBufferSize int = 4*1024
)





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
		if !bytes.Equal(c[index],data[index]) {
			return false
		}
	}
	return true
}




func (c rowCategoryData) String() (result string){
	chunks:= make([]string,len(c))
	for index := range(c) {
		chunks[index] = string(c[index])
	}

	return strings.Join(
		chunks,
		string([]byte{dataSeparatorByte}),
		)
}

type bufferedCategorySplitFileType struct{
	config *CategorySplitConfigType
	*dto.CategorySplitFileType
	buffer *bytes.Buffer
	currentRowCount int64
}

func (b *bufferedCategorySplitFileType) WriteDumpLine(line []byte) (flushed bool,err error){
	_, err =  b.buffer.Write(line)
	if err != nil {
		return
	}
	b.currentRowCount++
	if (err != nil && err == bytes.ErrTooLarge) ||
		(b.config.MaxRowCountPerFile > 0 && b.config.MaxRowCountPerFile == b.currentRowCount) {
		err = b.FlushToTempFile()
		if err != nil {
			return
		}
		flushed = true
		b.currentRowCount = 0
		b.buffer.Truncate(0)
	}
	return
}

func (b *bufferedCategorySplitFileType) FlushToAppNode() (err error) {

	return
}

func (b *bufferedCategorySplitFileType) FlushToTempFile() (err error) {

	tableDirectory := strconv.FormatInt(b.CategorySplitRowData.CategorySplit.Table.Id.Value(),10)
	splitSessionDirectory := strconv.FormatInt(b.CategorySplitRowData.CategorySplit.Id.Value(),10)
	categoryDataDirectory := strconv.FormatInt(b.CategorySplitRowData.Id.Value(),10)

	targetDirectory := path.Join(
		b.config.PathToSliceDirectory,
		tableDirectory,
		splitSessionDirectory,
		categoryDataDirectory,
	)

	err = os.MkdirAll(targetDirectory,0711)
	if err!= nil {
		err = fmt.Errorf("could not create directory %v: %v",targetDirectory,err)
		return
	}
	tempFile, err := ioutil.TempFile(targetDirectory,"")
	if err != nil {
		err = fmt.Errorf("could not create a temp file at %v: %v",targetDirectory,err)
		return
	}

	_, err = b.buffer.WriteTo(tempFile)
	if err != nil {
		err = fmt.Errorf(
			"could not flush category data slice of table %v to temp file %v: %v",
			b.CategorySplitRowData.CategorySplit.Table,
			tempFile.Name(),
			err,
		)
		return
	}

	b.RowCount = nullable.NewNullInt64(b.currentRowCount);
	b.PathToFile = tempFile.Name()
	err = tempFile.Close()
	if err != nil {
		err = fmt.Errorf("could not close temp file %v: %v",b.PathToFile,err)
	}
	return
}




func (splitter CategorySplitterType) SplitFile(ctx context.Context, pathToFile string, categoryColumnListInterface dto.ColumnListInterface) (err error){
	var targetTable *dto.TableInfoType
	counter := make(map[string]*bufferedCategorySplitFileType)


	if categoryColumnListInterface == nil {
		err = fmt.Errorf("parameter 'categoryColumnList' is not defined")
		return err
	}

	splitColumns := categoryColumnListInterface.ColumnList();
	if len(splitColumns ) == 0 {
		err = fmt.Errorf("parameter 'categoryColumnList' is empty")
		return err
	}

	targetTable = splitColumns [0].TableInfo

	categorySplit :=&dto.CategorySplitType{
		Table: targetTable,
		CategorySplitColumns: make(dto.CategorySplitColumnListType,0,len(splitColumns)),
		Status:"n",
	}

	repository.PutCategorySplit(ctx,categorySplit)

	slitColumnMap := make(map[*dto.ColumnInfoType]*dto.CategorySplitColumnType);

	tableColumns := targetTable.ColumnList()

	for position,column := range splitColumns {
		splitColumn := &dto.CategorySplitColumnType{
			Column:        column,
			Position:      position,
			CategorySplit: categorySplit,
		}
		slitColumnMap[column] = splitColumn
		categorySplit.CategorySplitColumns = append(categorySplit.CategorySplitColumns, splitColumn)
		err = repository.PutCategorySplitColumn(ctx,splitColumn)
		if err != nil {
			return err
		}
	}



	tableColumnCount := len(tableColumns)

	splitPositions, err := splitColumns.ColumnPositionFlagsAs(misc.PositionOn)

	dumperConfig := splitter.config.DumpReaderConfig

	lastRowCategoryData := newRowCategoryData(len(splitColumns))
	currentRowCategoryData := newRowCategoryData(len(splitColumns))

	timer := time.NewTicker(time.Second)

	var currentBufferedRowData *bufferedCategorySplitFileType
	processRowContent := func(
		ctx context.Context,
		config *dump.DumperConfigType,
		lineNumber,
		DataPosition uint64,
		rowFields [][]byte,
		original []byte,
	) (err error) {
		select {
		case _ = <- timer.C:
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			fmt.Println(stats)
			fmt.Println(stats.TotalAlloc,stats.TotalAlloc - stats.HeapAlloc,stats.HeapIdle,stats.HeapSys )
		default:
		}
		categoryColumnCount := 0
		if len(rowFields) != tableColumnCount  {
			err = fmt.Errorf("column count mismach given: %v, expected %v",len(rowFields),tableColumnCount)
			return
		}
		for columnNumber  := range targetTable.Columns {
			if splitPositions[columnNumber] {
				currentRowCategoryData[categoryColumnCount] = rowFields[columnNumber]
				categoryColumnCount++
			}
		}

		if lastRowCategoryData != nil && lastRowCategoryData.Equal(currentRowCategoryData) {
			var flushed bool
			flushed,err = currentBufferedRowData.WriteDumpLine(original)
			if err != nil {
				err = fmt.Errorf("could not write %v line to a slice: %v",
					targetTable,
					err,
				)
				return err
			}
			if flushed {
				currentBufferedRowData.CategorySplitFileType.Id = nullable.NullInt64{}
				err = repository.PutCategorySplitFile(ctx,currentBufferedRowData.CategorySplitFileType)
				if err != nil {
					return err
				}
			}
		} else {
			key := currentRowCategoryData.String()
			var found bool
			if currentBufferedRowData, found = counter[key]; !found {
				rowData := &dto.CategorySplitRowDataType{
					CategorySplit:categorySplit,
					CategorySplitId:categorySplit.Id,
					Data:key,
				}
				err = repository.PutCategorySplitRowDataType(ctx,rowData)
				if err!=nil {
					err =fmt.Errorf("could not persist CategorySplitRowDataType:%v",err)
					return err
				}
				currentBufferedRowData = &bufferedCategorySplitFileType{
					CategorySplitFileType: &dto.CategorySplitFileType{
						CategorySplitRowData:   rowData,
						CategorySplitRowDataId: rowData.Id,
					},
					buffer: bytes.NewBuffer(make([]byte, 0, initialBufferSize)),
					config: &splitter.config,
				}
				counter[key] = currentBufferedRowData
			}
			var flushed bool
			flushed, err = currentBufferedRowData.WriteDumpLine(original)
			if flushed {
				currentBufferedRowData.CategorySplitFileType.Id = nullable.NullInt64{}
				err = repository.PutCategorySplitFile(ctx,currentBufferedRowData.CategorySplitFileType)
				if err != nil {
					return err
				}
			}

			if err != nil {
				err = fmt.Errorf("could not write %v line to a slice: %v",
					targetTable,
					err,
				)
				return err
			}

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
	for _, currentBufferedRowData = range counter {
		err = currentBufferedRowData.FlushToTempFile()
		if err != nil {
			//TODO: err!
			return err
		}
		currentBufferedRowData.CategorySplitFileType.Id = nullable.NullInt64{}
		err = repository.PutCategorySplitFile(ctx,currentBufferedRowData.CategorySplitFileType)
		if err != nil {
			//TODO: err!
			return err
		}
	}

	if err != nil {
//		tracelog.Errorf(err, packageName, funcName, "Error while reading table %v in line #%v ", targetTable, linesRead)
		return
	} else {
		//tracelog.Info(packageName, funcName, "Table %v processed. %v lines have been read", targetTable, linesRead)
	}
	return
}

