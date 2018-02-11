package categorysplit

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/ovlad32/wax/hearth/misc"
	"github.com/ovlad32/wax/hearth/process/dump"
	"github.com/ovlad32/wax/hearth/repository"
	"runtime"
	"strings"
	"time"
)

type channel interface{}
type batchConsumer interface {
	CreateChannel(int64, int64) (channel, error)
	Transfer(channel channel, data []byte) (err error)
	CloseChannel(c channel) (err error)
}

type ConfigType struct {
	DumpReaderConfig *dump.DumperConfigType
	Log              handling.Logger
}

func validateCategorySplitConfig(cfg *ConfigType) (err error) {
	if cfg == nil {
		err = fmt.Errorf("config is not initialized")
		return
	}
	return
}

type CategorySplitterType struct {
	config ConfigType
}

func NewCategorySplitter(cfg *ConfigType) (splitter *CategorySplitterType, err error) {
	//TODO: fill me
	err = validateCategorySplitConfig(cfg)
	if err != nil {
		err = fmt.Errorf("coult not create a new category splitter: %v", err)
		return
	}

	splitter = &CategorySplitterType{
		config: *cfg,
	}

	return

}

type rowCategoryData [][]byte

func newRowCategoryData(size int) (result rowCategoryData) {
	result = make(rowCategoryData, size)
	return
}

func (c rowCategoryData) Copy(data [][]byte) {
	for index := range data {
		c[index] = make([]byte, len(data[index]))
		copy(c[index], data[index])
	}
	return
}

func (c rowCategoryData) Equal(data [][]byte) bool {

	if len(data) != len(c) {
		return false
	}

	for index := range c {
		if !bytes.Equal(c[index], data[index]) {
			return false
		}
	}
	return true
}

func (c rowCategoryData) String(separator ...byte) (result string) {
	chunks := make([]string, len(c))
	for index := range c {
		chunks[index] = string(c[index])
	}

	if len(separator) == 0 {
		separator = append(separator, 31)
	}
	return strings.Join(
		chunks,
		string(separator),
	)
}

/*
type splitDumpFileType struct{
	//config *CategorySplitConfigType
	outputStream io.WriteCloser
	dumpTableInfo *dto.TableInfoType
	currentRowCount int64
}

func newSplitFileDump(
	sourceTable *dto.TableInfoType,
	splitData *dto.CategorySplitDataType,
	) (result *splitDumpFileType, err error) {

		result = new(splitDumpFileType)
		result.dumpTableInfo = &dto.TableInfoType {
				MetadataId:sourceTable.MetadataId,
				Metadata:sourceTable.Metadata,
				SourceSliceTableInfoId:sourceTable.Id,
				DatabaseName:sourceTable.DatabaseName,
				Indexed:nullable.NewNullString("FALSE"),
				Dumped:nullable.NewNullString("FALSE"),
				SchemaName:sourceTable.SchemaName,
				TableName:nullable.NewNullString(
					fmt.Sprintf(
						"%v(categorySlice:%v)",
						sourceTable.TableName.Value(),
						splitData.Id.Value(),
					),
				),
				CategorySplitDataId:splitData.Id,
				Columns:make(dto.ColumnInfoListType,0,len(sourceTable.Columns)),
		}
		for _, sourceColumn := range sourceTable.ColumnList() {
			result.dumpTableInfo.Columns = append(
				result.dumpTableInfo.Columns,
				&dto.ColumnInfoType{
					TableInfo:result.dumpTableInfo,
					ColumnName:sourceColumn.ColumnName,
					DataType:sourceColumn.DataType,
					DataLength:sourceColumn.DataLength,
					DataPrecision:sourceColumn.DataPrecision,
					DataScale:sourceColumn.DataScale,
					Position:sourceColumn.Position,
					Nullable:sourceColumn.Nullable,
					SourceSliceColumnInfoId:sourceColumn.Id,
					FusionSeparator:sourceColumn.FusionSeparator,
				},
			)
		}
	return
}

func (b *splitDumpFileType) Write(line []byte) (n int,err error){
	n, err =  b.outputStream.Write(line)
	if err != nil {
		n = -1
		return
	}
	b.currentRowCount++
	return
}

func (b *splitDumpFileType) Close() (err error){
	return b.outputStream.Close()
}




func (b *splitDumpFileType) FlushToTempFile() (err error) {
	tableDirectory := strconv.FormatInt(b.CategorySplitData.CategorySplit.Table.Id.Value(),10)
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

	b.dumpTableInfo.RowCount = nullable.NewNullInt64(b.currentRowCount);
	b.PathToFile = tempFile.Name()
	err = tempFile.Close()
	if err != nil {
		err = fmt.Errorf("could not close temp file %v: %v",b.PathToFile,err)
	}
	return
}

*/

func (splitter CategorySplitterType) SplitFile(ctx context.Context,
	pathToFile string,
	categoryColumnListInterface dto.ColumnListInterface,
	consumer batchConsumer,
) (err error) {
	var sourceTable *dto.TableInfoType
	counter := make(map[string]*sliceWriterType)

	closeAll := func() {
		for key, closer := range counter {
			if closer != nil {
				closer.Close()
			}
			delete(counter, key)
		}
	}

	if categoryColumnListInterface == nil {
		err = fmt.Errorf("parameter 'categoryColumnList' is not defined")
		return err
	}

	splitColumns := categoryColumnListInterface.ColumnList()
	if len(splitColumns) == 0 {
		err = fmt.Errorf("parameter 'categoryColumnList' is empty")
		return err
	}

	sourceTable = splitColumns[0].TableInfo

	categorySplit := &dto.CategorySplitType{
		Table:                sourceTable,
		CategorySplitColumns: make(dto.CategorySplitColumnListType, 0, len(splitColumns)),
		Status:               "n",
	}

	repository.PutCategorySplit(ctx, categorySplit)

	slitColumnMap := make(map[*dto.ColumnInfoType]*dto.CategorySplitColumnType)

	tableColumns := sourceTable.ColumnList()

	for position, column := range splitColumns {
		splitColumn := &dto.CategorySplitColumnType{
			Column:        column,
			Position:      position,
			CategorySplit: categorySplit,
		}
		slitColumnMap[column] = splitColumn
		categorySplit.CategorySplitColumns = append(categorySplit.CategorySplitColumns, splitColumn)
		err = repository.PutCategorySplitColumn(ctx, splitColumn)
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

	var currentDumpWriter *sliceWriterType
	processRowContent := func(
		ctx context.Context,
		config *dump.DumperConfigType,
		lineNumber,
		DataPosition uint64,
		rowFields [][]byte,
		original []byte,
	) (err error) {
		select {
		case _ = <-timer.C:
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			//fmt.Println(stats)
			//fmt.Println(stats.TotalAlloc,stats.TotalAlloc - stats.HeapAlloc,stats.HeapIdle,stats.HeapSys )
		default:
		}
		categoryColumnCount := 0
		if len(rowFields) != tableColumnCount {
			err = fmt.Errorf("column count mismach given: %v, expected %v", len(rowFields), tableColumnCount)
			return
		}
		for columnNumber := range sourceTable.Columns {
			if splitPositions[columnNumber] {
				currentRowCategoryData[categoryColumnCount] = rowFields[columnNumber]
				categoryColumnCount++
			}
		}

		if lastRowCategoryData != nil && lastRowCategoryData.Equal(currentRowCategoryData) {
			_, err = currentDumpWriter.Write(original)
			if err != nil {
				err = fmt.Errorf("could not write %v line to a slice: %v",
					sourceTable,
					err,
				)
				return err
			}
		} else {
			key := currentRowCategoryData.String()
			var found bool
			if currentDumpWriter, found = counter[key]; !found {
				rowData := &dto.CategorySplitDataType{
					CategorySplit:   categorySplit,
					CategorySplitId: categorySplit.Id,
					Data:            key,
				}
				err = repository.PutCategorySplitDataType(ctx, rowData)
				if err != nil {
					err = fmt.Errorf("could not persist CategorySplitDataType:%v", err)
					return err
				}

				currentDumpWriter, err = newSliceWriter(
					consumer,
					sourceTable.Id.Value(),
					rowData.Id.Value(),
				)

				/*currentDumpWriter.outputStream, err =
					splitter.config.StreamFactory.OpenStream(
						sourceTable.Id.Value(),
						rowData.Id.Value(),
						)


				err = repository.PutTableInfo(ctx,currentSplitDumpFile.dumpTableInfo)
				if err != nil {
					err =fmt.Errorf("could not create new slice TableInfo :%v",err)
					return err
				}
				*/

				counter[key] = currentDumpWriter
			}
			_, err = currentDumpWriter.Write(original)

			if err != nil {
				err = fmt.Errorf("could not write %v line to a slice: %v",
					sourceTable,
					err,
				)
				return err
			}

			lastRowCategoryData.Copy(currentRowCategoryData)

		}
		return
	}

	dumper, err := dump.NewDumper(dumperConfig)
	if err != nil {
		return
	}

	linesRead, err := dumper.ReadFromFile(
		ctx,
		pathToFile,
		processRowContent,
	)

	_ = linesRead

	closeAll()

	if err != nil {
		//		tracelog.Errorf(err, packageName, funcName, "Error while reading table %v in line #%v ", targetTable, linesRead)
		return
	} else {
		//tracelog.Info(packageName, funcName, "Table %v processed. %v lines have been read", targetTable, linesRead)
	}
	return
}
