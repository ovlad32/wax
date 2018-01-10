package process

import (
	"context"
	"fmt"
	"github.com/goinggo/tracelog"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/ovlad32/wax/hearth/repository"
	"time"
)

type Indexer interface {
	BuildBitsetsForColumns(
		ctx context.Context,
		pathToDumpDirectory string,
		tableDumpFileName string,
		bitsetContent dto.BitsetContentArrayType,
		targetTable dto.TableInterface,
		targetColumns dto.ColumnListInterface,
	) (err error)
}

type BitsetIndexConfigType struct {
	DumpReaderConfig handling.DumpReaderConfigType
	BitsetPath       string
}

type BitsetIndexType struct {
	config BitsetIndexConfigType
}

func NewIndexer(config *BitsetIndexConfigType) (instance *BitsetIndexType, err error) {
	instance = new(BitsetIndexType)
	instance.config = *config
	return
}

func (indexer *BitsetIndexType) buildBitsets(
	ctx context.Context,
	pathToDumpDirectory string,
	tableDumpFileName string,
	bitsetContent dto.BitsetContentArrayType,
	targetTableInterface dto.TableInterface,
	targetColumnsInterface []dto.ColumnListInterface,
) (err error) {
	funcName := "BitsetIndexType::buildBitsets"

	if len(bitsetContent) == 0 {
		err = fmt.Errorf("bitset content type has to be specified")
		tracelog.Error(err, packageName, funcName)
		return
	}

	if indexer.config.BitsetPath == "" {
		err = fmt.Errorf("bitset path has to be specified")
		tracelog.Error(err, packageName, funcName)
		return
	}

	if targetTableInterface == nil && (targetColumnsInterface == nil || len(targetColumnsInterface) == 0) {
		err = fmt.Errorf("parameters 'targetTable' and 'targetColumns' are empty")
		tracelog.Error(err, packageName, funcName)
		return err
	}
	targetTable := targetTableInterface.Reference()
	var targetColumns []*dto.ColumnInfoType

	if targetColumnsInterface == nil || len(targetColumnsInterface) == 0 {
		targetColumns = targetTableInterface.ColumnList()
	}

	if targetTable == nil {
		targetTable = targetColumns[0].TableInfo
	}

	for index := range targetColumns {
		if targetColumns[index].TableInfo != targetTable {
			err = fmt.Errorf("indexed column %t is not a part of %t", targetColumns[index], targetTable)
			tracelog.Error(err, packageName, funcName)
			return
		}
	}
	targetTableColumns := targetTable.Columns

	omittingPositions,err := targetTable.ColumnPositionFlags(targetColumns,false)


	currentConfig := indexer.config.DumpReaderConfig
	currentConfig.TableName = targetTable.String()
	currentConfig.TableColumnCount = len(targetTableColumns)
	currentConfig.TableDumpFileName = targetTable.PathToFile.Value()

	if pathToDumpDirectory != "" {
		currentConfig.PathToDumpDirectory = pathToDumpDirectory
	}
	if tableDumpFileName != "" {
		currentConfig.TableDumpFileName = tableDumpFileName
	}

	var started time.Time
	var lineStarted uint64 = 1
	//sql.Open("postgres","")

	started = time.Now()
	processRowContent := func(
		ctx context.Context,
		lineNumber,
		DataPosition uint64,
		rowData [][]byte,
		_[]byte,
	) (result handling.DumpReaderActionType, err error) {

		if time.Since(started).Minutes() >= 1 {
			tracelog.Info(packageName, funcName, "Processing speed %.0f lps", float64(lineNumber-lineStarted)/60.0)
			lineStarted = lineNumber
			started = time.Now()
		}
		for columnNumber, column := range targetTableColumns {
			if omittingPositions[columnNumber] || len(rowData[columnNumber]) == 0 {
				continue
			}

			drop := dto.NewSyrupDrop(column, rowData[columnNumber])
			if drop == nil {
				continue
			}

			drop.LineNumber = lineNumber
			var buildBitset bool

			drop.DiscoverContentFeature(bitsetContent.IsPureContent())

			drop.Hash(bitsetContent.IsHashContent())

			_ = buildBitset

		}
		return handling.DumpReaderActionContinue, nil
	}

	_, linesRead, err := handling.ReadAstraDump(
		ctx,
		&currentConfig,
		processRowContent,
	)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Error while reading table %v in line #%v ", targetTable, linesRead)
		return
	} else {
		tracelog.Info(packageName, funcName, "Table %v processed. %v lines have been read", targetTable, linesRead)
	}

	for _, column := range targetTable.Columns {
		for _, feature := range column.ContentFeatures {
			err = feature.UpdateStatistics(ctx)
			if err != nil {
				tracelog.Errorf(err, packageName, funcName, "Error while update statistics for %v.%v (%v)", targetTable, column.ColumnName, feature.Key)
				return err
			}

			if feature.HashUniqueCount.Valid() && feature.HashUniqueCount.Value() > 0 {
				err = feature.WriteBitsetToDisk(ctx, indexer.config.BitsetPath, dto.HashContent)
				if err != nil {
					tracelog.Errorf(err, packageName, funcName, "Error while writting hash bitset data for %v.%v (%v)", targetTable, column.ColumnName, feature.Key)
					return err
				}
			}
			if feature.ItemUniqueCount.Valid() && feature.ItemUniqueCount.Value() > 0 {
				if feature.IsNumeric {
					if feature.IsInteger {
						err = feature.WriteBitsetToDisk(ctx, indexer.config.BitsetPath, dto.PureContent)
					}
				} else {
					err = feature.WriteBitsetToDisk(ctx, indexer.config.BitsetPath, dto.PureContent)
				}
				if err != nil {
					tracelog.Errorf(err, packageName, funcName, "Error while writing integer bitset data for %v.%v (%v)", targetTable, column.ColumnName, feature.Key)
					return err
				}
			}
		}
	}
	return err
}

func (indexer BitsetIndexType) BuildBitsets(ctx context.Context,
	bitsetContent dto.BitsetContentArrayType,
	targetTable dto.TableInterface) (err error) {
	funcName := "BitsetIndexType.BuildBitsets"
	tracelog.Startedf(packageName, funcName, "for table %v", targetTable)

	if len(bitsetContent) == 0 {
		err = fmt.Errorf("bitset content type has to be specified")
		tracelog.Error(err, packageName, funcName)
		return
	}

	if indexer.config.BitsetPath == "" {
		err = fmt.Errorf("bitset path has to be specified")
		tracelog.Error(err, packageName, funcName)
		return
	}

	err = indexer.buildBitsets(ctx,
		"",
		"",
		bitsetContent,
		targetTable, nil,
	)

	for _, column := range targetTable.ColumnList() {
		for _, feature := range column.ContentFeatures {
			err = repository.PutContentFeature(ctx, feature)
			if err != nil {
				tracelog.Errorf(err, packageName, funcName, "Error while persisting statistics for %v.%v (%v)", targetTable, column.ColumnName, feature.Key)
				return err
			}
		}
	}

	tracelog.Info(packageName, funcName, "Table %v bitsets have been written to disk", targetTable)

	tracelog.Completedf(packageName, funcName, "for table %v", targetTable)
	return err
}

func (indexer *BitsetIndexType) BuildBitsetsForColumns(
	ctx context.Context,
	pathToDumpDirectory string,
	tableDumpFileName string,
	bitsetContent dto.BitsetContentArrayType,
	targetTable dto.TableInterface,
	targetColumns []dto.ColumnListInterface,
) (err error) {
	return indexer.buildBitsets(
		ctx,
		pathToDumpDirectory,
		tableDumpFileName,
		bitsetContent,
		targetTable,
		targetColumns,
	)
}
