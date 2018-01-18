package index

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/ovlad32/wax/hearth/repository"
	dump "github.com/ovlad32/wax/hearth/process/dump"
	"time"
	"strings"
	"path"
	"github.com/ovlad32/wax/hearth/misc"
)


type BitsetIndexConfigType struct {
	DumperConfig *dump.DumperConfigType
	BitsetPath   string
	Log          handling.Logger
}
func validateBitsetConfig(cfg *BitsetIndexConfigType) (err error) {
	if cfg == nil {
		err = fmt.Errorf("config is not initialized")
		return
	}
	if strings.TrimSpace(cfg.BitsetPath) == "" {
		err = fmt.Errorf("bitset path is not defined")
		return
	}
	return
}

type BitsetIndexerType struct {
	config BitsetIndexConfigType
}

func NewIndexer(config *BitsetIndexConfigType) (instance *BitsetIndexerType, err error) {
	instance = new(BitsetIndexerType)
	instance.config = *config
	return
}

func (indexer *BitsetIndexerType) buildBitsets(
	ctx context.Context,
	pathToFile string,
	bitsetContent dto.BitsetContentArrayType,
	processingColumnListInterface dto.ColumnListInterface,
) (err error) {
	funcName:= "buildBitsets"
	log := indexer.config.Log
	err = validateBitsetConfig(&indexer.config)
	if err != nil {
		return err
	}

	if strings.TrimSpace(pathToFile) == "" {
		err = fmt.Errorf("path to dump file is not defined")
		return
	}


	if len(bitsetContent) == 0 {
		err = fmt.Errorf("bitset content is not defined")
		return
	}

	if processingColumnListInterface == nil {
		err = fmt.Errorf("parameter 'processingColumns' is not defined")
		return err
	}

	processingColumns := processingColumnListInterface.ColumnList();
	if len(processingColumns) == 0 {
		err = fmt.Errorf("parameter 'processingColumns' is empty")
		return err
	}

	targetTable := processingColumns[0].TableInfo
	targetTableColumns:= targetTable.ColumnList()
	targetTableColumnCount := len(targetTableColumns)

	offPostitions,err := processingColumns.ColumnPositionFlagsAs(misc.PositionOff)


	dumperConfig := indexer.config.DumperConfig


	var started time.Time
	var lineStarted uint64 = 1
	//sql.Open("postgres","")

	started = time.Now()
	processRowContent := func(
		ctx context.Context,
		lineNumber,
		DataPosition uint64,
		rowFields [][]byte,
		_[]byte,
	) (err error) {

		if len(rowFields) != targetTableColumnCount  {
			err = fmt.Errorf("Column count mismach given: %v, expected %v",len(rowFields),targetTableColumnCount)
			return
		}
		if log != nil {
			if time.Since(started).Minutes() >= 1 {
				log.Info(packageName, funcName, "Processing speed %.0f lps", float64(lineNumber-lineStarted)/60.0)
				lineStarted = lineNumber
				started = time.Now()
			}
		}
		for columnNumber, column := range targetTableColumns {
			if offPostitions[columnNumber] || len(rowFields[columnNumber]) == 0 {
				continue
			}

			drop := dto.NewSyrupDrop(column, rowFields[columnNumber])
			if drop == nil {
				continue
			}

			drop.LineNumber = lineNumber

			drop.DiscoverContentFeature(bitsetContent.IsPureContent())

			drop.Hash(bitsetContent.IsHashContent())


		}
		return nil
	}

	dumper,err  := dump.NewDumper(
		dumperConfig,
	)

	if err != nil {
		return err
	}


	if log != nil {
		log.Info(packageName, funcName, "Start processing file %v ", pathToFile)
	}

	linesRead, err := dumper.ReadFromFile(
		ctx,
		pathToFile,
		processRowContent,
	)

	if err != nil {
		fmt.Errorf("could not process dump file %v: %v", pathToFile,err)
		return
	} else {
		if log != nil {
			log.Info(packageName, funcName, "File %v of %v lines have been processed", pathToFile, linesRead)
		}
	}

	for _, column := range targetTable.Columns {
		for _, feature := range column.ContentFeatures {
			err = feature.UpdateStatistics(ctx)
			if err != nil {
				err := fmt.Errorf("could not update statistics for %v.%v(key:%v): %v ",targetTable,column,feature.Key,err)
				return err
			}

			if feature.HashUniqueCount.Valid() && feature.HashUniqueCount.Value() > 0 {
				err = feature.WriteBitsetToDisk(ctx, indexer.config.BitsetPath, dto.HashContent)
				if err != nil {
					err := fmt.Errorf("could not write hash bitset data for %v.%v(key:%v): %v ",targetTable,column,feature.Key,err)
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
					err := fmt.Errorf("could not write integer bitset data for %v.%v(key:%v): %v ",targetTable,column,feature.Key,err)
					return err
				}
			}
		}
	}
	return err
}

func (indexer BitsetIndexerType) BuildBitsets(ctx context.Context,
	bitsetContent dto.BitsetContentArrayType,
	pathToDumpDirectory string,
	targetTableInterface dto.ColumnListInterface) (err error) {

	targetTable := targetTableInterface.TableInfoReference()

	err = indexer.buildBitsets(ctx,
		path.Join(pathToDumpDirectory,targetTable.PathToFile.Value()),
		bitsetContent,
		targetTable,
	)

	for _, column := range targetTable.ColumnList() {
		for _, feature := range column.ContentFeatures {
			err = repository.PutContentFeature(ctx, feature)
			if err != nil {
				err := fmt.Errorf("could not persist statistics for %v: %v",targetTable,err)
				return err
			}
		}
	}

	return err
}
