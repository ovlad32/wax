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
	"strconv"
	"unsafe"
	"bufio"
	"bytes"
	"log"
	"github.com/ovlad32/wax/hearth/handling/nullable"
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
	type splitColumnDataType struct {
		sourceColumnInfo    *dto.ColumnInfoType
		splitDataBytes      [][]byte
		splitColumnInfoList dto.ColumnInfoListType
	}

	type splitColumnListDataType []*splitColumnDataType;
	var splitColumnMap map[string]map[int]dto.ColumnInfoListType

	//type fusionDataListType []*fusionDataType;

	/*fusKey := func (t *fusionDataListType) (result string) {
		if t == nil || len(*t) == 0 {
			return ""
		}
		a := make([]string,0,len(t))
		for _,v := range t {
			a = append(a,
				strconv.Itoa(v.columnNumber)+":"+strconv.Itoa(len(v.fuse)),
			)
		}
		return strings.Join(a,";")
	}*/




	processRowContent := func(
		ctx context.Context,
		config *dump.DumperConfigType,
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
		var fusionColunmList dto.FusionColumnListType
		var splitColumnListData splitColumnListDataType


		for columnNumber, column := range targetTableColumns {

			if offPostitions[columnNumber] || len(rowFields[columnNumber]) == 0 {
				continue
			}

			if len(config.FusionColumnSeparators) >= columnNumber && config.FusionColumnSeparators[columnNumber] != 0 {

				SplitData := misc.SplitDumpLine(rowFields[columnNumber],config.FusionColumnSeparators[columnNumber])
				if len(SplitData) > 1 {
					if fusionColunmList  == nil {
						fusionColunmList  = make(dto.FusionColumnListType, 0, len(targetTableColumns))
						splitColumnListData = make(splitColumnListDataType, 0, len(targetTableColumns))

					}

					fusionColunmList = append(
						fusionColunmList,
								&dto.FusionColumnType{
									SourceColumnPosition: columnNumber,
									ColumnCount:          len(SplitData),
								},
					)

					splitColumnListData = append (
						splitColumnListData,
						&splitColumnDataType{
							splitDataBytes:        SplitData,
							sourceColumnInfo: column,
						},
					)

					continue
				}
			}

			drop := dto.NewSyrupDrop(column, rowFields[columnNumber])
			if drop == nil {
				continue
			}

			drop.LineNumber = lineNumber

			drop.DiscoverContentFeature(bitsetContent.IsPureContent())

			drop.Hash(bitsetContent.IsHashContent())
		}
		if fusionColunmList != nil {
			fusionColumnGroupKey := fusionColunmList.String()
			if mappedSplitColumns,mappedFound := splitColumnMap[fusionColumnGroupKey];!mappedFound {
				var fusionColumnGroup *dto.FusionColumnGroupType
				fusionColumnGroup , err := repository.FusionColumnGroupByTableAndKey(ctx,targetTable,fusionColumnGroupKey )
				if err!= nil{
					//TODO:
				}
				if fusionColumnGroup  == nil {
					fusionColumnGroup =&dto.FusionColumnGroupType{
						TableInfoId:targetTable.Id,
						GroupKey:fusionColumnGroupKey,
					}
					err = repository.PutFusionColumnGroup(ctx,fusionColumnGroup)
					if err != nil {
						//TODO:
					}
				} else {
					//TODO: !!! continue here !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
					// u need to populate the map
				}

				maxPosition :=  targetTable.MaxColumnPosition()


				mappedSplitColumns = make(map[int]dto.ColumnInfoListType)
				//splitColumnInfoList := make(dto.ColumnInfoListType,0,len(fusionSplitRowData))
				for fusionIndex, scd := range splitColumnListData {
					if scd.splitColumnInfoList  == nil {
						scd.splitColumnInfoList = make(dto.ColumnInfoListType, 0, fusionColunmList[fusionIndex].ColumnCount)
					}
					for splitColumnNumber := 0; splitColumnNumber < fusionColunmList[fusionIndex].ColumnCount; splitColumnNumber ++{
						maxPosition ++

						splitColumn := &dto.ColumnInfoType{
							DataLength:           scd.sourceColumnInfo.DataLength,
							DataType:             scd.sourceColumnInfo.DataType,
							RealDataType:         scd.sourceColumnInfo.RealDataType,
							Nullable:             scd.sourceColumnInfo.Nullable,
							TableInfoId:          scd.sourceColumnInfo.TableInfoId,
							TableInfo:            scd.sourceColumnInfo.TableInfo,
							SourceFusionColumnId: scd.sourceColumnInfo.Id,
							FusionColumnGroupId:  fusionColumnGroup.Id,
							PositionInFusion:     nullable.NewNullInt64(int64(splitColumnNumber)),
							TotalInFusion:        nullable.NewNullInt64(int64(fusionColunmList[fusionIndex].ColumnCount)),
							ColumnName: nullable.NewNullString(
								fmt.Sprintf(
									"%v(%v/%v)",
									scd.sourceColumnInfo.ColumnName.Value(),
									splitColumnNumber, fusionColunmList[fusionIndex].ColumnCount,
								),
							),
							Position: nullable.NewNullInt64(int64(maxPosition)),
						}
						err = repository.PutColumnInfo(ctx, splitColumn)
						if err != nil {
							//TODO:
						}
						scd.splitColumnInfoList = append(scd.splitColumnInfoList, splitColumn)
					}
					targetTable.Columns = append(targetTable.Columns,scd.splitColumnInfoList...)

					mappedSplitColumns[fusionColunmList[fusionIndex].SourceColumnPosition] = scd.splitColumnInfoList
				}
				splitColumnMap[key] = mappedSplitColumns
			} else {
				for splitRowDataIndex, scd := range splitColumnListData {
					found := false;
					position := fusionColunmList[splitRowDataIndex].SourceColumnPosition;
					if scd.splitColumnInfoList,found = mappedSplitColumns[position]; !found {
						//TODO:
					}
				}
			}
			for _,scd :=  range splitColumnListData {
				for columnIndex := range scd.splitColumnInfoList {

					drop := dto.NewSyrupDrop(scd.splitColumnInfoList[columnIndex], scd.splitDataBytes[columnIndex])
					if drop == nil {
						continue
					}

					drop.LineNumber = lineNumber

					drop.DiscoverContentFeature(bitsetContent.IsPureContent())

					drop.Hash(bitsetContent.IsHashContent())
				}

			}

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
