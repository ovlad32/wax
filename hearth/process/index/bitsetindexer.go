package index

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"github.com/ovlad32/wax/hearth/misc"
	dump "github.com/ovlad32/wax/hearth/process/dump"
	"github.com/ovlad32/wax/hearth/repository"
	"github.com/sirupsen/logrus"
	"path"
	"strings"
	"time"
)

type BitsetIndexConfigType struct {
	DumperConfig *dump.DumperConfigType
	BitsetPath   string
	Log          *logrus.Logger
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

	processingColumns := processingColumnListInterface.ColumnList()
	if len(processingColumns) == 0 {
		err = fmt.Errorf("parameter 'processingColumns' is empty")
		return err
	}

	targetTable := processingColumns[0].TableInfo
	targetTableColumns := targetTable.ColumnList()
	targetTableColumnCount := len(targetTableColumns)

	offPositions, err := processingColumns.ColumnPositionFlagsAs(misc.PositionOff)

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
	type mappedSplitColumnType map[int]dto.ColumnInfoListType
	type mappedColumnGroupsType map[string]mappedSplitColumnType

	type splitColumnListDataType []*splitColumnDataType
	var mappedColumnGroups mappedColumnGroupsType

	var prevFusionColumnGroupKey string
	var prevMappedSplitColumns mappedSplitColumnType


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
		_ []byte,
	) (err error) {

		if len(rowFields) != targetTableColumnCount {
			err = fmt.Errorf("Column count mismach given: %v, expected %v", len(rowFields), targetTableColumnCount)
			return
		}
		if log != nil {
			if time.Since(started).Minutes() >= 1 {
				log.Infof("Processing speed %.0f lps", float64(lineNumber-lineStarted)/60.0)
				lineStarted = lineNumber
				started = time.Now()
			}
		}
		var fusionColumnList dto.FusionColumnListType
		var splitColumnListData splitColumnListDataType

		maxColumnPosition := targetTable.MaxColumnPosition()
		var splitColumnsAdded = 0

		for columnNumber, column := range targetTableColumns {

			if offPositions[columnNumber] || len(rowFields[columnNumber]) == 0 {
				continue
			}
			var SplitData [][]byte

			if column.FusionSeparator.Valid() && column.FusionSeparator.Value() != "" {
				fusionColumnSeparator := byte(column.FusionSeparator.Value()[0:1][0])
				SplitData = misc.SplitDumpLine(rowFields[columnNumber], fusionColumnSeparator)
			}

			if len(SplitData) < 2 {
				drop := dto.NewSyrupDrop(column, rowFields[columnNumber])
				if drop == nil {
					continue
				}

				drop.LineNumber = lineNumber

				drop.DiscoverContentFeature(bitsetContent.IsPureContent())

				drop.Hash(bitsetContent.IsHashContent())


			} else {
				if fusionColumnList == nil {
					fusionColumnList = make(dto.FusionColumnListType, 0, len(targetTableColumns))
					splitColumnListData = make(splitColumnListDataType, 0, len(targetTableColumns))
				}

				fusionColumnList = append(
					fusionColumnList,
					&dto.FusionColumnType{
						SourceColumnPosition: columnNumber,
						ColumnCount:          len(SplitData),
					},
				)

				splitColumnListData = append(
					splitColumnListData,
					&splitColumnDataType{
						splitDataBytes:   SplitData,
						sourceColumnInfo: column,
					},
				)

				continue
			}
		}
		if fusionColumnList != nil {
			var mappedSplitColumns mappedSplitColumnType
			var mappedFound bool = false
			if mappedColumnGroups == nil {
				mappedColumnGroups = make(mappedColumnGroupsType)
				groups, err := repository.FusionColumnGroupByTable(ctx, targetTable)
				if err != nil {
					//TODO:
				}
				if groups != nil && len(groups) > 0 {
					for _, fusionColumnGroup := range groups {
						var splitColumns dto.ColumnInfoListType
						splitColumns, err = repository.ColumnsByGroupId(ctx, fusionColumnGroup.Id.Value())
						if err != nil {
							//TODO:
						}
						if len(splitColumns) > 0 {
							splitColumnsAdded = len(splitColumns)
							columnIdMap := make(map[int64]dto.ColumnInfoListType)
							for _, splitColumn := range splitColumns {
								if columnList, found := columnIdMap[splitColumn.SourceFusionColumnInfoId.Value()]; !found {
									columnList = make(dto.ColumnInfoListType, 0, len(splitColumnListData))
									columnList = append(columnList, splitColumn)
									columnIdMap[splitColumn.SourceFusionColumnInfoId.Value()] = columnList
								} else {
									columnList = append(columnList, splitColumn)
									columnIdMap[splitColumn.SourceFusionColumnInfoId.Value()] = columnList
								}
							}
							mappedColumnGroups[fusionColumnGroup.GroupKey] = make(map[int]dto.ColumnInfoListType)
							for columnIndex, tabColumn := range targetTable.ColumnList() {
								if columnList, found := columnIdMap[tabColumn.Id.Value()]; found {
									mappedColumnGroups[fusionColumnGroup.GroupKey][columnIndex] = columnList
								}
							}
						}
					}

				}

			}

			fusionColumnGroupKey := fusionColumnList.String()

			if prevFusionColumnGroupKey == fusionColumnGroupKey {
				mappedSplitColumns = prevMappedSplitColumns
				mappedFound = true
			} else {
				mappedSplitColumns, mappedFound = mappedColumnGroups[fusionColumnGroupKey]
			}

			if !mappedFound {
				fusionColumnGroup := &dto.FusionColumnGroupType{
					TableInfoId: targetTable.Id,
					GroupKey:    fusionColumnGroupKey,
				}
				err = repository.PutFusionColumnGroup(ctx, fusionColumnGroup)
				if err != nil {
					//TODO:
				}

				mappedSplitColumns = make(map[int]dto.ColumnInfoListType)
				//splitColumnInfoList := make(dto.ColumnInfoListType,0,len(fusionSplitRowData))
				for fusionIndex, scd := range splitColumnListData {
					if scd.splitColumnInfoList == nil {
						scd.splitColumnInfoList = make(dto.ColumnInfoListType, 0, fusionColumnList[fusionIndex].ColumnCount)
					}
					for splitColumnNumber := 0; splitColumnNumber < fusionColumnList[fusionIndex].ColumnCount; splitColumnNumber++ {
						splitColumnsAdded++

						splitColumn := &dto.ColumnInfoType{
							DataLength:               scd.sourceColumnInfo.DataLength,
							DataType:                 scd.sourceColumnInfo.DataType,
							RealDataType:             scd.sourceColumnInfo.RealDataType,
							Nullable:                 scd.sourceColumnInfo.Nullable,
							TableInfoId:              scd.sourceColumnInfo.TableInfoId,
							TableInfo:                scd.sourceColumnInfo.TableInfo,
							SourceFusionColumnInfoId: scd.sourceColumnInfo.Id,
							FusionColumnGroupId:      fusionColumnGroup.Id,
							PositionInFusion:         nullable.NewNullInt64(int64(splitColumnNumber)),
							TotalInFusion:            nullable.NewNullInt64(int64(fusionColumnList[fusionIndex].ColumnCount)),
							ColumnName: nullable.NewNullString(
								fmt.Sprintf(
									"%v(%v/%v)",
									scd.sourceColumnInfo.ColumnName.Value(),
									splitColumnNumber, fusionColumnList[fusionIndex].ColumnCount,
								),
							),
							Position: nullable.NewNullInt64(int64(maxColumnPosition + splitColumnsAdded)),
						}
						err = repository.PutColumnInfo(ctx, splitColumn)
						if err != nil {
							//TODO:
						}
						scd.splitColumnInfoList = append(scd.splitColumnInfoList, splitColumn)
					}
					// TODO: Consider if needed
					// targetTable.Columns = append(targetTable.Columns,scd.splitColumnInfoList...)

					mappedSplitColumns[fusionColumnList[fusionIndex].SourceColumnPosition] = scd.splitColumnInfoList
				}
				mappedColumnGroups[fusionColumnGroupKey] = mappedSplitColumns
			} else {
				for splitRowDataIndex, scd := range splitColumnListData {
					found := false
					position := fusionColumnList[splitRowDataIndex].SourceColumnPosition
					if scd.splitColumnInfoList, found = mappedSplitColumns[position]; !found {
						//TODO:
					}
				}
			}

			prevFusionColumnGroupKey = fusionColumnGroupKey
			prevMappedSplitColumns = mappedSplitColumns

			for _, scd := range splitColumnListData {
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

	dumper, err := dump.NewDumper(
		dumperConfig,
	)

	if err != nil {
		return err
	}

	if log != nil {
		log.Infof("Start processing file %v ", pathToFile)
	}

	linesRead, err := dumper.ReadFromFile(
		ctx,
		pathToFile,
		processRowContent,
	)

	if err != nil {
		err = fmt.Errorf("could not process dump file %v:\n%v", pathToFile, err)
		log.Error(err)
		return
	} else {
		if log != nil {
			log.Infof("File %v of %v lines have been processed", pathToFile, linesRead)
		}
	}

	for _, column := range targetTable.Columns {
		for _, feature := range column.ContentFeatures {
			err = feature.UpdateStatistics(ctx)
			if err != nil {
				err := fmt.Errorf("could not update statistics for %v.%v(key:%v): %v ", targetTable, column, feature.Key, err)
				return err
			}

			if feature.HashUniqueCount.Valid() && feature.HashUniqueCount.Value() > 0 {
				err = feature.WriteBitsetToDisk(ctx, indexer.config.BitsetPath, dto.HashContent)
				if err != nil {
					err := fmt.Errorf("could not write hash bitset data for %v.%v(key:%v): %v ", targetTable, column, feature.Key, err)
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
					err := fmt.Errorf("could not write integer bitset data for %v.%v(key:%v): %v ", targetTable, column, feature.Key, err)
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
		path.Join(pathToDumpDirectory, targetTable.PathToFile.Value()),
		bitsetContent,
		targetTable,
	)

	for _, column := range targetTable.ColumnList() {
		for _, feature := range column.ContentFeatures {
			err = repository.PutContentFeature(ctx, feature)
			if err != nil {
				err := fmt.Errorf("could not persist statistics for %v: %v", targetTable, err)
				return err
			}
		}
	}

	return err
}
