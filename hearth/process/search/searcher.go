package search

import (
	"github.com/ovlad32/wax/hearth/process/dump"
	"context"
	"bytes"
	"github.com/ovlad32/wax/hearth/misc"
	"fmt"
)

type ColumnSearcherConfigType struct {
	DumpReaderConfig dump.DumperConfigType
}

type ColumnSearcherType struct {
	config ColumnSearcherConfigType
}

//	err = validateCategorySplitConfig(cfg)

func NewColumnSearcher(cfg *ColumnSearcherConfigType) (splitter *ColumnSearcherType,err error) {
	//TODO: fill me
//	err = validateCategorySplitConfig(cfg)
	if err != nil {
		err = fmt.Errorf("coult not create a new category splitter: %v",err)
		return
	}

	splitter = &ColumnSearcherType{
		config:*cfg,
	}

	return

}

func (searcher ColumnSearcherType)Search(
		ctx context.Context,
		pathToDumpFile string,
		returnLeftData bool,
		dominantLeftPosition, dominantRightPosition int,
		rightData[][]byte) (resultMaps []map[int][]int, resultLeftDataBytes [][]byte, err error) {

	resultMaps = make([]map[int][]int,0,100)
	resultLeftDataBytes = make([][]byte,0,100)

	dominantData := rightData[dominantRightPosition]
	resultMapArrayLength := len(rightData)/2

	fmt.Println(string(dominantData))
	fmt.Println("------------------")

	if resultMapArrayLength < 3 {
		resultMapArrayLength = 3
	}
	processRowContent := func(
		ctx context.Context,
		lineNumber,
		DataPosition uint64,
		leftData [][]byte,
		originalData []byte,
	) (err error) {



		result :=  misc.ByteBufferLess(leftData[dominantLeftPosition],dominantData)
		fmt.Println(string(leftData[dominantLeftPosition]))

		if result < 0 {
			return nil
		} else if result > 0 {
			return dump.ErrorAbortedByRowProcessing{}
		}
		for rightIndex := range rightData {
			if len(rightData[rightIndex]) == 0 {
				continue
			}
			for leftIndex := range leftData{
				if len(leftData[leftIndex]) == 0 {
					continue
				}
				valuesMatch := leftIndex == dominantLeftPosition && rightIndex == dominantRightPosition
				if !valuesMatch {
					valuesMatch = bytes.Equal(leftData[leftIndex],rightData[rightIndex])
				}
				if valuesMatch {
					resultMap := make(map[int][]int)

					if leftPositions, found := resultMap[rightIndex]; !found {
						leftPositions = make([]int,0,resultMapArrayLength)
						leftPositions = append(leftPositions,leftIndex)
						resultMap[rightIndex] = leftPositions
					} else {
						leftPositions = append(leftPositions,leftIndex)
						resultMap[rightIndex] = leftPositions
					}
					if returnLeftData {
						originalData = misc.TruncateFromCRLF(originalData);
						originalDataCopy := make([]byte, len(originalData))
						copy(originalDataCopy, originalData)
						resultLeftDataBytes = append(resultLeftDataBytes, originalDataCopy)
					}
					if len(resultMap)>1 {
						resultMaps = append(resultMaps,resultMap)
					}
				}
			}
		}
		return
	}

	dumperConfig := searcher.config.DumpReaderConfig
	dumper, err := dump.NewDumper(&dumperConfig)

	if err != nil {
		return
	}

	_,err = dumper.ReadFromFile(ctx,pathToDumpFile,processRowContent)
	if dump.IsErrorAbortedByRowProcessing(err) {
		err = nil
		return
	} else if err != nil {
		return nil,nil, err
	}




	return
}

