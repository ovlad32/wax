package sort

import (
	"context"
	"sort"
	"github.com/ovlad32/wax/hearth/process/dump"
	"github.com/ovlad32/wax/hearth/misc"
)

type ColumnSorterConfigType struct {
	DumpReaderConfig *dump.DumperConfigType
	PathToSortedSliceDirectory string
}
type ColumnSorterType struct {
	config ColumnSorterConfigType
}

type sortingColumnAddressType struct {
	offset, length uint16
}

type sortingColumnAddressListType []sortingColumnAddressType;
type sortableBufferType [][]byte

type SortByColumnParamInterface interface {
	ColumnCount() int
	SortingColumnIndexes() []int
	RowCount() uint64
}


func (sorter *ColumnSorterType) SortByColumn(ctx context.Context,pathToFile string,params SortByColumnParamInterface ) (err error){
	var dumpColumnCount = params.ColumnCount()
	var dumpRowCount = params.RowCount()

	var sortingIndexes []bool  = misc.PositionFlagsAs(true,dumpColumnCount, params.SortingColumnIndexes()...)
	var sortingColumnCount int = len(sortingIndexes)


	var addresses []sortingColumnAddressListType = make([]sortingColumnAddressListType,sortingColumnCount);
	for index:=0; index<sortingColumnCount; index++ {
		addresses[index] = make(sortingColumnAddressListType,0,dumpRowCount)
	}

	var maxLenIndexes []uint16 = make([]uint16,dumpColumnCount)
	var sortableBuffer = make(sortableBufferType ,0,dumpRowCount)

	var addressIndex int = 0
	processRowContent := func(
		ctx context.Context,
		lineNumber,
		DataPosition uint64,
		rowFields [][]byte,
		original []byte,
	) (err error) {
		var columnOffset uint16= 0
		for index := range rowFields {
			len16 := uint16(len(rowFields))
			if maxLenIndexes[index] < len16 {
				maxLenIndexes[index] = len16
			}
			columnOffset  += len16 + 1
			if !sortingIndexes[index] {
				continue
			}
			addressIndex++
			addresses[addressIndex] = append(addresses[addressIndex],
				sortingColumnAddressType{
					offset: columnOffset,
					length: uint16(len(rowFields[index])),
				})

		}
		sortableBuffer = append(sortableBuffer,original)
		return
	}

	for index:=0; index < sortingColumnCount; index++ {
		sort.Slice(sortableBuffer,
			func(i, j int) bool {
				startI := addresses[index][i].offset
				finishI := startI + addresses[index][i].length

				startJ := addresses[index][i].offset
				finishJ := startI + addresses[index][i].length

				buffI := sortableBuffer[i][startI:finishI]
				buffJ := sortableBuffer[j][startJ:finishJ]
				return misc.ByteBufferLess(buffI,buffJ)
			})
	}

	dumperConfig := sorter.config.DumpReaderConfig
	dumper, err  := dump.NewDumper(dumperConfig)
	if err != nil {
		return
	}

	linesRead, err := dumper.ReadFromFile(
		ctx,
		pathToFile,
		processRowContent,
	)

	return
}



