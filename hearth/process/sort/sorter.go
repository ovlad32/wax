package sort

import (
	"context"
	"sort"
	"github.com/ovlad32/wax/hearth/process/dump"
	"github.com/ovlad32/wax/hearth/misc"
	"os"
)

type ColumnSorterConfigType struct {
	DumpReaderConfig *dump.DumperConfigType
	PathToSortedSliceDirectory string
}
type ColumnSorterType struct {
	config ColumnSorterConfigType
}

//type sortingColumnAddressType struct {
	//start, finish uint16
//}

//type sortingColumnAddressListType []sortingColumnAddressType;
type sortableLineType struct {
	line []byte
	start []uint16
	finish []uint16
}

type SortByColumnParamInterface interface {
	TableColumnCount() int
	SortingColumnIndexes() []int
	RowCount() uint64
}

func NewColumnSorter(cfg *ColumnSorterConfigType) (result *ColumnSorterType) {
	result = &ColumnSorterType{
		config:*cfg,
	}

	return result
}

func (sorter *ColumnSorterType) SortByColumn(
		ctx context.Context,
		pathToDumpFile string,
		dumpRowCount uint64,
		sortingColumnPositions []bool,
		) (err error) {
	var dumpColumnCount= len(sortingColumnPositions)
	sortingColumnCount := 0
	for _, flag := range sortingColumnPositions {
		if flag {
			sortingColumnCount ++
		}
	}

	/*var addresses []sortingColumnAddressListType = make([]sortingColumnAddressListType,sortingColumnCount);
	for index:=0; index<sortingColumnCount; index++ {
		addresses[index] = make(sortingColumnAddressListType,0,dumpRowCount)
	}*/

	var maxLenIndexes []uint16 = make([]uint16, dumpColumnCount)
	var sortableLines= make([]sortableLineType, 0, dumpRowCount)

	var addressIndex int = 0
	processRowContent := func(
		ctx context.Context,
		lineNumber,
		DataPosition uint64,
		rowFields [][]byte,
		original []byte,
	) (err error) {
		var columnOffset uint16 = 0
		addressIndex = 0
		sortableLine := sortableLineType{
			start:  make([]uint16, 0, sortingColumnCount),
			finish: make([]uint16, 0, sortingColumnCount),
			line:   make([]byte, len(original)),
		}
		copy(sortableLine.line, original)

		for index := range rowFields {
			len16 := uint16(len(rowFields[index]))
			if maxLenIndexes[index] < len16 {
				maxLenIndexes[index] = len16
			}
			if sortingColumnPositions[index] {
				sortableLine.start = append(sortableLine.start, columnOffset)
				sortableLine.finish = append(sortableLine.finish, columnOffset+len16)
				//fmt.Println(string(original[columnOffset:columnOffset+len16]))
			}
			columnOffset += len16 + 1
		}
		sortableLines = append(sortableLines, sortableLine)
		return
	}

	dumperConfig := sorter.config.DumpReaderConfig
	dumper, err := dump.NewDumper(dumperConfig)
	if err != nil {
		return
	}

	linesRead, err := dumper.ReadFromFile(
		ctx,
		pathToDumpFile,
		processRowContent,
	)
	if err != nil {
		return err
	}
	_ = linesRead
	//for _, l := range sortableLines {
	//	fmt.Println(string(l.line[l.start[0]:l.finish[0]]))
	//}
	//
	//return

	//separator := []byte{dumper.Config().ColumnSeparator}
	sort.Slice(sortableLines,
		func(i, j int) bool {
			var result int
			for index := 0; index < sortingColumnCount; index++ {
				startI := sortableLines[i].start[index]
				finishI := sortableLines[i].finish[index]

				startJ := sortableLines[j].start[index]
				finishJ := sortableLines[j].finish[index]

				buffI := sortableLines[i].line[startI:finishI]
				buffJ := sortableLines[j].line[startJ:finishJ]
				result = misc.ByteBufferLess(buffI, buffJ)
				if result != 0 {
					break;
				}
			}
			return result<0
		},
	)

	file, _ := os.Create("./dump")

	for index := range sortableLines {
		_, err = file.Write(sortableLines[index].line)
	}
	file.Close()

	if false {

		spaceLen := uint16(0)
		for _, value := range maxLenIndexes {
			if value > spaceLen {
				spaceLen = value
			}
		}
		separator := dumper.Config().ColumnSeparator
		space := make([]byte, spaceLen+1)
		space[spaceLen] = misc.LineFeedByte
		file, _ := os.Create("./dump_square")

		for index := range sortableLines {
			fields := misc.SplitDumpLine(sortableLines[index].line, separator)
			var written int
			for fieldIndex := range fields {
				written, err = file.Write(fields[fieldIndex])
				if err != nil {
					return
				}
				if fieldIndex == dumpColumnCount-1 {
					file.Write(space[spaceLen-(maxLenIndexes[fieldIndex]-uint16(written)):])
				} else {
					file.Write(space[:maxLenIndexes[fieldIndex]-uint16(written)])
				}
			}
		}
		file.Close()
	} else {

	}
	return
}




