package misc

import "bytes"

//\n   U+000A line feed or newline
const LineFeedByte = byte('\n')
//\r   U+000D carriage return
const CarriageReturnByte = byte('\r')

func TruncateFromCRLF(line []byte) ([]byte){
	for _,value := range []byte{LineFeedByte,CarriageReturnByte} {
		if line[len(line)-1] == value {
			line = line[:len(line)-1]
		}
	}
	return line
}

func SplitDumpLine(line []byte,sep byte) ([][]byte) {
	return bytes.Split(TruncateFromCRLF(line), []byte{sep})
}



type PositionBitType bool
const (
	PositionOn = true
	PositionOff PositionBitType = false
)

func PositionFlagsAs(flag PositionBitType, totalPositions int, positions ...int) (result []bool) {
	result = make([]bool,totalPositions)
	var positionsSet int = 0
	var defaultFlag = !flag
	for index:=0; index< totalPositions; index++{
		result[index] = bool(defaultFlag)
		for _,position := range positions {
			if index == position {
				result[position] = bool(flag)
				positionsSet ++
				if defaultFlag == PositionOff && positionsSet == len(positions) {
					return
				} else {
					continue
				}
			}
		}
	}
	return
}


func ByteBufferLess(buff1, buff2 []byte) int {
	//return string(buff1)<string(buff2)

	size1 := len(buff1)
	size2 := len(buff2)
	limit := size1
	if limit > len(buff2) {
		limit = len(buff2)
	}
	for index := 0; index < limit; index++ {
		if buff1[index] != buff2[index] {
			return int(buff1[index]) - int(buff2[index])
		}
	}
	return  size1 - size2
}

func Iif( a bool, ifTrue string, ifFalse string) string {
	if a {
		return ifTrue
	} else {
		return ifFalse
	}
}



