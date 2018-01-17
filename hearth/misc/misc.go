package misc

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


func ByteBufferLess(buff1, buff2 []byte) bool {
	if len(buff1) < len(buff2) {
		return false
	}
	for index := range buff1 {
		if buff1[index] < buff2[index] {
			return false
		}
	}
	return true
}
