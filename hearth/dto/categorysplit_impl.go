package dto

import "bytes"




func NewCategorySplitRowData(categoryData [][]byte) (result CategorySplitColumnDataListType){
	result = make(CategorySplitColumnDataListType,len(categoryData))
	for index := range categoryData {
		result[index].Data = make([]byte,len(categoryData[index]))
		copy(result[index].Data,categoryData[index])
	}
	return
}

func (c CategorySplitColumnDataListType) Equal(data [][]byte) bool{

	if len(data) != len(c) {
		return false
	}

	for index := range c {
		if len(c[index].Data) != len(data[index]){
			return false
		}
		for position := range c[index].Data{
			if c[index].Data[position] != data[index][position] {
				return false
			}
		}
	}
	return true
}




func (c CategorySplitColumnDataListType) String() (result string){
	chunks:= make([][]byte,len(c))
	for index := range(c) {
		chunks[index]  = c[index].Data
	}
	return string(bytes.Join(chunks,[]byte{0x1F}))
}

