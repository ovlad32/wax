package dto

func NewCategorySplitRowData(categoryData [][]byte) (result CategorySplitColumnDataListType) {
	result = make(CategorySplitColumnDataListType, len(categoryData))
	for index := range categoryData {
		result[index].Data = string(categoryData[index])
	}
	return
}

func (c CategorySplitColumnDataListType) Equal(data [][]byte) bool {

	if len(data) != len(c) {
		return false
	}

	for index := range c {
		if len(c[index].Data) != len(data[index]) {
			return false
		}
		for position := range c[index].Data {
			if c[index].Data[position] != data[index][position] {
				return false
			}
		}
	}
	return true
}
