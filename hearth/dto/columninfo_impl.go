package dto

import (
	"fmt"
	"github.com/goinggo/tracelog"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"github.com/ovlad32/wax/hearth/misc"
	"io/ioutil"
	"path/filepath"
	"strconv"
)

func (ci *ColumnInfoType) FindContentFeatureByStringKey(key string, initFunc func() *ContentFeatureType,
) (result *ContentFeatureType) {
	if ci.ContentFeatures == nil {
		ci.ContentFeatures = make(ContentFeatureMapType)
	}
	if value, found := ci.ContentFeatures[key]; !found {
		if initFunc != nil {
			result = initFunc()
		}
		ci.ContentFeatures[key] = result
	} else {
		result = value
	}
	return result
}

func (col *ColumnInfoType) AggregateDataCategoryStatistics() (err error) {
	funcName := "ColumnInfoType.AggregateDataCategoryStatistics"
	var hashUniqueCount, totalCount int64 = 0, 0
	for _, category := range col.ContentFeatures {
		if !category.HashUniqueCount.Valid() {
			err = fmt.Errorf("HashUniqueCount statistics is empty in %v", category)
			tracelog.Error(err, packageName, funcName)
			return err
		}
		if !category.TotalCount.Valid() {
			err = fmt.Errorf("NonNullCount statistics is empty in %v", category)
			tracelog.Error(err, packageName, funcName)
			return err
		}
		hashUniqueCount += category.HashUniqueCount.Value()
		totalCount += category.TotalCount.Value()
	}

	col.HashUniqueCount = nullable.NewNullInt64(int64(hashUniqueCount))
	col.NonNullCount = nullable.NewNullInt64(int64(totalCount))
	return nil
}

func (ca ColumnInfoListType) ColumnIdString() (result string) {
	result = ""
	for index, col := range ca {
		if index == 0 {
			result = strconv.FormatInt(int64(col.Id.Value()), 10)
		} else {
		}
		result = result + "-" + strconv.FormatInt(int64(col.Id.Value()), 10)
	}
	return result
}
func (ca ColumnInfoListType) Map() (result map[*ColumnInfoType]bool) {
	result = make(map[*ColumnInfoType]bool)
	for _, column := range ca {
		result[column] = true
	}
	return
}
func (ca ColumnInfoListType) isSubsetOf(another ColumnInfoListType) bool {
	if len(ca) == 0 {
		return false
	}
	if len(ca) > len(another) {
		return false
	}

ext:
	for _, curColumn := range ca {
		for _, theirColumn := range another {
			if curColumn.Id.Value() == theirColumn.Id.Value() {
				continue ext
			}
		}
		//Our column has not been found in their set
		return false
	}
	return true
}

func (c ColumnInfoType) IndexFileExists(baseDir string) (result bool, err error) {
	funcName := "ColumnInfoType.IndexFileExists"
	tracelog.Started(packageName, funcName)
	fileMask := filepath.Join(baseDir, fmt.Sprintf("%v.*.bitset", c.Id.Value()))

	files, err := ioutil.ReadDir(fileMask)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Cannot list files with mask %v", fileMask)
		return false, err
	}

	for _, f := range files {
		if !f.IsDir() {
			return true, nil
		}
	}
	tracelog.Completed(packageName, funcName)
	return false, nil
}

func (c ColumnInfoType) String() (result string) {
	return c.ColumnName.Value()
}

func (c ColumnInfoType) GoString() (result string) {
	return fmt.Sprintf("ColumnInfo[id:%v,name:%v]", c.Id.Value(), c.ColumnName.Value())
}

func (c ColumnInfoType) TableInfoReference() *TableInfoType {
	return c.TableInfo
}

func (c ColumnInfoListType) ColumnList() ColumnInfoListType {
	return c
}
func (c ColumnInfoListType) TableInfoReference() *TableInfoType {
	if c == nil || len(c) == 0 {
		return nil
	}
	return c[0].TableInfo
}

func (c ColumnInfoListType) ColumnPositionFlagsAs(flag misc.PositionBitType) (result []bool, err error) {
	if c == nil {
		err = fmt.Errorf("column list is not initialized")
		return
	}
	if len(c) == 0 {
		err = fmt.Errorf("column list is empty")
		return
	}
	var table *TableInfoType
	for index := 0; index < len(c); index++ {
		if table == nil {
			table = c[index].TableInfoReference()
			if table == nil {
				err = fmt.Errorf("reference to parent table is not initialized")
				return
			}
		} else {
			if c[index].TableInfoReference() == nil {
				err = fmt.Errorf("reference to parent table is not initialized")
				return
			} else if c[index].TableInfoReference() != table {
				err = fmt.Errorf("column %v has not been found in table %v", c[index], table)
				return nil, err
			}
		}
	}
	positions := make([]int, 0, len(c))
	tableColumnList := table.ColumnList()
outer:
	for index := 0; index < len(tableColumnList); index++ {
		for _, column := range c {
			if tableColumnList[index].Id.Value() == column.Id.Value() {
				positions = append(positions, index)
			}
			if len(positions) == cap(positions) {
				break outer
			}
		}
	}

	if len(positions) == 0 {
		err = fmt.Errorf("column position result is empty")
		return
	}

	return misc.PositionFlagsAs(flag, len(tableColumnList), positions...), nil
}

func (c *ColumnInfoType) ResetBitset(contentType BitsetContentType) {
	if c.ContentFeatures != nil {
		for _, v := range c.ContentFeatures {
			v.ResetBitset(contentType)
		}
	}
}

func (c *ColumnInfoType) IsNumericDataType() bool {
	realType := c.RealDataType.Value()
	result :=
		realType == "java.lang.Byte" ||
			realType == "java.lang.Short" ||
			realType == "java.lang.Integer" ||
			realType == "java.lang.Long" ||
			realType == "java.lang.Double" ||
			realType == "java.lang.Float" ||
			realType == "java.math.BigDecimal" ||
			realType == "java.math.BigInteger"
	return result
}

/*
type tableBinaryType struct {
	*bufio.Writer
	dFile         *os.File
	dFullFileName string
}

func (t *tableBinaryType) Close() (err error) {
	funcName := "tableBinaryType.Close"

	if t == nil {
		return nil
	}

	err = t.Flush()
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Flushing data to %v ", t.dFullFileName)
		return err
	}

	if t.dFile != nil {
		err = t.dFile.Close()
		if err != nil {
			tracelog.Errorf(err, packageName, funcName, "Closing file %v ", t.dFullFileName)
			return err
		}
	}
	return nil
}

*/
