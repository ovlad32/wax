package repository

import (
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"fmt"
)

func CategorySplitSeqId() (id int64, err error) {
	err = iDb.QueryRow("select nextval('SPLIT_COLUMN_DATA_SEQ')").Scan(&id)
	if err != nil {
		err = fmt.Errorf("could not get a sequential number from SPLIT_COLUMN_DATA_SEQ: %v", err)
	}
	return
}

func PutCategorySplit(entity *dto.CategorySplitType) (err error){
	if entity == nil {
		err = fmt.Errorf("categorySplit reference is not initialized")
		return
	}
	if entity.Table != nil {
		if !entity.Table.Id.Valid() {
			err = fmt.Errorf("categorySplit's Table.Id is not initialized")
			return
		}
		entity.TableInfoId = entity.Table.Id
	} else {
		if !entity.TableInfoId.Valid() {
			err = fmt.Errorf("categorySplit's TableInfoId is not initialized")
			return
		}
	}

	var newOne bool
	if !entity.Id.Valid() {
		id, err := CategorySplitSeqId()
		if err != nil {
			return err
		}
		entity.Id = nullable.NewNullInt64(id)
		newOne = true
	}

	var dml string
	if  newOne {
		switch currentDbType {
		case H2:
			dml = "insert into category_split(id,table_info_id,status) values(%v,%v,'%v')"
			dml = fmt.Sprintf(dml,entity.Id,entity.TableInfoId,entity.Status)
			_,err = iDb.Exec(dml)
		default:
			dml = "insert into category_split(id,table_info_id,status) values(?,?,?)"
			_,err = iDb.Exec(dml,entity.Id,entity.TableInfoId,entity.Status)
		}
		if err != nil {
			err = fmt.Errorf("could not add a new category_split row: %v",err)
			return
		}
	} else {
		switch currentDbType {
		case H2:
			dml = "update category_split set built=%v, status='%v' where id = %v"
			dml = fmt.Sprintf(dml,entity.Built,entity.Status,entity.Id)
			_,err = iDb.Exec(dml)
		default:
			dml = "update category_split set built=%v, status=? where id = ?"
			_,err = iDb.Exec(dml,entity.Built,entity.Status,entity.Id)
		}
		if err != nil {
			err = fmt.Errorf("could not update category_split row where id=%v: %v",entity.Id.Value(), err)
			return
		}
	}
	return
}





func PutCategorySplitColumn(entity *dto.CategorySplitColumnType) (err error) {
	if entity == nil {
		err = fmt.Errorf("categorySplitColumn reference is not initialized")
		return
	}

	if entity.CategorySplit != nil {
		if !entity.CategorySplit.Id.Valid() {
			err = fmt.Errorf("categorySplitColumn's CategorySplit.Id is not initialized is not initialized")
			return
		}
		entity.CategorySplitId = entity.CategorySplit.Id
	} else {
		if !entity.CategorySplitId.Valid() {
			err = fmt.Errorf("categorySplitColumn's CategorySplitId is not initialized")
			return
		}
	}

	if entity.Column != nil {
		if !entity.Column.Id.Valid() {
			err = fmt.Errorf("categorySplitColumn's Column.Id is not initialized is not initialized")
			return
		}
		entity.ColumnInfoId = entity.Column.Id
	} else {
		if !entity.ColumnInfoId.Valid() {
			err = fmt.Errorf("categorySplitColumn's ColumnInfoId is not initialized")
			return
		}
	}

	var newOne bool
	if !entity.Id.Valid() {
		id,err := CategorySplitSeqId()
		if err != nil {
			return err
		}
		entity.Id = nullable.NewNullInt64(id)
		newOne = true
	}

	var dml string
	if  newOne {
		switch currentDbType {
		case H2:
			dml = "insert into category_split_column(id,category_split_id,column_info_id) values(%v,%v,%v)"
			dml = fmt.Sprintf(dml,entity.Id,entity.CategorySplitId,entity.ColumnInfoId)
			_,err = iDb.Exec(dml)
		default:
			dml = "insert into category_split_column(id, category_split_id, column_info_id) values(?,?,?)"
			_,err = iDb.Exec(dml,entity.Id,entity.CategorySplitId,entity.ColumnInfoId)
		}
		if err != nil {
			err = fmt.Errorf("could not add a new category_split_column row: %v",err)
			return
		}
	}
	return
}




func PutCategorySplitColumnDataType(entity *dto.CategorySplitColumnDataType) (err error) {
	if entity == nil {
		err = fmt.Errorf("categorySplitColumnData reference is not initialized")
		return
	}

	if len(entity.Data) == 0 {
		err = fmt.Errorf("categorySplitColumnData's Data is empty")
		return
	}

	if entity.CategorySplitColumn != nil {
		if !entity.CategorySplitColumn.Id.Valid() {
			err = fmt.Errorf("categorySplitColumnData's CategorySplitColumn.Id is not initialized is not initialized")
			return
		}
		entity.CategorySplitColumnId= entity.CategorySplitColumn.Id
	} else {
		if !entity.CategorySplitColumnId.Valid() {
			err = fmt.Errorf("categorySplitColumnData's CategorySplitColumnId is not initialized")
			return
		}
	}



	var newOne bool
	if !entity.Id.Valid() {
		id,err := CategorySplitSeqId()
		if err != nil {
			return err
		}
		entity.Id = nullable.NewNullInt64(id)
		newOne = true
	}

	var dml string
	if  newOne {
		switch currentDbType {
		case H2:
			dml = "insert into category_split_coldata(id,category_split_column_id, data) values(%v,%v,'%v')"
			dml = fmt.Sprintf(dml,entity.Id,entity.CategorySplitColumnId,entity.Data)
			_,err = iDb.Exec(dml)
		default:
			dml = "insert into category_split_coldata(id, category_split_column_id, data) values(?,?,?)"
			_,err = iDb.Exec(dml,entity.Id,entity.CategorySplitColumnId,entity.Data)
		}
		if err != nil {
			err = fmt.Errorf("could not add a new category_split_coldata row: %v",err)
			return
		}
	}
	return
}




func PutCategorySplitRowDataType(entity *dto.CategorySplitRowDataType) (err error) {
	if entity == nil {
		err = fmt.Errorf("categorySplitRowData reference is not initialized")
		return
	}


	if len(entity.Data) == 0 {
		err = fmt.Errorf("categorySplitRowData's Data is empty")
		return
	}

	if entity.CategorySplit != nil {
		if !entity.CategorySplit.Id.Valid() {
			err = fmt.Errorf("categorySplitRowData's CategorySplit.Id is not initialized is not initialized")
			return
		}
		entity.CategorySplitId = entity.CategorySplit.Id
	} else {
		if !entity.CategorySplitId.Valid() {
			err = fmt.Errorf("categorySplitRowData's CategorySplitId is not initialized")
			return
		}
	}



	var newOne bool
	if !entity.Id.Valid() {
		id,err := CategorySplitSeqId()
		if err != nil {
			return err
		}
		entity.Id = nullable.NewNullInt64(id)
		newOne = true
	}

	var dml string
	if  newOne {
		switch currentDbType {
		case H2:
			dml = "insert into category_split_rowdata(id,category_split_id, data) values(%v,%v,'%v')"
			dml = fmt.Sprintf(dml,entity.Id,entity.CategorySplitId,entity.Data)
			_,err = iDb.Exec(dml)
		default:
			dml = "insert into category_split_rowdata(id, category_split_id, data) values(?,?,?)"
			_,err = iDb.Exec(dml,entity.Id,entity.CategorySplitId,entity.Data)
		}
		if err != nil {
			err = fmt.Errorf("could not add a new category_split_rowdata row: %v",err)
			return
		}
	}
	return
}


func PutCategorySplitFile(entity *dto.CategorySplitFileType) (err error) {
	if entity == nil {
		err = fmt.Errorf("categorySplitFile reference is not initialized")
		return
	}


	if len(entity.PathToFile) == 0 {
		err = fmt.Errorf("categorySplitFile's PathToFile is empty")
		return
	}

	if entity.CategorySplitRowData!= nil {
		if !entity.CategorySplitRowData.Id.Valid() {
			err = fmt.Errorf("categorySplitFile's CategorySplitRowData.Id is not initialized is not initialized")
			return
		}
		entity.CategorySplitRowDataId = entity.CategorySplitRowData.Id
	} else {
		if !entity.CategorySplitRowDataId.Valid() {
			err = fmt.Errorf("categorySplitFile's CategorySplitRowDataId is not initialized")
			return
		}
	}



	var newOne bool
	if !entity.Id.Valid() {
		id,err := CategorySplitSeqId()
		if err != nil {
			return err
		}
		entity.Id = nullable.NewNullInt64(id)
		newOne = true
	}

	var dml string
	if  newOne {
		switch currentDbType {
		case H2:
			dml = "insert into category_split_file(id,category_split_tbldata_id, path_to_file,row_count) values(%v,%v,'%v',%v)"
			dml = fmt.Sprintf(dml,entity.Id,entity.CategorySplitRowDataId,entity.PathToFile,entity.RowCount)
			_,err = iDb.Exec(dml)
		default:
			dml = "insert into category_split_file(id, category_split_tbldata_id, path_to_file,row_count) values(?,?,?,?)"
			_,err = iDb.Exec(dml,entity.Id,entity.CategorySplitRowDataId,entity.PathToFile,entity.RowCount)
		}
		if err != nil {
			err = fmt.Errorf("could not add a new category_split_file row: %v",err)
			return
		}
	} else {
		panic("not implemented yet!")
	}
	return
}

