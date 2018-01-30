package repository

import (
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"fmt"
	"context"
)

func CategorySplitSeqId() (id int64, err error) {
	err = iDb.QueryRow("select nextval('SPLIT_COLUMN_DATA_SEQ')").Scan(&id)
	if err != nil {
		err = fmt.Errorf("could not get a sequential number from SPLIT_COLUMN_DATA_SEQ: %v", err)
	}
	return
}

func PutCategorySplit(ctx context.Context, entity *dto.CategorySplitType) (err error){
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

	if  newOne {
		data := varray {entity.Id,entity.TableInfoId,entity.Status}
		_, err = ExecContext(ctx,
			`insert into category_split(id,table_info_id,status)`+data.valuePlaceholders(),
			data...,
		)
		if err != nil {
			err = fmt.Errorf("could not add a new category_split row: %v",err)
			entity.Id = nullable.NullInt64{}
			return
		}
	} else {
		_,err =ExecContext(ctx,
			`update category_split set built=?, status=? where id=?`,
				entity.Built,entity.Status,entity.Id,
		)
		if err != nil {
			err = fmt.Errorf("could not update category_split row where id=%v: %v",entity.Id.Value(), err)
			return
		}
	}
	return
}

func PutCategorySplitColumn(ctx context.Context, entity *dto.CategorySplitColumnType) (err error) {
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

	if  newOne {
		_,err = ExecContext(ctx,
			`insert into category_split_column(id,category_split_id,column_info_id) values(?,?,?)`,
				entity.Id,entity.CategorySplitId,entity.ColumnInfoId,
		)
		if err != nil {
			err = fmt.Errorf("could not add a new category_split_column row: %v",err)
			entity.Id = nullable.NullInt64{}
			return
		}
	}
	return
}

func PutCategorySplitColumnDataType(ctx context.Context, entity *dto.CategorySplitColumnDataType) (err error) {
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

	if  newOne {
		data := varray{entity.Id,entity.CategorySplitColumnId,entity.Data}
		_,err = ExecContext(ctx,
			`insert into category_split_coldata(id,category_split_column_id, data)`+data.valuePlaceholders(),
				data...,
		)
		if err != nil {
			err = fmt.Errorf("could not add a new category_split_coldata row: %v",err)
			entity.Id = nullable.NullInt64{}
			return
		}
	}
	return
}

func PutCategorySplitDataType(ctx context.Context, entity *dto.CategorySplitDataType) (err error) {
	if entity == nil {
		err = fmt.Errorf("categorySplitData reference is not initialized")
		return
	}


	if len(entity.Data) == 0 {
		err = fmt.Errorf("categorySplitData's Data is empty")
		return
	}

	if entity.CategorySplit != nil {
		if !entity.CategorySplit.Id.Valid() {
			err = fmt.Errorf("categorySplitData's CategorySplit.Id is not initialized is not initialized")
			return
		}
		entity.CategorySplitId = entity.CategorySplit.Id
	} else {
		if !entity.CategorySplitId.Valid() {
			err = fmt.Errorf("categorySplitData's CategorySplitId is not initialized")
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

	if  newOne {
		data := varray{entity.Id,entity.CategorySplitId,entity.Data}
		_,err = ExecContext(ctx,
			`insert into category_split_data(id,category_split_id, data)`+data.valuePlaceholders(),
			data...,
			)
		if err != nil {
			err = fmt.Errorf("could not add a new category_split_data entity: %v",err)
			entity.Id = nullable.NullInt64{}
			return
		}
	}
	return
}
/*
func PutCategorySplitFile(ctx context.Context, entity *dto.CategorySplitFileType) (err error) {
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

	if  newOne {
		_, err =ExecContext(ctx,
			`insert into category_split_file(id,category_split_tbldata_id, path_to_file,row_count) values(?,?,?,?)`,
			entity.Id,entity.CategorySplitRowDataId,entity.PathToFile,entity.RowCount,
			)
		if err != nil {
			entity.Id = nullable.NullInt64{}
			err = fmt.Errorf("could not add a new category_split_file row: %v",err)
			return
		}
	} else {
		panic("not implemented yet!")
	}
	return
}
*/
