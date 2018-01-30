package repository

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

func TableInfoSeqId() (id int64, err error) {
	err = iDb.QueryRow("select nextval('TABLE_INFO_SEQ')").Scan(&id)
	if err != nil {
		err = fmt.Errorf("could not get a sequential number from TABLE_INFO_SEQ: %v", err)
	}
	return
}

func tableInfo(ctx context.Context, where string, args... interface{}) (result []*dto.TableInfoType, err error) {

	result = make([]*dto.TableInfoType, 0, 1)
	query := `SELECT 
		 ID
		 ,DATABASE_NAME
		 ,SCHEMA_NAME
		 ,NAME
		 ,ROW_COUNT
		 ,DUMPED
         ,INDEXED
		 ,PATH_TO_FILE
		 ,PATH_TO_DATA_DIR
		 ,METADATA_ID
         ,SOURCE_SLICE_TABLE_INFO_ID
		 FROM TABLE_INFO `

	if where != "" {
		query = query + where
	}
	rows, err := QueryContext(ctx,query,args...)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			break
		default:
			var row dto.TableInfoType
			err = rows.Scan(
				&row.Id,
				&row.DatabaseName,
				&row.SchemaName,
				&row.TableName,
				&row.RowCount,
				&row.Dumped,
				&row.Indexed,
				&row.PathToFile,
				&row.PathToDataDir,
				&row.MetadataId,
				&row.SourceSliceTableInfoId,
			)
			if err != nil {
				err = fmt.Errorf("could not scan table_info entity data: %v", err)
				return
			}
			result = append(result, &row)
		}
	}
	return
}

func PutTableInfo(ctx context.Context, entity *dto.TableInfoType) (err error) {
	var newOne bool

	if entity == nil {
		err = fmt.Errorf("table reference is not initialized")
		return
	}

	if !entity.Id.Valid() {
		var id int64
		id, err = TableInfoSeqId()
		if err != nil {
			return
		}
		entity.Id = nullable.NewNullInt64(id)
		newOne = true
	}

	data := varray{
		entity.Id,
		entity.DatabaseName,
		entity.Dumped,
		entity.Indexed,
		entity.TableName,
		entity.PathToDataDir,
		entity.PathToFile,
		entity.RowCount,
		entity.SchemaName,
		entity.MetadataId,
		entity.SourceSliceTableInfoId,
	}


	_,err = ExecContext(ctx,
		`merge into table_info (
			ID
			,DATABASE_NAME
			,DUMPED
			,INDEXED
			,NAME
			,PATH_TO_DATA_DIR
			,PATH_TO_FILE
			,ROW_COUNT
			,SCHEMA_NAME
			,METADATA_ID
			,SOURCE_SLICE_TABLE_INFO_ID
       ) key(ID) `+data.valuePlaceholders(),
       	data...
       	)

	if err != nil {
		if newOne {
			entity.Id = nullable.NullInt64{}
			err = fmt.Errorf("could not add a new table_info row: %v", err)
		} else {
			err = fmt.Errorf("could not update table_info row with id=%v: %v", entity.Id.Value(), err)
		}
		return
	}
	return
}

func TableInfoByMetadata(ctx context.Context, metadata *dto.MetadataType) (result []*dto.TableInfoType, err error) {

	if metadata == nil {
		err = fmt.Errorf("metadata is not initialized")
		return
	}
	if !metadata.Id.Valid() {
		err = fmt.Errorf("metadata.id is not initialized!")
		return
	}

	result, err = tableInfo(
		ctx,
" WHERE METADATA_ID = ? and DUMPED = true",
		metadata.Id,
		)

	if err != nil {
		return
	}

	for tableIndex := range result {
		result[tableIndex].Metadata = metadata
		_, err = ColumnInfoByTable(ctx, result[tableIndex])
		if err != nil {
			return
		}
	}
	return
}

func TableInfoById(ctx context.Context, id int) (result *dto.TableInfoType, err error) {

	res, err := tableInfo(
		ctx,
	" WHERE ID = ? ",
	id,
	)

	if err == nil && len(res) > 0 {
		res[0].Columns, err = ColumnInfoByTable(ctx, res[0])
		if err != nil {
			err = fmt.Errorf("could not read columns for table %v: %v", res[0], err)
			return
		}
		if err == nil {
			result = res[0]
		}
	}
	return
}
