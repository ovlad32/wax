package repository

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
)

func tableInfo(ctx context.Context, whereFunc func() string) (result []*dto.TableInfoType, err error) {

	tx, err := iDb.Conn(ctx)
	if err != nil {
		return
	}
	result = make([]*dto.TableInfoType, 0)

	query := "SELECT " +
		" ID" +
		" ,DATABASE_NAME" +
		" ,SCHEMA_NAME" +
		" ,NAME" +
		" ,ROW_COUNT" +
		" ,DUMPED" +
		" ,PATH_TO_FILE" +
		" ,PATH_TO_DATA_DIR" +
		" ,METADATA_ID" +
		" FROM TABLE_INFO "

	if whereFunc != nil {
		query = query + whereFunc()
	}
	query = query + " ORDER BY NAME"

	rws, err := tx.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer rws.Close()

	for rws.Next() {
		select {
		case <-ctx.Done():
			break
		default:
			var row dto.TableInfoType
			err = rws.Scan(
				&row.Id,
				&row.DatabaseName,
				&row.SchemaName,
				&row.TableName,
				&row.RowCount,
				&row.Dumped,
				&row.PathToFile,
				&row.PathToDataDir,
				&row.MetadataId,
			)
			if err != nil {
				return
			}
			result = append(result, &row)
		}
	}
	return
}

func TableInfoByMetadata(ctx context.Context, metadata *dto.MetadataType) (result []*dto.TableInfoType, err error) {
	whereFunc := func() string {
		if metadata != nil && metadata.Id.Valid() {
			return fmt.Sprintf(" WHERE METADATA_ID = %v and DUMPED=true", metadata.Id)
		}
		return ""
	}
	result, err = tableInfo(ctx, whereFunc)
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
	whereFunc := func() string {
		return fmt.Sprintf(" WHERE ID = %v", id)
	}
	res, err := tableInfo(ctx, whereFunc)
	if err == nil && len(res) > 0 {
		res[0].Columns, err = ColumnInfoByTable(ctx, res[0])
		if err == nil {
			result = res[0]
		}
	}
	return
}
