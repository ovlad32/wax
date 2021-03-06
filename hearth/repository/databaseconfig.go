package repository

import (
	"context"
	"github.com/ovlad32/wax/hearth/dto"
)

func databaseConfig(ctx context.Context, where string, args ...interface{}) (result []*dto.DatabaseConfigType, err error) {
	result = make([]*dto.DatabaseConfigType, 0)

	query := "SELECT " +
		" ID" +
		" ,DATABASE_NAME" +
		" ,DB_GROUP" +
		" ,NAME" +
		" ,HOST" +
		" ,PORT" +
		" ,TARGET" +
		" ,SCHEMA" +
		" ,USERNAME" +
		" ,PASSWORD " +
		" FROM DATABASE_CONFIG "

	if where != "" {
		query = query + where
	}
	query = query + " ORDER BY NAME"

	rws, err := QueryContext(ctx, query, args...)
	if err != nil {
		return
	}

	defer rws.Close()

	for rws.Next() {
		select {
		case <-ctx.Done():
			return
		default:

			var row dto.DatabaseConfigType
			err = rws.Scan(
				&row.Id,
				&row.DatabaseName,
				&row.DatabaseGroup,
				&row.DatabaseAlias,
				&row.ServerHost,
				&row.ServerPort,
				&row.ServerType,
				&row.TargetSchema,
				&row.UserName,
				&row.Password,
			)
			if err != nil {
				return
			}
			result = append(result, &row)
		}
	}

	return
}

func DatabaseConfigAll(ctx context.Context) (result []*dto.DatabaseConfigType, err error) {
	return databaseConfig(ctx, "")
}

func DatabaseConfigById(Id int) (ctx context.Context, result *dto.DatabaseConfigType, err error) {
	res, err := databaseConfig(ctx,
		" WHERE ID = ?",
		Id,
	)

	if err == nil && len(res) > 0 {
		result = res[0]
	}
	return
}
