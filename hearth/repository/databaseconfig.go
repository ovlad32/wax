package repository

import (
	"context"
	"fmt"
	"github.com/goinggo/tracelog"
	"github.com/ovlad32/wax/hearth/dto"
)




func databaseConfig(ctx context.Context, where  whereFunc, args []interface{}) (result []*dto.DatabaseConfigType, err error) {
	var funcName = "repository::databaseConfig"
	tracelog.Started(packageName, funcName)

	tx, err := iDb.Conn(ctx)
	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}

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

	if where != nil {
		query = query + where()
	}
	query = query + " ORDER BY NAME"

	rws, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		tracelog.Error(err, packageName, funcName)
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
				tracelog.Error(err, packageName, funcName)
				return
			}
			result = append(result, &row)
		}
	}

	tracelog.Completed(packageName, funcName)
	return
}

func DatabaseConfigAll(ctx context.Context) (result []*dto.DatabaseConfigType, err error) {
	return databaseConfig(ctx, nil,nil)
}

func DatabaseConfigById(Id int) (ctx context.Context, result *dto.DatabaseConfigType, err error) {
	args := MakeWhereArgs()
	whereString := " WHERE ID = %v"
	switch currentDbType {
	case H2:
		whereString = fmt.Sprintf(whereString,Id)
	default:
		whereString = fmt.Sprintf(whereString,"?")
		args = append(args,Id)
	}
	where := func ()string{ return whereString }
	res, err := databaseConfig(ctx, where, args)

	if err == nil && len(res) > 0 {
		result = res[0]
	}
	return
}
