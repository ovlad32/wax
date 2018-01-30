package repository

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

func AppNodeSeqId() (id int64, err error) {
	err = iDb.QueryRow("select nextval('APP_NODE_SEQ')").Scan(&id)
	if err != nil {
		err = fmt.Errorf("could not get a sequential number from SPLIT_COLUMN_DATA_SEQ: %v", err)
	}
	return
}

func PutAppNode(ctx context.Context, entity *dto.AppNodeType) (err error) {
	var newOne bool
	if !entity.Id.Valid() {
		id, err := AppNodeSeqId()
		if err != nil {
			return err
		}
		entity.Id = nullable.NewNullInt64(id)
		newOne = true
	}
	if newOne {
		params := varray{
			entity.Id,
			entity.Hostname,
			entity.Address,
			entity.LastHeartbeat,
			entity.State,
			entity.Role,
			}
		dml :=
			`insert into app_node(id,hostname,address,last_heartbeat,state,role)`+
		 	params.valuePlaceholders()

		 	_, err = ExecContext(ctx, dml,params...)
		if err != nil {
			err = fmt.Errorf("could not add a new app_node row: %v", err)
			entity.Id = nullable.NullInt64{}
			return
		}
	} else {
		dml := `update app_node set last_heartbeat=?, state=? where id = ?`
		_, err = ExecContext(ctx, dml, entity.LastHeartbeat, entity.State, entity.Id)
		if err != nil {
			err = fmt.Errorf("could not update app_node row where id=%v: %v", entity.Id.Value(), err)
			return
		}
	}
	return
}

func appNode(ctx context.Context, where string,args ...interface{}) (result []*dto.AppNodeType, err error) {


	result = make([]*dto.AppNodeType, 0, 1)
	query := `SELECT 
		 ID
		 ,HOSTNAME
		 ,ADDRESS
		 ,LAST_HEARTBEAT
		 ,STATE
		 ,ROLE 
		 FROM APP_NODE `

	if where != "" {
		query = query + where
	}
	rows, err := QueryContext(ctx, query, args...)
	if err != nil {
		return
	}

	for rows.Next() {
		var row dto.AppNodeType
		err = rows.Scan(
			&row.Id,
			&row.Hostname,
			&row.Address,
			&row.LastHeartbeat,
			&row.State,
			&row.Role,
		)
		if err != nil {
			return
		}
		result = append(result, &row)
	}
	return
}

func AppNameById(ctx context.Context, appNodeId int64) (result *dto.AppNodeType, err error) {
	results, err := appNode(
		ctx,
		`WHERE ID = ?`,
		appNodeId,
		)
	if err == nil && len(results) > 0 {
		result = results[0]
	}
	return
}
