package repository

import (
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"context"
)

func AppNodeSeqId() (id int64, err error) {
	err = iDb.QueryRow("select nextval('SPLIT_COLUMN_DATA_SEQ')").Scan(&id)
	if err != nil {
		err = fmt.Errorf("could not get a sequential number from SPLIT_COLUMN_DATA_SEQ: %v", err)
	}
	return
}

func PutAppNode(entity *dto.AppNodeType) (err error) {

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
			dml = "insert into app_node(id,hostname,address,last_heartbeat,state,role) values(%v,'%v','%v','%v','%v','%v')"
			dml = fmt.Sprintf(dml,entity.Id,entity.Hostname,entity.Address,entity.LastHeartbeat,entity.State,entity.Role)
			_,err = iDb.Exec(dml)
		default:
			dml = "insert into app_node(id,hostname,address,last_heartbeat,state,role) values(?,?,?,?,?,?)"
			_,err = iDb.Exec(dml,entity.Id,entity.Hostname,entity.Address,entity.LastHeartbeat,entity.State,entity.Role)
		}
		if err != nil {
			err = fmt.Errorf("could not add a new category_split row: %v",err)
			return
		}
	} else {
		switch currentDbType {
		case H2:
			dml = "update app_node set last_heartbeat='%v', state='%v' where id = %v"
			dml = fmt.Sprintf(dml,entity.LastHeartbeat,entity.State,entity.Id)
			_,err = iDb.Exec(dml)
		default:
			dml = "update app_node set last_heartbeat=?, state=? where id = ?"
			_,err = iDb.Exec(dml,entity.LastHeartbeat,entity.State,entity.Id)
		}
		if err != nil {
			err = fmt.Errorf("could not update category_split row where id=%v: %v",entity.Id.Value(), err)
			return
		}
	}
	return
}




func appNode(ctx context.Context, where whereFunc, args []interface{}) (result []*dto.AppNodeType, err error) {
	tx, err := iDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	result = make([]*dto.AppNodeType, 0)
	query := "SELECT " +
		" ID" +
		" ,HOSTNAME" +
		" ,ADDRESS" +
		" ,LAST_HEARTBEAT" +
		" ,STATE" +
		" ,ROLE" +
		" FROM APP_NODE "

	if where != nil {
		query = query + where()
	}
	//	query = query + " ORDER BY ID"
	rws, err := tx.QueryContext(ctx,query,args...)
	if err != nil {
		return
	}

	for rws.Next() {
		var row dto.AppNodeType
		err = rws.Scan(
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



func AppNameById(ctx context.Context, appNodeId int) (result *dto.AppNodeType, err error) {
	args := MakeWhereArgs()
	whereString := " WHERE ID = %v"
	switch currentDbType {
	case H2:
		whereString = fmt.Sprintf(whereString, appNodeId)
	default:
		whereString = fmt.Sprintf(whereString, "?")
		args = append(args,appNodeId)
	}
	results, err := appNode(
		ctx,
		func() string {
			return whereString
		},
		args,
	)
	if err == nil && len(results) > 0 {
		result = results[0]
	}
	return
}
