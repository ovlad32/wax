package repository

import (
	"fmt"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"github.com/ovlad32/wax/hearth/dto"
	"context"
	"github.com/pkg/errors"
)

func distTaskSeqId() (id int64, err error) {
	err = iDb.QueryRow("select nextval('DIST_TASK_SEQ')").Scan(&id)
	if err != nil {
		err = fmt.Errorf("could not get a sequential number from DIST_TASK_SEQ: %v", err)
	}
	return
}



func PutDistTask(ctx context.Context, entity *dto.DistTaskType) (err error) {
	var newOne bool
	if !entity.Id.Valid() {
		id, err := distTaskSeqId()
		if err != nil {
			return err
		}
		entity.Id = nullable.NewNullInt64(id)
		newOne = true
	}
	if newOne {
		params := varray{
			entity.Id,
			entity.Task,
		}
		dml :=
			`insert into dist_task(id,task)` +
				params.valuePlaceholders()

		_, err = ExecContext(ctx, dml, params...)
		if err != nil {
		    err= errors.Wrap(err,"could not add a new nodework row")
			entity.Id = nullable.NullInt64{}
			return
		}
	} else {
		dml := `update dist_task set status = ? where id = ?`
		_, err = ExecContext(ctx, dml, entity.Status, entity.Id)
		if err != nil {
			err = errors.Wrapf(err,"could not update dist_task row where id=%v:",entity.Id)
			return
		}
	}
	return
}




func PutDistTaskNode(ctx context.Context, entity *dto.DistTaskNodeType) (err error) {
	var newOne bool
	if !entity.Id.Valid() {
		id, err := distTaskSeqId()
		if err != nil {
			return err
		}
		entity.Id = nullable.NewNullInt64(id)
		newOne = true
	}
	if newOne {
		params := varray{
			entity.Id,
			entity.DistTaskId,
			entity.NodeId,
			entity.WorkerId,
			entity.DataSubject,
		}
		dml :=
			`insert into dist_task_node(id, dist_task_id, node_id, worker_id, data_subject)` +
				params.valuePlaceholders()

		_, err = ExecContext(ctx, dml, params...)
		if err != nil {
			err= errors.Wrap(err,"could not add a new dist_task_node row")
			entity.Id = nullable.NullInt64{}
			return
		}
	} else {
		panic("PutDistTaskNode Update is not implemented")

		dml := `update dist_task_node set ? where id = ?`
		_, err = ExecContext(ctx, dml, entity.Id)
		if err != nil {
			err = errors.Wrapf(err,"could not update dist_task_node row where id=%v:",entity.Id)
			return
		}
	}
	return
}