package repository

import (
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"context"
)

func metadata(ctx context.Context, where whereFuncType) (result []*dto.MetadataType, err error) {
	var args varray
	result = make([]*dto.MetadataType, 0)
	query := "SELECT " +
		" ID" +
		" ,INDEX" +
		" ,VERSION" +
		" ,DATABASE_CONFIG_ID" +
		" FROM METADATA "

	if where != nil {
		var whereClause string
		whereClause,args = where()
		query = query + whereClause
	}
//	query = query + " ORDER BY ID"
	rws, err := QueryContext(ctx,query,args...)
	if err != nil {
		return
	}

	for rws.Next() {
		var row dto.MetadataType
		err = rws.Scan(
			&row.Id,
			&row.Index,
			&row.Version,
			&row.DatabaseConfigId,
		)
		if err != nil {
			return
		}
		result = append(result, &row)
	}
	return
}

func HighestDatabaseConfigVersion(ctx context.Context, DatabaseConfigId uint) (result nullable.NullInt64, err error) {
	err = QueryRowContext(ctx,
		`select max(version) from metadata where database_config_id = ?`,
			varray{DatabaseConfigId},
			).Scan(result)
	return
}

/*func LastTakenMetadata(ctx context.Context, DatabaseConfigId uint) (result *dto.MetadataType, err error) {
	version, err := HighestDatabaseConfigVersion(DatabaseConfigId)

	tx, err := iDb.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()
	args := MakeWhereArgs()
	whereString := " WHERE DATABASE_CONFIG_ID = %v and VERSION = %v "
	switch currentDbType {
	case H2:
		whereString = fmt.Sprintf(whereString, DatabaseConfigId, version)
	default:
		whereString = fmt.Sprintf(whereString, "?","?")
		args = append(args, DatabaseConfigId, version)
	}
	results, err := metadata(
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
*/
func MetadataById(ctx context.Context, metadataId int) (result *dto.MetadataType, err error) {
	results, err := metadata(
		ctx,
		func() (string,varray) {
			return " WHERE ID = ?",varray{metadataId}
		},
	)
	if err == nil && len(results) > 0 {
		result = results[0]
	}
	return
}

func MetadataByWorkflowId(workflowId int) (metadataId []int, err error) {
	queryText := fmt.Sprintf("select distinct t.metadata_id from link l "+
		" inner join column_info  c on c.id in (l.parent_column_info_id,l.child_column_info_id) "+
		" inner join table_info t on t.id = c.table_info_id "+
		" where l.workflow_id = %v ", workflowId)

	tx, err := iDb.Begin()
	if err != nil {
		return
	}

	result, err := tx.Query(queryText)
	if err != nil {
		return
	}
	defer result.Close()
	var id int
	for result.Next() {
		err = result.Scan(&id)
		if err != nil {

		}
		if metadataId == nil {
			metadataId = make([]int, 0, 2)
		}
		metadataId = append(metadataId, id)
	}
	return
}

func PutMetadata(m *dto.MetadataType) (err error) {
	tx, err := iDb.Begin()
	if err != nil {
		return
	}

	if !m.Id.Valid() {
		row := tx.QueryRow("select nextval('META_DATA_SEQ')")
		var id int64
		err = row.Scan(&id)
		if err != nil {
			return
		}
		m.Id = nullable.NewNullInt64(id)
	}

	statement := "merge into metadata (id, index, version, database_config) " +
		" key(id) values(%v,%v,%v,%v,%v,%v,%v,%v)"

	statement = fmt.Sprintf(
		statement,
		m.Id,
		m.Index,
		m.Version,
		m.DatabaseConfigId,
	)
	_, err = tx.Exec(statement)
	if err != nil {
		return
	}

	tx.Commit()
	return
}
