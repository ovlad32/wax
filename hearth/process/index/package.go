package index

import (
	"context"
	"github.com/ovlad32/wax/hearth/dto"
)

var packageName string = "index"

type BitsetIndexer interface {
	BuildBitsetsForColumns(
		ctx context.Context,
		pathToDumpDirectory string,
		tableDumpFileName string,
		bitsetContent dto.BitsetContentArrayType,
		targetColumns dto.ColumnListInterface,
	) (err error)
}
