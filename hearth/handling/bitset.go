package handling

import (
	"context"
	"io"
)

type BitsetInterface interface {
	Set(uint64) bool
	//Test(uint64) bool
	BitChan(context.Context) chan uint64
	WriteTo(context.Context, io.Writer) error
	ReadFrom(context.Context, io.Reader) error
	Cardinality() uint64
}
