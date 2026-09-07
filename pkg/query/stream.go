package query

import (
	"database/sql"
	"reflect"
)

// Column describes a result column before any rows are consumed.
type Column struct {
	Name          string
	DatabaseType  string
	Nullable      bool
	NullableKnown bool
	Length        int64
	LengthKnown   bool
	Precision     int64
	Scale         int64
	DecimalKnown  bool
	ScanType      reflect.Type
}

// ColumnsFromSQL exposes database/sql's native result metadata without
// requiring the result to contain a row.
func ColumnsFromSQL(types []*sql.ColumnType) []Column {
	columns := make([]Column, len(types))
	for i, columnType := range types {
		columns[i] = Column{
			Name:         columnType.Name(),
			DatabaseType: columnType.DatabaseTypeName(),
			ScanType:     columnType.ScanType(),
		}
		columns[i].Nullable, columns[i].NullableKnown = columnType.Nullable()
		columns[i].Length, columns[i].LengthKnown = columnType.Length()
		columns[i].Precision, columns[i].Scale, columns[i].DecimalKnown = columnType.DecimalSize()
	}
	return columns
}

// RowStream is a forward-only native result. Values returns values for the
// current row; byte slices are owned by the returned row and remain valid after
// the next call to Next.
type RowStream interface {
	Columns() []Column
	Next() bool
	Values() ([]any, error)
	Err() error
	Close() error
}
