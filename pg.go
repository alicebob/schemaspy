package schemaspy

import (
	"github.com/jackc/pgx"
)

// Queryer is either a ConnPool, a Conn, or a Tx.
type Queryer interface {
	Query(sql string, args ...interface{}) (*pgx.Rows, error)
}

type schemaCatalogName struct {
	CatalogName string
}

func pgCatalogName(conn Queryer) ([]schemaCatalogName, error) {
	rows, err := conn.Query(`
			SELECT
				catalog_name
			FROM
				information_schema.information_schema_catalog_name
		`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []schemaCatalogName
	for rows.Next() {
		var m schemaCatalogName
		if err := rows.Scan(&m.CatalogName); err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, rows.Err()
}

type schemaTable struct {
	Catalog string
	Schema  string
	Name    string
	Type    string
}

func pgTables(conn Queryer) ([]schemaTable, error) {
	rows, err := conn.Query(`
			SELECT
				table_catalog, table_schema, table_name, table_type
			FROM
				information_schema.tables
		`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []schemaTable
	for rows.Next() {
		var t schemaTable
		if err := rows.Scan(&t.Catalog, &t.Schema, &t.Name, &t.Type); err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, rows.Err()
}

type schemaColumn struct {
	TableCatalog    string
	TableSchema     string
	TableName       string
	ColumnName      string
	OrdinalPosition int
	ColumnDefault   *string
	IsNullable      string
	DataType        string
}

func pgColumns(conn Queryer) ([]schemaColumn, error) {
	rows, err := conn.Query(`
			SELECT
				table_catalog
				, table_schema
				, table_name
				, column_name
				, ordinal_position
				, column_default
				, is_nullable
				, data_type
			FROM
				information_schema.columns
		`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []schemaColumn
	for rows.Next() {
		var c schemaColumn
		if err := rows.Scan(
			&c.TableCatalog,
			&c.TableSchema,
			&c.TableName,
			&c.ColumnName,
			&c.OrdinalPosition,
			&c.ColumnDefault,
			&c.IsNullable,
			&c.DataType,
		); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, rows.Err()
}
