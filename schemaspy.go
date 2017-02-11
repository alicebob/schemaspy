// schemaspy reads the definition of Postgres's databases, tables, functions,
// &c.
//
// It can't change things; the use case is for maintance and setup scripts
// which need to inspect the current state of the database. Those scripts are
// expected to draw their conclusions and apply changes with `ALTER` commands.
//
package schemaspy

import (
	"fmt"

	"github.com/jackc/pgx"
)

type Catalog struct {
	Name   string
	Tables map[string]Table
}

type Table struct {
	Type    string
	Columns map[string]Column
}

type Column struct {
	Name string
	Type string
	// Null bool
	// ...
}

// Describe the current catalog (database). This is the main entry point.
func Describe(db *pgx.ConnPool) (*Catalog, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	d := &Catalog{
		Tables: map[string]Table{},
	}

	names, err := pgCatalogName(tx)
	if err != nil {
		return nil, err
	}
	if found := len(names); found != 1 {
		return nil, fmt.Errorf("expected 1 catalog_name row, got %d", found)
	}
	d.Name = names[0].CatalogName

	tables, err := pgTables(db)
	if err != nil {
		return d, err
	}
	if err := d.addTables(tables); err != nil {
		return d, err
	}

	return d, nil
}

func (c *Catalog) addTables(ts []schemaTable) error {
	for _, st := range ts {
		if st.Catalog != c.Name {
			continue
		}
		if st.Schema != "public" {
			continue
		}
		t := Table{
			Type: st.Type,
		}
		c.Tables[st.Name] = t
	}
	return nil
}
