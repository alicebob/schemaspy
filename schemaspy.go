// schemaspy reads the definition of a PostgreSQL schema: tables, functions,
// indexes &c. and returns it as a datastructure.
//
// It can't change things; the use case is for maintenance and setup scripts
// which need to inspect the current state of the database. Those scripts are
// expected to draw their conclusions and if needed apply their changes with
// `ALTER` commands.
//
package schemaspy

import (
	"fmt"

	"github.com/jackc/pgx"
)

type Schema struct {
	Name   string
	Tables map[string]Table
}

type Table struct {
	Columns  map[string]Column
	Inherits []string
	Children []string
}

type Column struct {
	Type     string
	NotNull  bool
	Position int
}

// Describe a schema. This is the main entry point. Leave schema empty for the
// public schema.
func Describe(conn *pgx.ConnPool, schema string) (*Schema, error) {
	if schema == "" {
		schema = "public"
	}
	tx, err := conn.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	dbs, err := pgNamespace(tx)
	if err != nil {
		return nil, err
	}
	var db schemaNamespace
	for _, db = range dbs {
		if db.NspName == schema {
			goto found
		}
	}
	return nil, fmt.Errorf("schema %q not found in pg_catalog", schema)
found:
	oids, err := loadSchema(tx, db.OID)
	if err != nil {
		return nil, err
	}

	d := &Schema{
		Name:   db.NspName,
		Tables: map[string]Table{},
	}
	d.addTables(oids)
	d.addInherits(oids)
	d.addColumns(oids)

	return d, nil
}

func (s *Schema) addTables(oids *OIDs) {
	for _, st := range oids.class {
		switch st.RelKind {
		case "r":
			// ordinary table
			s.Tables[st.RelName] = Table{
				Columns: map[string]Column{},
			}
		}
	}
}

func (s *Schema) addInherits(oids *OIDs) {
	for _, e := range oids.inherits {
		childO, ok := oids.class[e.InhRelID]
		if !ok {
			continue
		}
		childTable := childO.RelName
		parentO, ok := oids.class[e.InhParent]
		if !ok {
			continue
		}
		parentTable := parentO.RelName
		child := s.Tables[childTable]
		child.Inherits = append(child.Inherits, parentTable)
		s.Tables[childTable] = child
		parent := s.Tables[parentTable]
		parent.Children = append(parent.Children, childTable)
		s.Tables[parentTable] = parent
	}
}

func (s *Schema) addColumns(oids *OIDs) {
	for _, ct := range oids.attribute {
		cl, ok := oids.class[ct.AttRelID]
		if !ok {
			continue
		}
		tab, ok := s.Tables[cl.RelName]
		if !ok {
			// not in our schema
			continue
		}
		if ct.AttNum < 0 {
			// system column
			continue
		}
		tab.Columns[ct.AttName] = Column{
			Type:     oids.typ[ct.AttTypID].TypName,
			NotNull:  ct.AttNotNull,
			Position: ct.AttNum,
		}
		s.Tables[cl.RelName] = tab
	}
}

// ColumnNames lists all columns in table order
func (t *Table) ColumnNames() []string {
	var names = make([]string, len(t.Columns))
	for c, d := range t.Columns {
		names[d.Position-1] = c
	}
	return names
}

type OIDs struct {
	class     map[pgx.Oid]schemaClass
	typ       map[pgx.Oid]schemaType
	inherits  []schemaInherits
	attribute []schemaAttribute
}

func loadSchema(tx *pgx.Tx, schema pgx.Oid) (*OIDs, error) {
	m := &OIDs{
		class: map[pgx.Oid]schemaClass{},
		typ:   map[pgx.Oid]schemaType{},
	}

	classes, err := pgClass(tx, schema)
	if err != nil {
		return nil, err
	}
	for _, c := range classes {
		m.class[c.OID] = c
	}

	types, err := pgType(tx)
	if err != nil {
		return nil, err
	}
	for _, t := range types {
		m.typ[t.OID] = t
	}

	inherits, err := pgInherits(tx)
	if err != nil {
		return nil, err
	}
	m.inherits = inherits

	attrs, err := pgAttribute(tx)
	if err != nil {
		return nil, err
	}
	m.attribute = attrs

	return m, nil
}
