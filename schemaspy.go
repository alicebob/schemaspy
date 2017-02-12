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
	"sort"

	"github.com/jackc/pgx"
)

type Schema struct {
	Name string

	// Relations are all tables, views, and materialized views
	Relations map[string]Relation

	// all plain tables, ordered alphabetically. Every table has an entry in
	// Relations
	Tables []string

	// all views, ordered alphabetically. Every view has an entry in Relations
	Views []string

	// all materialized views, ordered alphabetically. Every materialized view
	// has an entry in Relations
	Materialized []string

	Indexes map[string]Index
}

// Relation is a table, view, or materialized view.
type Relation struct {
	// Type is "table", "view", or "materialized view"
	Type     string
	Columns  map[string]Column
	Inherits []string
	Children []string
	Indexes  []string
}

type Column struct {
	Type     string
	NotNull  bool
	Position int
}

type Index struct {
	Table   string
	Type    string
	Unique  bool
	Primary bool
	Columns []string // column name or '[function]' for expressions
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
	db, ok := dbs[schema]
	if !ok {
		return nil, fmt.Errorf("schema %q not found in pg_catalog", schema)
	}
	oids, err := loadSchema(tx, db.OID)
	if err != nil {
		return nil, err
	}

	d := &Schema{
		Name:      db.NspName,
		Relations: map[string]Relation{},
		Indexes:   map[string]Index{},
	}
	d.addRelations(oids)
	d.addInherits(oids)
	d.addColumns(oids)
	d.addIndexes(oids)

	return d, nil
}

func (s *Schema) addRelations(oids *_OIDs) {
	for _, st := range oids.class {
		r := Relation{
			Columns: map[string]Column{},
		}
		switch st.RelKind {
		case "r":
			r.Type = "table"
			s.Tables = append(s.Tables, st.RelName)
			sort.Strings(s.Tables)
		case "v":
			r.Type = "view"
			s.Views = append(s.Views, st.RelName)
			sort.Strings(s.Views)
		case "m":
			r.Type = "materialized view"
			s.Materialized = append(s.Materialized, st.RelName)
			sort.Strings(s.Materialized)
		default:
			continue
		}
		s.Relations[st.RelName] = r
	}
}

func (s *Schema) addInherits(oids *_OIDs) {
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
		child := s.Relations[childTable]
		child.Inherits = append(child.Inherits, parentTable)
		s.Relations[childTable] = child
		parent := s.Relations[parentTable]
		parent.Children = append(parent.Children, childTable)
		s.Relations[parentTable] = parent
	}
}

func (s *Schema) addColumns(oids *_OIDs) {
	for _, ct := range oids.attribute {
		if ct.AttNum < 0 {
			// system column
			continue
		}

		cl, ok := oids.class[ct.AttRelID]
		if !ok {
			continue
		}
		rel, ok := s.Relations[cl.RelName]
		if !ok {
			continue
		}
		rel.Columns[ct.AttName] = Column{
			Type:     oids.typ[ct.AttTypID].TypName,
			NotNull:  ct.AttNotNull,
			Position: ct.AttNum,
		}
		s.Relations[cl.RelName] = rel
	}
}

func (s *Schema) addIndexes(oids *_OIDs) {
	// indexes columns are split over pg_class 'i' records, and over pg_index
	for tOid, st := range oids.class {
		switch st.RelKind {
		case "i": // index
			index := oids.index[tOid]
			relName := oids.class[index.IndRelID].RelName
			rel := s.Relations[relName]

			var cols []string
			for _, i := range index.IndKey {
				if i == 0 {
					// TODO: indexprs could be used to render the function
					cols = append(cols, "[function]")
					continue
				}
				cols = append(cols, rel.ColumnNames()[i-1])
			}
			s.Indexes[st.RelName] = Index{
				Table:   relName,
				Type:    oids.am[st.RelAm].AmName,
				Unique:  index.IndIsUnique,
				Primary: index.IndIsPrimary,
				Columns: cols,
			}

			rel.Indexes = append(rel.Indexes, st.RelName)
			sort.Strings(rel.Indexes)
			s.Relations[relName] = rel
		}
	}
}

// ColumnNames lists all columns in database order
func (t *Relation) ColumnNames() []string {
	var names = make([]string, len(t.Columns))
	for c, d := range t.Columns {
		names[d.Position-1] = c
	}
	return names
}

// _OIDs has all the info from the pg_catalog tables in raw format
type _OIDs struct {
	class     map[pgx.Oid]schemaClass
	typ       map[pgx.Oid]schemaType
	inherits  []schemaInherits
	attribute []schemaAttribute
	index     map[pgx.Oid]schemaIndex
	am        map[pgx.Oid]schemaAm
}

func loadSchema(tx *pgx.Tx, schema pgx.Oid) (*_OIDs, error) {
	var (
		m   = &_OIDs{}
		err error
	)

	m.class, err = pgClass(tx, schema)
	if err != nil {
		return nil, err
	}

	m.typ, err = pgType(tx)
	if err != nil {
		return nil, err
	}

	m.inherits, err = pgInherits(tx)
	if err != nil {
		return nil, err
	}

	m.attribute, err = pgAttribute(tx)
	if err != nil {
		return nil, err
	}

	m.index, err = pgIndex(tx)
	if err != nil {
		return nil, err
	}

	m.am, err = pgAm(tx)
	if err != nil {
		return nil, err
	}

	return m, nil
}
