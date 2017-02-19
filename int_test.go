// +build int

package schemaspy

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx"
)

const (
	intPGURL = "postgres://@localhost/schemaspy"
)

func setup(t *testing.T) *Schema {
	db := mustDBPool(t)
	d, err := Describe(db, "schemaspyint")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func Test(t *testing.T) {
	d := setup(t)

	if have, want := d.Name, "schemaspyint"; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}

	if have, want := len(d.Tables), 4; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}
}

func TestSimple(t *testing.T) {
	d := setup(t)

	tab, ok := d.Relations["simple"]
	if have, want := ok, true; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}
	if have, want := tab.Type, "table"; have != want {
		t.Fatalf("have %#v, want %#v", have, want)
	}
	if have, want := len(tab.Columns), 3; have != want {
		t.Fatalf("have %#v, want %#v", have, want)
	}
	if have, want := tab.ColumnNames(), []string{"id", "name", "t"}; !reflect.DeepEqual(have, want) {
		t.Errorf("have %#v, want %#v", have, want)
	}
	if have, want := tab.Columns["id"], (Column{
		Type:     "uuid",
		NotNull:  true,
		Position: 1,
	}); have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}
	if have, want := tab.Columns["name"], (Column{
		Type:     "text",
		NotNull:  false,
		Position: 2,
	}); have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}
}

func TestInherit(t *testing.T) {
	d := setup(t)

	{
		tab := d.Relations["root"]
		if have, want := tab.Inherits, []string(nil); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
		if have, want := tab.Children, []string{"root_123"}; !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
	{
		tab := d.Relations["root_123"]
		if have, want := tab.Inherits, []string{"root"}; !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
}

func TestIndexes(t *testing.T) {
	d := setup(t)
	if have, want := len(d.Indexes), 4; have != want {
		t.Fatalf("have %#v, want %#v", have, want)
	}
	{
		u, ok := d.Indexes["unique_indexed"]
		if have, want := ok, true; have != want {
			t.Errorf("have %#v, want %#v", have, want)
		}
		if have, want := u, (Index{
			Table:   "indexed",
			Type:    "btree",
			Unique:  true,
			Columns: []string{"name"},
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
	{
		u, ok := d.Indexes["index_indexed"]
		if have, want := ok, true; have != want {
			t.Errorf("have %#v, want %#v", have, want)
		}
		if have, want := u, (Index{
			Table:   "indexed",
			Type:    "btree",
			Unique:  false,
			Columns: []string{"major", "minor"},
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
	{
		u, ok := d.Indexes["indexed_name_lower_idx"]
		if have, want := ok, true; have != want {
			t.Errorf("have %#v, want %#v", have, want)
		}
		if have, want := u, (Index{
			Table:   "indexed",
			Type:    "btree",
			Unique:  false,
			Columns: []string{"[function]", "minor"},
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}

	if have, want := d.Relations["indexed"].Indexes, []string{
		"index_indexed", "indexed_name_lower_idx", "unique_indexed",
	}; !reflect.DeepEqual(have, want) {
		t.Fatalf("have %#v, want %#v", have, want)
	}

	// Simple has a primary key
	{
		u := d.Indexes["simple_pkey"]
		if have, want := u, (Index{
			Table:   "simple",
			Type:    "btree",
			Unique:  true,
			Primary: true,
			Columns: []string{"id"},
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
}

func TestViews(t *testing.T) {
	d := setup(t)

	if have, want := d.Views, []string{"myview_now"}; !reflect.DeepEqual(have, want) {
		t.Errorf("have %#v, want %#v", have, want)
	}

	{
		u := d.Relations["myview_now"]
		if have, want := u, (Relation{
			Type: "view",
			Columns: map[string]Column{
				"id": {
					Type:     "uuid",
					Position: 1,
				},
				"name": {
					Type:     "text",
					Position: 2,
				},
			},
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
}

func TestMaterialized(t *testing.T) {
	d := setup(t)
	if have, want := d.Materialized, []string{"myview_forever"}; !reflect.DeepEqual(have, want) {
		t.Errorf("have %#v, want %#v", have, want)
	}

	{
		u := d.Relations["myview_forever"]
		if have, want := u, (Relation{
			Type: "materialized view",
			Columns: map[string]Column{
				"id": {
					Type:     "uuid",
					Position: 1,
				},
				"name": {
					Type:     "text",
					Position: 2,
				},
			},
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
}

func TestSequence(t *testing.T) {
	d := setup(t)
	if have, want := len(d.Sequences), 1; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}

	{
		s := d.Sequences["countme"]
		if have, want := s, (Sequence{
			IncrementBy: 42,
			MinValue:    4001,
			MaxValue:    400100,
			Start:       40010,
			Cycle:       true,
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
}

func TestFunctions(t *testing.T) {
	d := setup(t)
	if have, want := len(d.Functions), 3; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}

	{
		s := d.Functions["my_first_sql_function"]
		if have, want := s, (Function{
			Language:      "sql",
			ArgumentTypes: []string(nil),
			Src:           "\n    SELECT name FROM schemaspyint.indexed\n    WHERE minor < 0;\n",
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}

	{
		s := d.Functions["my_first_plpgsql_function"]
		if have, want := s, (Function{
			Language:      "plpgsql",
			ArgumentTypes: []string{"float4"},
			Src:           "\nBEGIN\n    RETURN subtotal * 0.06;\nEND;\n",
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}

	{
		s := d.Functions["my_first_variadic_function"]
		if have, want := s, (Function{
			Language:      "sql",
			ArgumentTypes: []string{"numeric[]"},
			Src:           "\n    SELECT min($1[i]) FROM generate_subscripts($1, 1) g(i);\n",
		}); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
}

func mustDBPool(t *testing.T) *pgx.ConnPool {
	cc, err := pgx.ParseURI(intPGURL)
	if err != nil {
		t.Fatal(err)
	}
	db, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: cc,
	})
	if err != nil {
		t.Fatal(err)
	}
	return db
}
