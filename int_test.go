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
	d, err := Describe(db, "")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func Test(t *testing.T) {
	d := setup(t)

	if have, want := d.Name, "public"; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}

	if have, want := len(d.Tables), 4; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}
}

func TestSimple(t *testing.T) {
	d := setup(t)

	tab, ok := d.Tables["simple"]
	if have, want := ok, true; have != want {
		t.Errorf("have %#v, want %#v", have, want)
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
		tab := d.Tables["root"]
		if have, want := tab.Inherits, []string(nil); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
		if have, want := tab.Children, []string{"root_123"}; !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}
	{
		tab := d.Tables["root_123"]
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

	if have, want := d.Tables["indexed"].Indexes, []string{
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
