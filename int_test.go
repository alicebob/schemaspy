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

func Test(t *testing.T) {
	db := mustDBPool(t)

	d, err := Describe(db, "")
	if err != nil {
		t.Fatal(err)
	}
	if have, want := d.Name, "public"; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}

	if have, want := len(d.Tables), 3; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}
	{
		tab, ok := d.Tables["simple"]
		if have, want := ok, true; have != want {
			t.Errorf("have %#v, want %#v", have, want)
		}
		if have, want := len(tab.Columns), 3; have != want {
			t.Fatalf("have %#v, want %#v", have, want)
		}
		if have, want := []string{"id", "name", "t"}, tab.ColumnNames(); !reflect.DeepEqual(have, want) {
			t.Errorf("have %#v, want %#v", have, want)
		}
		if have, want := (Column{
			Type:     "uuid",
			NotNull:  true,
			Position: 1,
		}), tab.Columns["id"]; have != want {
			t.Errorf("have %#v, want %#v", have, want)
		}
		if have, want := (Column{
			Type:     "text",
			NotNull:  false,
			Position: 2,
		}), tab.Columns["name"]; have != want {
			t.Errorf("have %#v, want %#v", have, want)
		}
	}

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
