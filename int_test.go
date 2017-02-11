// +build int

package schemaspy

import (
	"testing"

	"github.com/jackc/pgx"
)

const (
	intPGURL = "postgres://@localhost/schemaspy"
)

func Test(t *testing.T) {
	db := mustDBPool(t)

	d, err := Describe(db)
	if err != nil {
		t.Fatal(err)
	}
	if have, want := d.Name, "schemaspy"; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}

	if have, want := len(d.Tables), 1; have != want {
		t.Errorf("have %#v, want %#v", have, want)
	}
	{
		tab, ok := d.Tables["simple"]
		if have, want := ok, true; have != want {
			t.Errorf("have %#v, want %#v", have, want)
		}
		if have, want := len(tab.Columns), 3; have != want {
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
