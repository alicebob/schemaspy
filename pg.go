package schemaspy

import (
	"github.com/jackc/pgx"
)

// Queryer is either a ConnPool, a Conn, or a Tx.
type Queryer interface {
	Query(sql string, args ...interface{}) (*pgx.Rows, error)
}

type schemaNamespace struct {
	OID     pgx.Oid
	NspName string
}

func pgNamespace(conn Queryer) ([]schemaNamespace, error) {
	rows, err := conn.Query(`
		SELECT
			oid, nspname
		FROM
			pg_catalog.pg_namespace
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []schemaNamespace
	for rows.Next() {
		var c schemaNamespace
		if err := rows.Scan(&c.OID, &c.NspName); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, rows.Err()
}

// tables (and related things like views)
// https://www.postgresql.org/docs/9.6/static/catalog-pg-class.html
type schemaClass struct {
	RelName string
	RelType pgx.Oid
	RelAm   pgx.Oid
	RelKind string
}

func pgClass(conn Queryer, namespace pgx.Oid) (map[pgx.Oid]schemaClass, error) {
	rows, err := conn.Query(`
			SELECT
				oid, relname, reltype, relam, relkind
			FROM
				pg_catalog.pg_class
			WHERE
				relnamespace=$1
		`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res = map[pgx.Oid]schemaClass{}
	for rows.Next() {
		var (
			t   schemaClass
			oid pgx.Oid
		)
		if err := rows.Scan(&oid, &t.RelName, &t.RelType, &t.RelAm, &t.RelKind); err != nil {
			return nil, err
		}
		res[oid] = t
	}
	return res, rows.Err()
}

// columns
// https://www.postgresql.org/docs/9.6/static/catalog-pg-attribute.html
type schemaAttribute struct {
	AttRelID   pgx.Oid
	AttName    string
	AttTypID   pgx.Oid
	AttNum     int
	AttNotNull bool
}

func pgAttribute(conn Queryer) ([]schemaAttribute, error) {
	rows, err := conn.Query(`
			SELECT
				attrelid, attname, atttypid, attnum, attnotnull
			FROM
				pg_catalog.pg_attribute
		`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []schemaAttribute
	for rows.Next() {
		var c schemaAttribute
		if err := rows.Scan(
			&c.AttRelID,
			&c.AttName,
			&c.AttTypID,
			&c.AttNum,
			&c.AttNotNull,
		); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, rows.Err()
}

// types
// https://www.postgresql.org/docs/9.6/static/catalog-pg-type.html
type schemaType struct {
	TypName string
}

func pgType(conn Queryer) (map[pgx.Oid]schemaType, error) {
	rows, err := conn.Query(`
			SELECT
				oid, typname
			FROM
				pg_catalog.pg_type
		`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res = map[pgx.Oid]schemaType{}
	for rows.Next() {
		var (
			c   schemaType
			oid pgx.Oid
		)
		if err := rows.Scan(
			&oid,
			&c.TypName,
		); err != nil {
			return nil, err
		}
		res[oid] = c
	}
	return res, rows.Err()
}

// inheritence
// https://www.postgresql.org/docs/9.6/static/catalog-pg-inherits.html
type schemaInherits struct {
	InhRelID, InhParent pgx.Oid
	InhSeqNo            int
}

func pgInherits(conn Queryer) ([]schemaInherits, error) {
	rows, err := conn.Query(`
			SELECT
				inhrelid, inhparent, inhseqno
			FROM
				pg_catalog.pg_inherits
		`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []schemaInherits
	for rows.Next() {
		var c schemaInherits
		if err := rows.Scan(
			&c.InhRelID,
			&c.InhParent,
			&c.InhSeqNo,
		); err != nil {
			return nil, err
		}
		res = append(res, c)
	}
	return res, rows.Err()
}

// index
// This is in addition to the entries in pg_class
// https://www.postgresql.org/docs/9.6/static/catalog-pg-index.html
type schemaIndex struct {
	IndexRelID   pgx.Oid
	IndRelID     pgx.Oid
	IndIsUnique  bool
	IndIsPrimary bool
	IndKey       []int32
}

// pgIndex mapped to the pg_class entry they belong to
func pgIndex(conn Queryer) (map[pgx.Oid]schemaIndex, error) {
	// TODO: expressions can be rendered with:
	// > pg_get_expr(indexprs, indrelid) as expression
	// But no idea how to use that with multiple expressions.
	rows, err := conn.Query(`
			SELECT
				indexrelid, indrelid, indisunique, indisprimary, indkey::int4[]
			FROM
				pg_catalog.pg_index
		`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res = map[pgx.Oid]schemaIndex{}
	for rows.Next() {
		var c schemaIndex
		if err := rows.Scan(
			&c.IndexRelID,
			&c.IndRelID,
			&c.IndIsUnique,
			&c.IndIsPrimary,
			&c.IndKey,
		); err != nil {
			return nil, err
		}
		res[c.IndexRelID] = c
	}
	return res, rows.Err()
}

// am (access methods)
// https://www.postgresql.org/docs/9.6/static/catalog-pg-am.html
type schemaAm struct {
	AmName string
}

func pgAm(conn Queryer) (map[pgx.Oid]schemaAm, error) {
	rows, err := conn.Query(`
			SELECT
				oid, amname
			FROM
				pg_catalog.pg_am
		`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res = map[pgx.Oid]schemaAm{}
	for rows.Next() {
		var (
			c   schemaAm
			oid pgx.Oid
		)
		if err := rows.Scan(
			&oid,
			&c.AmName,
		); err != nil {
			return nil, err
		}
		res[oid] = c
	}
	return res, rows.Err()
}
