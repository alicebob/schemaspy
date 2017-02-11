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
	OID     pgx.Oid
	RelType pgx.Oid
	RelName string
	RelKind string
}

func pgClass(conn Queryer, namespace pgx.Oid) ([]schemaClass, error) {
	rows, err := conn.Query(`
			SELECT
				oid, relname, reltype, relkind
			FROM
				pg_catalog.pg_class
			WHERE
				relnamespace=$1
		`, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []schemaClass
	for rows.Next() {
		var t schemaClass
		if err := rows.Scan(&t.OID, &t.RelName, &t.RelType, &t.RelKind); err != nil {
			return nil, err
		}
		res = append(res, t)
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
	OID     pgx.Oid
	TypName string
}

func pgType(conn Queryer) ([]schemaType, error) {
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

	var res []schemaType
	for rows.Next() {
		var c schemaType
		if err := rows.Scan(
			&c.OID,
			&c.TypName,
		); err != nil {
			return nil, err
		}
		res = append(res, c)
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
