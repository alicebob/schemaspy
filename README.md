# What

Schemaspy gets a schema from a PostgreSQL server as a simple Go datastructure.
It describes which tables there are, their columns, &c. Schemaspy only reads; any changes to the database need to be done by
other means, such as `ALTER TABLE`.

# Use cases

how this is used:

- a maintance script which creates and archives partitioned tables. It needs to know which tables are there already, and which need to be created or have an outdated definition.
- to compare on deployment the current database (as returned by schemaspy) against the wanted state, so the deploy process can warn about missing database changes.

# Test

The tests need access to a PostgreSQL server, with a database `schemaspy`:

    # su  -c "createuser -DRS yourusername" postgres
    # su  -c "createdb -O yourusername schemaspy" postgres
    $ make int
