# what

...

# test

To run the tests against a real PostgreSQL server:

Make a schema `schemaspy`:

    # su  -c "createuser -DRS yourusername" postgres
    # su  -c "createdb -O yourusername schemaspy" postgres
    $ make int
