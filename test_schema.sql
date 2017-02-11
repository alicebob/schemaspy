DROP TABLE IF EXISTS simple;
CREATE TABLE simple
  ( id uuid PRIMARY KEY
  , name text NULL
  , t timestamptz
  );

DROP TABLE IF EXISTS root_123;
DROP TABLE IF EXISTS root;
CREATE TABLE root
  ( id text NOT NULL
  );
CREATE TABLE root_123 () INHERITS (root);

DROP TABLE IF EXISTS indexed;
CREATE TABLE indexed
  ( major int
  , minor int
  , name varchar
  );
CREATE INDEX index_indexed ON indexed (major, minor);
CREATE UNIQUE INDEX unique_indexed ON indexed (name);
CREATE INDEX indexed_name_lower_idx ON indexed (lower(name), minor);
