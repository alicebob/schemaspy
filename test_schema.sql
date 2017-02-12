DROP SCHEMA IF EXISTS schemaspyint CASCADE;
CREATE SCHEMA schemaspyint;
SET search_path TO schemaspyint;

CREATE TABLE simple
  ( id uuid PRIMARY KEY
  , name text NULL
  , t timestamptz
  );

CREATE TABLE root
  ( id text NOT NULL
  );
CREATE TABLE root_123 () INHERITS (root);

CREATE TABLE indexed
  ( major int
  , minor int
  , name varchar
  );
CREATE INDEX index_indexed ON indexed (major, minor);
CREATE UNIQUE INDEX unique_indexed ON indexed (name);
CREATE INDEX indexed_name_lower_idx ON indexed (lower(name), minor);

CREATE VIEW myview_now AS SELECT id, name FROM simple where t > current_timestamp;
CREATE MATERIALIZED VIEW myview_forever AS SELECT id, name FROM simple where t > current_timestamp;
