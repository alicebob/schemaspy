DROP TABLE IF EXISTS simple;
CREATE TABLE simple
  ( id uuid NOT NULL
  , name text NULL
  , t timestamptz
  );

DROP TABLE IF EXISTS root_123;
DROP TABLE IF EXISTS root;
CREATE TABLE root
  ( id text NOT NULL
  );
CREATE TABLE root_123 () INHERITS (root);
