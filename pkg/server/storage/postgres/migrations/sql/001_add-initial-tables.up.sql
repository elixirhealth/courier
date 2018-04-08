
CREATE SCHEMA cache;

CREATE TABLE cache.access_record (
  row_id SERIAL PRIMARY KEY,
  transaction_period TSTZRANGE NOT NULL DEFAULT tstzrange(NOW(), 'infinity', '[)'),
  key BYTEA NOT NULL,
  cache_put_occurred BOOLEAN NOT NULL DEFAULT FALSE,
  cache_put_time_min INT,
  cache_put_time_micro BIGINT,
  libri_put_occurred BOOLEAN NOT NULL DEFAULT FALSE,
  libri_put_time_micro BIGINT,
  cache_get_occurred BOOLEAN NOT NULL DEFAULT FALSE,
  cache_get_time_micro BIGINT
);

CREATE UNIQUE INDEX access_record_key_put
  ON cache.access_record (key, cache_put_occurred, libri_put_occurred)
  WHERE NOT cache_get_occurred;  -- multiple cache get records have same put_occurred flag values
CREATE INDEX access_record_eviction ON cache.access_record(libri_put_occurred, cache_put_time_min);


CREATE TABLE cache.document (
  row_id SERIAL PRIMARY KEY,
  transaction_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  key BYTEA NOT NULL,
  value BYTEA NOT NULL
);

CREATE UNIQUE INDEX cache_key ON cache.document (key);
