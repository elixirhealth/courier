
CREATE SCHEMA cache;

CREATE TABLE cache.access_record (
  row_id SERIAL PRIMARY KEY,
  transaction_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  key BYTEA NOT NULL,
  cache_put_occurred BOOLEAN NOT NULL DEFAULT FALSE,
  cache_put_date INT,
  cache_put_time BIGINT,
  libri_put_occurred BOOLEAN NOT NULL DEFAULT FALSE,
  libri_put_time BIGINT NOT NULL,
  cache_get_ocurred BOOLEAN NOT NULL DEFAULT FALSE,
  cache_get_time BIGINT NOT NULL
);

CREATE UNIQUE INDEX access_record_key_cache_put ON cache.access_record (key, cache_put_occurred);
CREATE UNIQUE INDEX access_record_key_libri_put ON cache.access_record (key, libri_put_occurred);
CREATE INDEX access_record_eviction ON cache.access_record(libri_put_occurred, cache_put_date);


CREATE TABLE cache.document (
  row_id SERIAL PRIMARY KEY,
  transaction_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  key BYTEA NOT NULL,
  value BYTEA NOT NULL
);

CREATE UNIQUE INDEX cache_key ON cache.document (key);
