MIGRATIONS = """
CREATE TABLE IF NOT EXISTS es_aggregates (
  id              {AGGREGATE_ID_TYPE}     PRIMARY KEY,
  version         INTEGER  NOT NULL,
  aggregate_type  TEXT     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_es_aggregate_aggregate_type ON es_aggregates (aggregate_type);

CREATE TABLE IF NOT EXISTS es_events (
  id              BIGSERIAL  PRIMARY KEY,
  transaction_id  XID8       NOT NULL,
  aggregate_id    {AGGREGATE_ID_TYPE}       NOT NULL REFERENCES es_aggregates (id),
  version         INTEGER    NOT NULL,
  EVENT_TYPE      TEXT       NOT NULL,
  json_data       JSON       NOT NULL,
  UNIQUE (aggregate_id, version)
);

CREATE INDEX IF NOT EXISTS idx_es_event_transaction_id_id ON es_events (transaction_id, id);
CREATE INDEX IF NOT EXISTS idx_es_event_aggregate_id ON es_events (aggregate_id);
CREATE INDEX IF NOT EXISTS idx_es_event_version ON es_events (version);

CREATE TABLE IF NOT EXISTS es_aggregate_snapshot (
  aggregate_id  {AGGREGATE_ID_TYPE}     NOT NULL REFERENCES es_aggregates (id),
  version       INTEGER  NOT NULL,
  json_data     JSON     NOT NULL,
  PRIMARY KEY (aggregate_id, version)
);

CREATE INDEX IF NOT EXISTS idx_es_aggregate_snapshot_aggregate_id ON es_aggregate_snapshot (aggregate_id);
CREATE INDEX IF NOT EXISTS idx_es_aggregate_snapshot_version ON es_aggregate_snapshot (version);

CREATE TABLE IF NOT EXISTS es_event_subscriptions (
  subscription_name    TEXT    PRIMARY KEY,
  last_transaction_id  XID8    NOT NULL,
  last_event_id        BIGINT  NOT NULL
);


CREATE OR REPLACE FUNCTION channel_event_notify_fct()
RETURNS TRIGGER AS
  $BODY$
  DECLARE
    aggregate_type  TEXT;
  BEGIN
    SELECT a.aggregate_type INTO aggregate_type FROM es_aggregates a WHERE a.ID = NEW.aggregate_id;
    PERFORM pg_notify('channel_event_notify', aggregate_type);
    RETURN NEW;
  END;
  $BODY$
  LANGUAGE PLPGSQL;

CREATE OR REPLACE TRIGGER channel_event_notify_trg
  AFTER INSERT ON es_events
  FOR EACH ROW
  EXECUTE PROCEDURE channel_event_notify_fct();
"""
