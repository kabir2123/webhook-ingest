-- Replace the plain index with a UNIQUE constraint so Postgres enforces
-- one-event-per-event_id at the storage layer.
DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT uq_events_event_id UNIQUE (event_id);
