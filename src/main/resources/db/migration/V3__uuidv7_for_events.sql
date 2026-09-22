-- Use UUIDv7 for event IDs (time-ordered for tenant replay/debugging)
-- Requires PostgreSQL 18+ which has native uuidv7() support

ALTER TABLE events ALTER COLUMN id SET DEFAULT uuidv7();