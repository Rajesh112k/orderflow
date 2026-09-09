ALTER TABLE failed_events
    ADD COLUMN IF NOT EXISTS replay_started_at TIMESTAMPTZ;


ALTER TABLE failed_events
DROP CONSTRAINT IF EXISTS failed_events_status_check;


ALTER TABLE failed_events
    ADD CONSTRAINT failed_events_status_check
        CHECK (
            status IN (
                       'FAILED',
                       'REPLAYING',
                       'REPLAYED',
                       'RESOLVED'
                )
            );