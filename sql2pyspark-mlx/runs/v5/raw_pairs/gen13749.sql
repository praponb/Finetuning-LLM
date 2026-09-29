WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        event_type,
        duration_sec
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) = 1
)
SELECT id, duration_sec
FROM deduped
ORDER BY duration_sec DESC
