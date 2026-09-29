WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec, id) = 1
)
SELECT
    id,
    page,
    event_time
FROM latest
ORDER BY page DESC NULLS LAST;
