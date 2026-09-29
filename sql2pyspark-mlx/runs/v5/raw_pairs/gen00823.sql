WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        LOWER(page) page_lower,
        COALESCE(page, 'N/A') page_filled
    FROM web_events
    WHERE page IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    *,
    LAG(duration_sec, 1) OVER (ORDER BY duration_sec, id) prev_duration_sec,
    SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_duration_sec
FROM cleaned
