WITH kept_keys AS (
    -- Set intersection on keys
    SELECT id, page
    FROM web_events
    INTERSECT
    SELECT id, page
    FROM web_events
    WHERE duration_sec BETWEEN 343 AND 388
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    JOIN kept_keys k ON s.id = k.id AND s.page = k.page
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    MIN(duration_sec) AS min_duration_sec,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_view_rows
FROM kept_rows
GROUP BY DATE_TRUNC('YEAR', event_time)
ORDER BY min_duration_sec DESC;
