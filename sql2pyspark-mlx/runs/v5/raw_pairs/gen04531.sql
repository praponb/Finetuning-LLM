WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY duration_sec NULLS LAST, id) AS latest_rank
        FROM web_events
    ) t
    WHERE latest_rank = 1
)
SELECT
    CASE WHEN duration_sec >= 393 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    MAX(event_time) AS ended_at,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes
FROM deduped
GROUP BY ROLLUP (CASE WHEN duration_sec >= 393 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING span_minutes < 1000000;
