WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page, session_id ORDER BY duration_sec DESC, id) AS latest_rank
    FROM web_events
    QUALIFY latest_rank = 1
)
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    CASE WHEN duration_sec >= 508 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    COUNT(CASE WHEN event_type = 'search' THEN 1 END) AS search_count,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    GROUPING_ID() AS grouping_level
FROM deduped
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', event_time), CASE WHEN duration_sec >= 508 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('QUARTER', event_time)), ())
HAVING search_count > 1
