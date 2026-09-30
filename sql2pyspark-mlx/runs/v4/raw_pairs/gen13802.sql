WITH flagged AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    session_id,
    event_type,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    AVG(island_id) FILTER (WHERE event_type = 'page_view') AS page_view_avg_island_id,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes
FROM sessionized
GROUP BY session_id, event_type, DATE_TRUNC('QUARTER', event_time)
ORDER BY span_minutes DESC
