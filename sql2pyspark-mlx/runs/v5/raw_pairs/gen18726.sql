WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        MIN(duration_sec) OVER (PARTITION BY event_type) AS event_type_min_duration_sec,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS duration_sec_bucket,
        SUM(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_2,
        SUM(duration_sec) OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
    FROM web_events
    WHERE page != '/product'
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    SUM(CASE WHEN page = '/search' THEN cumulative_duration_sec_2 ELSE 0 END) AS search_cumulative_duration_sec_2,
    COUNT(*) AS num_rows,
    MAX(cumulative_duration_sec) AS max_cumulative_duration_sec,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes
FROM with_windows
GROUP BY 1
