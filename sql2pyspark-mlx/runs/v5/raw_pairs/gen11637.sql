WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_time,
        SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    COUNT(*) AS cnt,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_time) ORDER BY (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 DESC) AS span_minutes_dense_rank
FROM islands
GROUP BY 1, 2
ORDER BY week_start DESC NULLS LAST
