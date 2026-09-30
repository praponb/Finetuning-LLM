WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        SUM(is_new_session) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Aggregate with conditional metrics
SELECT
    session_no,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    SIZE(COLLECT_SET(event_type)) AS num_event_types
FROM islands
GROUP BY session_no
ORDER BY num_event_types;
