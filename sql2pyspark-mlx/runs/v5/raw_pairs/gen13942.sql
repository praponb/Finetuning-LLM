WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    MIN(event_time) AS first_event_time,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN event_type = 'purchase' THEN 1 END), 0), 2) AS duration_sec_per_purchase,
    MAX(event_time) AS last_event_time,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev
FROM islands
GROUP BY CUBE (DATE_TRUNC('WEEK', event_time))
ORDER BY avg_duration_sec DESC;
