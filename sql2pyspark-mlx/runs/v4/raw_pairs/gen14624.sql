WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(duration_sec AS STRING) AS duration_sec_str
    FROM web_events
    WHERE event_time > DATE '2023-05-27'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    island_id,
    COUNT(*) FILTER (WHERE page = '/product') AS product_rows,
    COUNT_IF(page = '/cart') AS cart_events,
    ROUND(AVG(session_start_flag), 2) AS avg_session_start_flag
FROM islands
GROUP BY island_id
ORDER BY avg_session_start_flag DESC;
