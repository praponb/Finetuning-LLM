WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        ROUND(duration_sec, 1) duration_sec_rounded
    FROM web_events
    WHERE event_time >= DATE '2024-01-23'
      AND page IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    AVG(burst_id) AS avg_burst_id,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows
FROM islands
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', event_time))
HAVING COUNT(*) < 1
ORDER BY avg_burst_id DESC NULLS LAST
